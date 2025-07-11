#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge with Reliability and Robust Anti-Duplication Features
"""

import discord
import aiohttp
import asyncio
import os
import json
import re
import time
import threading
import hashlib
from datetime import datetime, timedelta
from collections import defaultdict, deque
from discord.ext import commands
from aiohttp import web
import logging
from typing import Dict, Any, Optional, List, Tuple, Set

# Configure logging with more detail for debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable debug logging for the duplicate detector specifically
dup_logger = logging.getLogger('duplicate_detector')
dup_logger.setLevel(logging.DEBUG)

print("🔥 ENHANCED BIDIRECTIONAL BRIDGE WITH ROBUST ANTI-DUPLICATION FEATURES STARTING!")

# Environment Configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GROUPME_BOT_ID = os.getenv("GROUPME_BOT_ID")
GROUPME_ACCESS_TOKEN = os.getenv("GROUPME_ACCESS_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
GROUPME_GROUP_ID = os.getenv("GROUPME_GROUP_ID")
PORT = int(os.getenv("PORT", "8080"))

# API Endpoints
GROUPME_POST_URL = "https://api.groupme.com/v3/bots/post"
GROUPME_MESSAGES_URL = f"https://api.groupme.com/v3/groups/{GROUPME_GROUP_ID}/messages"

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Enhanced Global State with Thread Safety
bot_status = {"ready": False, "start_time": time.time()}
message_mapping = {}
reply_context_cache = {}
recent_discord_messages = deque(maxlen=20)
recent_groupme_messages = deque(maxlen=20)

# Thread-safe locks
mapping_lock = threading.Lock()
cache_lock = threading.Lock()
discord_messages_lock = threading.Lock()
groupme_messages_lock = threading.Lock()

# Message Queue System
message_queue = asyncio.Queue(maxsize=1000)
failed_messages = deque(maxlen=100)
failed_messages_lock = threading.Lock()

# Persistence file
FAILED_MESSAGES_FILE = "failed_messages.json"

# Health monitoring stats
health_stats = {
    "messages_sent": 0,
    "messages_failed": 0,
    "last_success": time.time(),
    "last_failure": None,
    "queue_size": 0
}
health_stats_lock = threading.Lock()

# Message retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# Message synchronization tracking
sync_tracking = {
    "discord_messages": {},
    "groupme_messages": {},
    "verification_history": deque(maxlen=10),
    "last_sync_check": 0,
    "synced_messages": deque(maxlen=500)
}
sync_tracking_lock = threading.Lock()

# Synchronization check interval (in seconds)
SYNC_CHECK_INTERVAL = 600  # 10 minutes
SYNC_MESSAGE_WINDOW = 3600  # Check messages from last hour
AUTO_REQUEUE_MISSING = False  # Set to False to prevent spam
MAX_RESYNC_MESSAGES = 10

# ============================================================================
# ROBUST DUPLICATE DETECTION SYSTEM
# ============================================================================

class RobustDuplicateDetector:
    """Thread-safe, multi-layered duplicate detection system"""
    
    def __init__(self):
        # Thread safety locks
        self._lock = threading.RLock()
        
        # Message ID tracking
        self.processed_message_ids: Set[str] = set()
        self.message_id_timestamps: Dict[str, float] = {}
        
        # Content-based duplicate detection
        self.content_hashes: Dict[str, float] = {}
        
        # Enhanced rate limiting per user
        self.user_last_message: Dict[str, float] = {}
        self.user_recent_messages: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))
        
        # Queue-level duplicate tracking
        self.queued_message_hashes: Set[str] = set()
        
        # Webhook duplicate detection
        self.webhook_request_ids: Dict[str, float] = {}
        
        # Configuration - Made less aggressive
        self.RATE_LIMIT_SECONDS = 0.05  # Only block messages < 50ms apart
        self.CONTENT_DUPLICATE_WINDOW = 120  # Reduced from 5 minutes to 2 minutes
        self.MESSAGE_ID_TTL = 1800  # Reduced from 1 hour to 30 minutes
        self.MAX_MESSAGES_PER_MINUTE = 20  # Increased from 15
        self.WEBHOOK_DUPLICATE_WINDOW = 15  # Reduced from 30 seconds
        
        # Temporary disable mechanism for debugging
        self.temporarily_disabled = False
        
        # Statistics
        self.stats = {
            'total_checked': 0,
            'duplicates_blocked': 0,
            'rate_limited': 0,
            'content_duplicates': 0,
            'id_duplicates': 0,
            'webhook_duplicates': 0,
            'queue_duplicates': 0
        }
    
    def _generate_content_hash(self, content: str, author: str, platform: str) -> str:
        """Generate hash for content-based duplicate detection"""
        # Normalize content
        normalized = content.strip().lower()
        normalized = normalized.replace('\n', ' ').replace('\r', '')
        normalized = ' '.join(normalized.split())
        
        # Create hash including author and platform
        hash_data = f"{normalized}:{author}:{platform}"
        return hashlib.md5(hash_data.encode()).hexdigest()
    
    def _generate_queue_hash(self, message_data: Dict[str, Any]) -> str:
        """Generate hash for queue-level duplicate detection"""
        content = str(message_data.get('text', ''))
        author = str(message_data.get('name', ''))
        msg_type = str(message_data.get('type', ''))
        
        hash_data = f"{content}:{author}:{msg_type}:{int(time.time() // 60)}"
        return hashlib.md5(hash_data.encode()).hexdigest()
    
    def _cleanup_old_data(self):
        """Remove old tracking data to prevent memory leaks"""
        current_time = time.time()
        cleanup_count = 0
        
        # Clean old message IDs (platform-specific)
        expired_ids = [
            msg_id for msg_id, timestamp in self.message_id_timestamps.items()
            if current_time - timestamp > self.MESSAGE_ID_TTL
        ]
        for msg_id in expired_ids:
            self.processed_message_ids.discard(msg_id)
            self.message_id_timestamps.pop(msg_id, None)
            cleanup_count += 1
        
        # Clean old content hashes
        expired_hashes = [
            content_hash for content_hash, timestamp in self.content_hashes.items()
            if current_time - timestamp > self.CONTENT_DUPLICATE_WINDOW
        ]
        for content_hash in expired_hashes:
            self.content_hashes.pop(content_hash, None)
            cleanup_count += 1
        
        # Clean old webhook requests
        expired_webhooks = [
            req_id for req_id, timestamp in self.webhook_request_ids.items()
            if current_time - timestamp > self.WEBHOOK_DUPLICATE_WINDOW
        ]
        for req_id in expired_webhooks:
            self.webhook_request_ids.pop(req_id, None)
            cleanup_count += 1
        
        # Clean old user rate limit data
        expired_users = []
        for user_id, msgs in self.user_recent_messages.items():
            # Remove old messages from each user's deque
            while msgs and current_time - msgs[0] > 300:  # 5 minutes
                msgs.popleft()
            # If no recent messages, remove the user entirely
            if not msgs:
                expired_users.append(user_id)
        
        for user_id in expired_users:
            self.user_recent_messages.pop(user_id, None)
            cleanup_count += 1
        
        if cleanup_count > 0:
            logger.debug(f"🧹 Cleaned up {cleanup_count} expired duplicate detection entries")
    
    def reset_all_caches(self):
        """Reset all caches - useful for debugging"""
        with self._lock:
            old_counts = {
                'message_ids': len(self.processed_message_ids),
                'content_hashes': len(self.content_hashes),
                'queue_hashes': len(self.queued_message_hashes),
                'webhook_requests': len(self.webhook_request_ids),
                'user_tracking': len(self.user_recent_messages)
            }
            
            self.processed_message_ids.clear()
            self.message_id_timestamps.clear()
            self.content_hashes.clear()
            self.queued_message_hashes.clear()
            self.webhook_request_ids.clear()
            self.user_recent_messages.clear()
            
            logger.info(f"🧹 Reset all caches: {old_counts}")
            return old_counts
    
    def check_webhook_duplicate(self, request_id: str = None, 
                              client_ip: str = None, 
                              message_data: Dict = None) -> bool:
        """Check for webhook-level duplicates"""
        with self._lock:
            current_time = time.time()
            
            if not request_id:
                msg_id = message_data.get('id', '')
                content = message_data.get('text', '')[:50]
                request_id = f"{msg_id}:{content}:{client_ip}"
            
            if request_id in self.webhook_request_ids:
                time_diff = current_time - self.webhook_request_ids[request_id]
                if time_diff < self.WEBHOOK_DUPLICATE_WINDOW:
                    logger.warning(f"🚫 Webhook duplicate detected: {request_id} (last seen {time_diff:.1f}s ago)")
                    self.stats['webhook_duplicates'] += 1
                    self.stats['duplicates_blocked'] += 1
                    return True
            
            self.webhook_request_ids[request_id] = current_time
            return False
    
    def temporarily_disable(self, seconds: int = 60):
        """Temporarily disable duplicate detection for debugging"""
        with self._lock:
            self.temporarily_disabled = True
            logger.warning(f"🔓 Duplicate detection temporarily disabled for {seconds} seconds")
        
        def re_enable():
            time.sleep(seconds)
            with self._lock:
                self.temporarily_disabled = False
                logger.info("🔒 Duplicate detection re-enabled")
        
        threading.Thread(target=re_enable, daemon=True).start()
    
    def check_message_duplicate(self, message_data: Dict[str, Any], 
                              platform: str = "unknown", 
                              skip_content_check: bool = False) -> Tuple[bool, str]:
        """Comprehensive duplicate detection"""
        with self._lock:
            # If temporarily disabled, only check for basic duplicates
            if self.temporarily_disabled:
                msg_id = message_data.get('id')
                if msg_id:
                    platform_msg_id = f"{platform}:{msg_id}"
                    if platform_msg_id in self.processed_message_ids:
                        return True, "message_id_duplicate"
                    self.processed_message_ids.add(platform_msg_id)
                    self.message_id_timestamps[platform_msg_id] = time.time()
                return False, "not_duplicate_disabled"
            
            self.stats['total_checked'] += 1
            current_time = time.time()
            
            # Cleanup old data periodically
            if self.stats['total_checked'] % 50 == 0:  # More frequent cleanup
                self._cleanup_old_data()
            
            msg_id = message_data.get('id')
            content = message_data.get('text', '')
            author = message_data.get('name', 'Unknown')
            user_id = message_data.get('user_id', message_data.get('sender_id', ''))
            
            # Create platform-specific message ID to prevent cross-platform conflicts
            platform_msg_id = f"{platform}:{msg_id}" if msg_id else None
            
            logger.debug(f"🔍 Checking message: {platform_msg_id} from {author} on {platform}")
            
            # 1. Message ID duplicate check (platform-specific)
            if platform_msg_id and platform_msg_id in self.processed_message_ids:
                time_since = current_time - self.message_id_timestamps.get(platform_msg_id, 0)
                logger.info(f"🚫 ID duplicate: {platform_msg_id} (last seen {time_since:.1f}s ago)")
                self.stats['id_duplicates'] += 1
                self.stats['duplicates_blocked'] += 1
                return True, "message_id_duplicate"
            
            # 2. Content-based duplicate check (can be skipped)
            if not skip_content_check and content.strip():
                content_hash = self._generate_content_hash(content, author, platform)
                if content_hash in self.content_hashes:
                    time_since = current_time - self.content_hashes[content_hash]
                    if time_since < self.CONTENT_DUPLICATE_WINDOW:
                        logger.info(f"🚫 Content duplicate from {author}: {content[:30]}... (last seen {time_since:.1f}s ago)")
                        self.stats['content_duplicates'] += 1
                        self.stats['duplicates_blocked'] += 1
                        return True, "content_duplicate"
                
                self.content_hashes[content_hash] = current_time
            
            # 3. Enhanced rate limiting (more lenient)
            if user_id:
                platform_user_id = f"{platform}:{user_id}"
                recent_msgs = self.user_recent_messages[platform_user_id]
                
                # Remove old messages
                while recent_msgs and current_time - recent_msgs[0] > 60:
                    recent_msgs.popleft()
                
                # Check rate limit (only for extreme cases)
                if len(recent_msgs) >= self.MAX_MESSAGES_PER_MINUTE:
                    logger.warning(f"🚫 Rate limit exceeded for {author}: {len(recent_msgs)} msgs/min")
                    self.stats['rate_limited'] += 1
                    self.stats['duplicates_blocked'] += 1
                    return True, "rate_limit_exceeded"
                
                # Very lenient time check - only block super rapid messages
                if recent_msgs and current_time - recent_msgs[-1] < 0.05:  # 50ms
                    time_diff = current_time - recent_msgs[-1]
                    logger.info(f"🚫 Rate limited: {author} (waited {time_diff:.3f}s)")
                    self.stats['rate_limited'] += 1
                    self.stats['duplicates_blocked'] += 1
                    return True, "rate_limit_too_fast"
                
                recent_msgs.append(current_time)
            
            # 4. Record message as processed (platform-specific)
            if platform_msg_id:
                self.processed_message_ids.add(platform_msg_id)
                self.message_id_timestamps[platform_msg_id] = current_time
                logger.debug(f"✅ Recorded new message: {platform_msg_id}")
            
            return False, "not_duplicate"
    
    def check_queue_duplicate(self, message_data: Dict[str, Any]) -> bool:
        """Check if message is already queued for processing"""
        with self._lock:
            queue_hash = self._generate_queue_hash(message_data)
            
            if queue_hash in self.queued_message_hashes:
                logger.info(f"🚫 Queue duplicate detected: {message_data.get('name', 'Unknown')}")
                self.stats['queue_duplicates'] += 1
                self.stats['duplicates_blocked'] += 1
                return True
            
            self.queued_message_hashes.add(queue_hash)
            
            # Keep queue tracking size manageable
            if len(self.queued_message_hashes) > 1000:
                old_hashes = list(self.queued_message_hashes)[:200]
                for old_hash in old_hashes:
                    self.queued_message_hashes.discard(old_hash)
            
            return False
    
    def mark_message_processed(self, message_data: Dict[str, Any], platform: str = "unknown"):
        """Mark a message as successfully processed"""
        with self._lock:
            queue_hash = self._generate_queue_hash(message_data)
            self.queued_message_hashes.discard(queue_hash)
            
            # Also remove from processed message IDs if it's been too long
            msg_id = message_data.get('id')
            if msg_id:
                platform_msg_id = f"{platform}:{msg_id}"
                # Remove old entries to prevent buildup
                current_time = time.time()
                if platform_msg_id in self.message_id_timestamps:
                    if current_time - self.message_id_timestamps[platform_msg_id] > 300:  # 5 minutes
                        self.processed_message_ids.discard(platform_msg_id)
                        self.message_id_timestamps.pop(platform_msg_id, None)
                        logger.debug(f"🗑️ Removed old processed message: {platform_msg_id}")
    
    def is_system_overloaded(self) -> bool:
        """Check if system is overloaded"""
        with self._lock:
            current_time = time.time()
            recent_blocks = sum(1 for timestamp in self.content_hashes.values() 
                           if current_time - timestamp < 60)
            return recent_blocks > 50
    
    def get_stats(self) -> Dict[str, Any]:
        """Get duplicate detection statistics"""
        with self._lock:
            stats = self.stats.copy()
            total_checked = max(1, self.stats['total_checked'])
            duplicate_rate = (self.stats['duplicates_blocked'] / total_checked) * 100
            
            stats.update({
                'processed_ids_count': len(self.processed_message_ids),
                'content_hashes_count': len(self.content_hashes),
                'queued_hashes_count': len(self.queued_message_hashes),
                'webhook_requests_count': len(self.webhook_request_ids),
                'duplicate_rate': duplicate_rate,
                'system_overloaded': self.is_system_overloaded(),
                'temporarily_disabled': self.temporarily_disabled
            })
            return stats
    
    def reset_stats(self):
        """Reset statistics"""
        with self._lock:
            self.stats = {
                'total_checked': 0,
                'duplicates_blocked': 0,
                'rate_limited': 0,
                'content_duplicates': 0,
                'id_duplicates': 0,
                'webhook_duplicates': 0,
                'queue_duplicates': 0
            }

# Global duplicate detector instance
duplicate_detector = RobustDuplicateDetector()

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

async def wait_for_bot_ready(timeout=30):
    """Wait for bot to be ready with timeout"""
    start_time = time.time()
    while not bot.is_ready() and time.time() - start_time < timeout:
        await asyncio.sleep(0.5)
    return bot.is_ready()

def update_health_stats(success: bool):
    """Update health monitoring statistics"""
    with health_stats_lock:
        if success:
            health_stats["messages_sent"] += 1
            health_stats["last_success"] = time.time()
        else:
            health_stats["messages_failed"] += 1
            health_stats["last_failure"] = time.time()

def save_failed_messages():
    """Persist failed messages to disk"""
    try:
        with failed_messages_lock:
            if failed_messages:
                with open(FAILED_MESSAGES_FILE, 'w') as f:
                    messages = list(failed_messages)
                    json.dump(messages, f)
                logger.info(f"💾 Saved {len(messages)} failed messages to disk")
    except Exception as e:
        logger.error(f"❌ Error saving failed messages: {e}")

def load_failed_messages():
    """Load and retry previously failed messages"""
    try:
        if os.path.exists(FAILED_MESSAGES_FILE):
            with open(FAILED_MESSAGES_FILE, 'r') as f:
                messages = json.load(f)
                logger.info(f"📥 Loaded {len(messages)} failed messages from disk")
                return messages
    except Exception as e:
        logger.error(f"❌ Error loading failed messages: {e}")
    return []

def generate_message_hash(content: str, author: str, timestamp: float) -> str:
    """Generate a unique hash for a message to track duplicates"""
    data = f"{content}:{author}:{int(timestamp)}"
    return hashlib.md5(data.encode()).hexdigest()

def is_message_already_synced(content: str, author: str, timestamp: float) -> bool:
    """Check if a message has already been synced to prevent duplicates"""
    msg_hash = generate_message_hash(content, author, timestamp)
    with sync_tracking_lock:
        return msg_hash in sync_tracking['synced_messages']

def mark_message_as_synced(content: str, author: str, timestamp: float):
    """Mark a message as synced to prevent future duplicates"""
    msg_hash = generate_message_hash(content, author, timestamp)
    with sync_tracking_lock:
        sync_tracking['synced_messages'].append(msg_hash)

def is_bot_message(data):
    """Check if message is from a bot"""
    if data.get('sender_type') == 'bot':
        return True
    if data.get('sender_id') == GROUPME_BOT_ID:
        return True
    name = data.get('name', '').lower()
    if name in ['bot', 'groupme', 'system']:
        return True
    return False

# Enhanced HTTP request with retry logic
async def make_http_request(url, method='GET', data=None, headers=None, retries=3):
    """HTTP request helper with retry logic"""
    for attempt in range(retries):
        async with aiohttp.ClientSession() as session:
            try:
                if method.upper() == 'POST':
                    async with session.post(url, json=data, headers=headers) as response:
                        result = {
                            'status': response.status,
                            'data': await response.json() if response.status in [200, 202] else None,
                            'text': await response.text()
                        }
                        if response.status in [200, 202]:
                            return result
                else:
                    async with session.get(url, headers=headers) as response:
                        result = {
                            'status': response.status,
                            'data': await response.json() if response.status == 200 else None,
                            'text': await response.text()
                        }
                        if response.status == 200:
                            return result
                
                if attempt < retries - 1:
                    wait_time = RETRY_DELAY_BASE ** attempt
                    logger.warning(f"⏳ HTTP {response.status}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    return result
                    
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = RETRY_DELAY_BASE ** attempt
                    logger.warning(f"⏳ HTTP request error: {e}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ HTTP request failed after {retries} attempts: {e}")
                    return {'status': 500, 'data': None, 'text': str(e)}

async def get_groupme_messages(group_id, limit=20):
    """Get recent GroupMe messages"""
    if not GROUPME_ACCESS_TOKEN:
        return []
    
    url = f"{GROUPME_MESSAGES_URL}?token={GROUPME_ACCESS_TOKEN}&limit={limit}"
    response = await make_http_request(url)
    if response['status'] == 200 and response['data']:
        return response['data'].get('response', {}).get('messages', [])
    return []

async def fetch_recent_discord_messages(limit=100):
    """Fetch recent messages from Discord channel"""
    try:
        channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not channel:
            return []
        
        messages = []
        after_time = datetime.utcnow() - timedelta(seconds=SYNC_MESSAGE_WINDOW)
        
        async for message in channel.history(limit=limit, after=after_time):
            # Skip bot messages AND commands
            if not message.author.bot and not message.content.startswith('!'):
                messages.append({
                    'id': str(message.id),
                    'content': message.content,
                    'author': message.author.display_name,
                    'timestamp': message.created_at.timestamp(),
                    'has_attachments': len(message.attachments) > 0,
                    'raw_message': message
                })
        
        return messages
    except Exception as e:
        logger.error(f"Error fetching Discord messages: {e}")
        return []

async def fetch_recent_groupme_messages_detailed(limit=100):
    """Fetch recent messages from GroupMe with full details"""
    try:
        messages = await get_groupme_messages(GROUPME_GROUP_ID, limit)
        current_time = time.time()
        
        filtered_messages = []
        for msg in messages:
            if msg.get('sender_type') != 'bot' and (current_time - msg.get('created_at', 0)) <= SYNC_MESSAGE_WINDOW:
                filtered_messages.append({
                    'id': msg.get('id'),
                    'content': msg.get('text', ''),
                    'author': msg.get('name', 'Unknown'),
                    'timestamp': msg.get('created_at', 0),
                    'has_attachments': bool(msg.get('attachments', [])),
                    'raw_message': msg
                })
        
        return filtered_messages
    except Exception as e:
        logger.error(f"Error fetching GroupMe messages: {e}")
        return []

def normalize_message_content(content):
    """Normalize message content for comparison"""
    if not content:
        return ""
    
    # Remove bridge-specific formatting
    content = re.sub(r'^[^:]+: ', '', content)
    content = re.sub(r'^\*\*[^*]+\*\*: ', '', content)
    content = re.sub(r'↪️ \*\*[^*]+\*\*.*?\n\n', '', content, flags=re.DOTALL)
    
    # Normalize whitespace
    content = ' '.join(content.split())
    
    return content.lower().strip()

def find_matching_message(message, target_messages, time_tolerance=60):
    """Find a matching message in the target platform"""
    normalized_content = normalize_message_content(message['content'])
    if not normalized_content and not message.get('has_attachments'):
        return None
    
    for target_msg in target_messages:
        time_diff = abs(message['timestamp'] - target_msg['timestamp'])
        if time_diff > time_tolerance:
            continue
        
        target_normalized = normalize_message_content(target_msg['content'])
        
        if not normalized_content and not target_normalized:
            if message.get('has_attachments') and target_msg.get('has_attachments'):
                return target_msg
        
        if normalized_content and target_normalized and normalized_content == target_normalized:
            return target_msg
        
        if normalized_content and target_normalized:
            if normalized_content in target_normalized or target_normalized in normalized_content:
                return target_msg
    
    return None

async def verify_message_sync():
    """Verify that messages are properly synced between platforms"""
    try:
        logger.info("🔍 Starting message synchronization verification...")
        
        discord_messages = await fetch_recent_discord_messages()
        groupme_messages = await fetch_recent_groupme_messages_detailed()
        
        logger.info(f"📊 Found {len(discord_messages)} Discord messages and {len(groupme_messages)} GroupMe messages")
        
        missing_on_groupme = []
        missing_on_discord = []
        
        # Check Discord -> GroupMe
        for discord_msg in discord_messages:
            match = find_matching_message(discord_msg, groupme_messages)
            if not match:
                if not is_message_already_synced(discord_msg['content'], discord_msg['author'], discord_msg['timestamp']):
                    missing_on_groupme.append(discord_msg)
        
        # Check GroupMe -> Discord
        for groupme_msg in groupme_messages:
            match = find_matching_message(groupme_msg, discord_messages)
            if not match:
                if not is_message_already_synced(groupme_msg['content'], groupme_msg['author'], groupme_msg['timestamp']):
                    missing_on_discord.append(groupme_msg)
        
        # Generate sync report
        sync_report = {
            'timestamp': time.time(),
            'discord_total': len(discord_messages),
            'groupme_total': len(groupme_messages),
            'missing_on_groupme': len(missing_on_groupme),
            'missing_on_discord': len(missing_on_discord),
            'sync_rate': 100.0
        }
        
        total_unique = len(set(msg['id'] for msg in discord_messages)) + len(set(msg['id'] for msg in groupme_messages))
        if total_unique > 0:
            synced = total_unique - len(missing_on_groupme) - len(missing_on_discord)
            sync_report['sync_rate'] = (synced / total_unique) * 100
        
        # Store verification history
        with sync_tracking_lock:
            sync_tracking['verification_history'].append(sync_report)
            sync_tracking['last_sync_check'] = time.time()
        
        if missing_on_groupme or missing_on_discord:
            logger.warning(f"⚠️ SYNC VERIFICATION ISSUE DETECTED:")
            logger.warning(f"Missing on GroupMe: {len(missing_on_groupme)} messages")
            logger.warning(f"Missing on Discord: {len(missing_on_discord)} messages")
            logger.warning(f"Sync Rate: {sync_report['sync_rate']:.1f}%")
            
            if missing_on_groupme:
                logger.info("Missing on GroupMe:")
                for msg in missing_on_groupme[:5]:
                    logger.info(f"  - {msg['author']}: {msg['content'][:50]}...")
            
            if missing_on_discord:
                logger.info("Missing on Discord:")
                for msg in missing_on_discord[:5]:
                    logger.info(f"  - {msg['author']}: {msg['content'][:50]}...")
            
            if AUTO_REQUEUE_MISSING:
                logger.info("🔄 Auto-requeue is enabled, re-syncing missing messages...")
                await resync_missing_messages(missing_on_groupme, missing_on_discord)
            else:
                logger.info("🔄 Auto-requeue is disabled, skipping automatic resync (use !syncfix to manually resync)")
        else:
            logger.info(f"✅ Sync verification passed! All messages synced properly. Rate: {sync_report['sync_rate']:.1f}%")
        
        return sync_report, missing_on_groupme, missing_on_discord
        
    except Exception as e:
        logger.error(f"Error during sync verification: {e}")
        return None, [], []

async def resync_missing_messages(missing_on_groupme: List[Dict], missing_on_discord: List[Dict]):
    """Resync missing messages without creating duplicates"""
    resync_to_groupme = missing_on_groupme[:MAX_RESYNC_MESSAGES]
    resync_to_discord = missing_on_discord[:MAX_RESYNC_MESSAGES]
    
    logger.info(f"🔄 Resync requested: {len(resync_to_groupme)} to GroupMe, {len(resync_to_discord)} to Discord")
    
    # Resync Discord messages to GroupMe
    for msg in resync_to_groupme:
        # Skip if message is very recent
        if time.time() - msg['timestamp'] < 30:
            logger.info(f"⏩ Skipping recent message from {msg['author']} (too recent)")
            continue
            
        # Skip commands that somehow got through
        if msg['content'].startswith('!'):
            logger.info(f"⏩ Skipping command message: {msg['content']}")
            continue
        
        mark_message_as_synced(msg['content'], msg['author'], msg['timestamp'])
        
        logger.info(f"🔄 Re-syncing Discord message from {msg['author']}: {msg['content'][:50]}...")
        await message_queue.put({
            'send_func': send_to_groupme,
            'kwargs': {
                'text': msg['content'],
                'author_name': msg['author']
            },
            'type': 'sync_recovery_to_groupme',
            'retries': 0,
            'original_data': {
                'text': msg['content'],
                'name': msg['author'],
                'id': f"resync_discord_{msg['id']}",
                'user_id': 'resync_user'
            }
        })
    
    # Resync GroupMe messages to Discord
    for msg in resync_to_discord:
        if time.time() - msg['timestamp'] < 30:
            logger.info(f"⏩ Skipping recent message from {msg['author']} (too recent)")
            continue
        
        mark_message_as_synced(msg['content'], msg['author'], msg['timestamp'])
        
        logger.info(f"🔄 Re-syncing GroupMe message from {msg['author']}: {msg['content'][:50]}...")
        await message_queue.put({
            'send_func': send_to_discord,
            'kwargs': {
                'message': msg['raw_message']
            },
            'type': 'sync_recovery_to_discord',
            'retries': 0,
            'original_data': {
                'text': msg['content'],
                'name': msg['author'],
                'id': f"resync_groupme_{msg['id']}",
                'user_id': 'resync_user'
            }
        })
    
    actual_queued = len([m for m in resync_to_groupme if not m['content'].startswith('!') and time.time() - m['timestamp'] >= 30])
    actual_queued += len([m for m in resync_to_discord if time.time() - m['timestamp'] >= 30])
    
    if actual_queued > 0:
        logger.info(f"✅ Actually queued {actual_queued} messages for resync")
    else:
        logger.info("ℹ️ No messages needed resyncing (all recent or filtered)")

# Enhanced reply detection
async def detect_reply_context(data):
    """Enhanced reply detection"""
    reply_context = None
    
    # Method 1: Official GroupMe reply attachments
    if data.get('attachments'):
        reply_attachment = next(
            (att for att in data['attachments'] if att.get('type') == 'reply'), 
            None
        )
        if reply_attachment:
            reply_id = reply_attachment.get('reply_id') or reply_attachment.get('base_reply_id')
            if reply_id:
                with cache_lock:
                    if reply_id in reply_context_cache:
                        original_msg = reply_context_cache[reply_id]
                        reply_context = {
                            'text': original_msg.get('text', '[No text]'),
                            'name': original_msg.get('name', 'Unknown'),
                            'type': 'official_reply'
                        }
                        logger.info(f"✅ Found official reply to {reply_context['name']}")
    
    # Method 2: Enhanced @mention detection
    if not reply_context and data.get('text'):
        text = data['text']
        mention_match = re.search(r'@(\w+)', text, re.IGNORECASE)
        if mention_match:
            mentioned_name = mention_match.group(1).lower()
            
            with discord_messages_lock:
                for discord_msg in reversed(recent_discord_messages):
                    display_name = discord_msg['author'].lower()
                    username = discord_msg.get('username', '').lower()
                    
                    if (mentioned_name in display_name or 
                        mentioned_name in username or
                        mentioned_name == display_name or
                        mentioned_name == username):
                        
                        reply_context = {
                            'text': discord_msg['content'],
                            'name': discord_msg['author'],
                            'type': 'mention_reply'
                        }
                        logger.info(f"✅ Found @mention reply to {reply_context['name']} (Discord nickname)")
                        break
    
    return reply_context

# Discord mention conversion
async def convert_discord_mentions_to_nicknames(text):
    """Convert Discord mentions to readable nicknames"""
    if not text or not bot.is_ready():
        return text
    
    try:
        mention_pattern = r'<@!?(\d+)>'
        mentions = re.findall(mention_pattern, text)
        
        if not mentions:
            return text
        
        for user_id in mentions:
            try:
                user = bot.get_user(int(user_id))
                if not user:
                    user = await bot.fetch_user(int(user_id))
                
                if user:
                    display_name = getattr(user, 'display_name', user.name)
                    
                    original_mention = f'<@{user_id}>'
                    nickname_mention = f'<@!{user_id}>'
                    readable_mention = f'@{display_name}'
                    
                    text = text.replace(original_mention, readable_mention)
                    text = text.replace(nickname_mention, readable_mention)
                    
                    logger.info(f"🏷️ Converted mention: {user_id} → @{display_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error converting mention for user ID {user_id}: {e}")
                continue
        
        return text
        
    except Exception as e:
        logger.error(f"❌ Error in mention conversion: {e}")
        return text

# Message sending functions
async def send_to_groupme(text, author_name=None, reply_context=None):
    """Send message to GroupMe with retry logic"""
    try:
        text = await convert_discord_mentions_to_nicknames(text)
        
        if reply_context:
            quoted_text = reply_context.get('text', 'previous message')
            reply_author = reply_context.get('name', 'Someone')
            preview = quoted_text[:100] + '...' if len(quoted_text) > 100 else quoted_text
            preview = await convert_discord_mentions_to_nicknames(preview)
            text = f"↪️ **{author_name} replying to {reply_author}:**\n> {preview}\n\n{text}"
        else:
            if author_name and not text.startswith(author_name):
                text = f"{author_name}: {text}" if text.strip() else f"{author_name} sent content"
        
        payload = {"bot_id": GROUPME_BOT_ID, "text": text}
        response = await make_http_request(GROUPME_POST_URL, 'POST', payload)
        
        success = response['status'] == 202
        update_health_stats(success)
        
        if success:
            logger.info(f"✅ Message sent to GroupMe: {text[:50]}...")
        else:
            logger.error(f"❌ Failed to send to GroupMe: {response['status']}")
            
        return success
            
    except Exception as e:
        logger.error(f"❌ Error sending to GroupMe: {e}")
        update_health_stats(False)
        return False

async def send_to_discord(message, reply_context=None):
    """Send message to Discord with retry logic"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            logger.error(f"Discord channel {DISCORD_CHANNEL_ID} not found")
            return False
        
        content = message.get('text', '[No text content]')
        author = message.get('name', 'GroupMe User')
        
        if reply_context:
            original_text = reply_context.get('text', '[No text]')
            original_author = reply_context.get('name', 'Unknown')
            preview = original_text[:200] + '...' if len(original_text) > 200 else original_text
            content = f"↪️ **{author}** replying to **{original_author}**:\n> {preview}\n\n{content}"
        
        # Handle images
        embeds = []
        if message.get('attachments'):
            for attachment in message['attachments']:
                if attachment.get('type') == 'image' and attachment.get('url'):
                    embeds.append(discord.Embed().set_image(url=attachment['url']))
        
        formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
        sent_message = await discord_channel.send(formatted_content, embeds=embeds)
        
        if message.get('id'):
            with mapping_lock:
                message_mapping[sent_message.id] = message['id']
            with cache_lock:
                reply_context_cache[message['id']] = {
                    **message,
                    'discord_message_id': sent_message.id,
                    'processed_timestamp': time.time()
                }
        
        update_health_stats(True)
        logger.info(f"✅ Message sent to Discord: {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        update_health_stats(False)
        return False

# Enhanced message queue processor
async def message_queue_processor():
    """Enhanced queue processor with duplicate protection"""
    logger.info("📬 Starting enhanced message queue processor...")
    
    while True:
        try:
            with health_stats_lock:
                health_stats["queue_size"] = message_queue.qsize()
            
            msg_data = await message_queue.get()
            
            send_func = msg_data['send_func']
            kwargs = msg_data['kwargs']
            retries = msg_data.get('retries', 0)
            msg_type = msg_data.get('type', 'unknown')
            original_data = msg_data.get('original_data')
            
            # Double-check for duplicates at processing time
            if original_data:
                is_duplicate, reason = duplicate_detector.check_message_duplicate(
                    original_data, "queue_processing"
                )
                if is_duplicate:
                    logger.warning(f"🚫 Caught duplicate at processing stage: {reason}")
                    duplicate_detector.mark_message_processed(original_data, "queue_processing")
                    continue
            
            logger.info(f"📤 Processing {msg_type} message (attempt {retries + 1}/{MAX_RETRIES})")
            
            success = await send_func(**kwargs)
            
            if success:
                logger.info(f"✅ Message sent successfully")
                if original_data:
                    # Determine platform based on message type
                    platform = "discord" if "discord" in msg_type else "groupme"
                    duplicate_detector.mark_message_processed(original_data, platform)
            elif retries < MAX_RETRIES - 1:
                msg_data['retries'] = retries + 1
                wait_time = RETRY_DELAY_BASE ** (retries + 1)
                logger.warning(f"⏳ Message failed, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                await message_queue.put(msg_data)
            else:
                with failed_messages_lock:
                    failed_messages.append(msg_data)
                logger.error(f"❌ Message failed after {MAX_RETRIES} attempts")
                if original_data:
                    # Still mark as processed to prevent infinite retries
                    platform = "discord" if "discord" in msg_type else "groupme"
                    duplicate_detector.mark_message_processed(original_data, platform)
                save_failed_messages()
                
        except Exception as e:
            logger.error(f"❌ Queue processor error: {e}")
            await asyncio.sleep(1)

# Sync verification task
async def sync_verification_task():
    """Periodically verify message synchronization"""
    await asyncio.sleep(60)
    
    while True:
        try:
            await asyncio.sleep(SYNC_CHECK_INTERVAL)
            await verify_message_sync()
        except Exception as e:
            logger.error(f"Sync verification task error: {e}")

# Enhanced webhook server
async def run_webhook_server():
    """Enhanced webhook server with multi-layered duplicate protection"""
    
    async def health_check(request):
        with health_stats_lock:
            stats = health_stats.copy()
        
        with sync_tracking_lock:
            last_sync = sync_tracking.get('last_sync_check', 0)
            sync_history = list(sync_tracking['verification_history'])
        
        dup_stats = duplicate_detector.get_stats()
        
        latest_sync_rate = "N/A"
        if sync_history:
            latest_sync_rate = f"{sync_history[-1]['sync_rate']:.1f}%"
        
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "features": {
                "robust_duplicate_detection": True,
                "multi_layered_protection": True,
                "message_queue": True,
                "retry_logic": True,
                "health_monitoring": True,
                "persistence": True,
                "thread_safety": True,
                "sync_verification": True,
                "discord_nicknames_only": True,
                "discord_mention_conversion": True
            },
            "health_stats": stats,
            "duplicate_stats": dup_stats,
            "reply_cache_size": len(reply_context_cache),
            "failed_messages": len(failed_messages),
            "sync_status": {
                "latest_sync_rate": latest_sync_rate,
                "last_check": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync)) if last_sync else "Never",
                "auto_requeue_enabled": AUTO_REQUEUE_MISSING
            }
        })
    
    async def groupme_webhook(request):
        """Enhanced webhook handler with multi-layered duplicate protection"""
        start_time = time.time()
        
        try:
            client_ip = request.remote
            request_id = request.headers.get('X-Request-ID')
            
            data = await request.json()
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            logger.info(f"📨 GroupMe webhook: {sender_info} - {data.get('text', '')[:50]}...")
            
            # 1. Webhook-level duplicate check
            if duplicate_detector.check_webhook_duplicate(request_id, client_ip, data):
                return web.json_response({"status": "ignored", "reason": "webhook_duplicate"})
            
            # 2. Bot message filter
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
                return web.json_response({"status": "ignored", "reason": "bot_message"})
            
            # 3. Comprehensive duplicate check (less aggressive for normal flow)
            is_duplicate, reason = duplicate_detector.check_message_duplicate(data, "groupme", skip_content_check=False)
            if is_duplicate and reason in ["message_id_duplicate", "rate_limit_exceeded", "rate_limit_too_fast"]:
                return web.json_response({"status": "ignored", "reason": reason})
            
            # 4. Queue-level duplicate check
            if duplicate_detector.check_queue_duplicate(data):
                return web.json_response({"status": "ignored", "reason": "queue_duplicate"})
            
            logger.info(f"✅ Processing unique message from {data.get('name', 'Unknown')}")
            
            # Store in recent messages
            if data.get('id'):
                with groupme_messages_lock:
                    recent_groupme_messages.append({
                        'id': data['id'],
                        'text': data.get('text', ''),
                        'name': data.get('name', ''),
                        'created_at': data.get('created_at', time.time())
                    })
                with cache_lock:
                    reply_context_cache[data['id']] = data
            
            # Check if bot is ready
            if not bot.is_ready():
                logger.warning("⏳ Bot not ready, queuing message...")
                ready = await wait_for_bot_ready(timeout=5)
                if not ready:
                    logger.error("❌ Bot still not ready after timeout")
            
            reply_context = await detect_reply_context(data)
            
            message_item = {
                'send_func': send_to_discord,
                'kwargs': {'message': data, 'reply_context': reply_context},
                'type': 'groupme_to_discord',
                'retries': 0,
                'original_data': data
            }
            
            await message_queue.put(message_item)
            
            processing_time = time.time() - start_time
            if processing_time > 3.0:
                logger.warning(f"⚠️ Slow webhook processing: {processing_time:.3f}s - may cause retries")
            else:
                logger.debug(f"⏱️ Webhook processed in {processing_time:.3f}s")
            
            return web.json_response({"status": "queued"})
            
        except Exception as e:
            processing_time = time.time() - start_time
            logger.error(f"❌ Error handling GroupMe webhook ({processing_time:.3f}s): {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    # Create web application
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_post('/groupme', groupme_webhook)
    
    # Start server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    logger.info(f"🌐 Enhanced webhook server running on 0.0.0.0:{PORT}")
    
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await runner.cleanup()

def start_webhook_server():
    """Start webhook server in thread"""
    def run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_webhook_server())
        except Exception as e:
            logger.error(f"Webhook server error: {e}")
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    return thread

# Discord Bot Events
@bot.event
async def on_ready():
    """Bot ready event"""
    global bot_status
    bot_status["ready"] = True
    
    logger.info(f'🤖 {bot.user} connected to Discord!')
    logger.info(f'📺 Channel ID: {DISCORD_CHANNEL_ID}')
    logger.info(f'📬 Message queue: ✅')
    logger.info(f'🔄 Retry logic: ✅')
    logger.info(f'💾 Persistence: ✅')
    logger.info(f'🔒 Thread safety: ✅')
    logger.info(f'📊 Health monitoring: ✅')
    logger.info(f'🛡️ Robust duplicate detection: ✅')
    logger.info(f'🔍 Sync verification: ✅ (every {SYNC_CHECK_INTERVAL//60} min)')
    logger.info(f'🔄 Auto-requeue missing: {"✅" if AUTO_REQUEUE_MISSING else "❌ (DISABLED)"}')
    
    # Start tasks
    asyncio.create_task(message_queue_processor())
    asyncio.create_task(sync_verification_task())
    
    # Load failed messages
    failed = load_failed_messages()
    if failed:
        for msg in failed:
            await message_queue.put(msg)
        try:
            os.remove(FAILED_MESSAGES_FILE)
        except:
            pass

@bot.event
async def on_message(message):
    """Enhanced message handler"""
    if message.author.bot or message.channel.id != DISCORD_CHANNEL_ID:
        await bot.process_commands(message)
        return
    
    if message.content.startswith('!'):
        await bot.process_commands(message)
        return
    
    discord_nickname = message.author.display_name
    discord_username = message.author.name
    
    logger.info(f"📨 Processing Discord message from '{discord_nickname}' (username: {discord_username})")
    
    discord_msg_data = {
        'content': message.content,
        'author': discord_nickname,
        'username': discord_username,
        'author_id': message.author.id,
        'timestamp': time.time(),
        'message_id': message.id
    }
    
    with discord_messages_lock:
        recent_discord_messages.append(discord_msg_data)
    
    reply_context = None
    if message.reference and message.reference.message_id:
        try:
            replied_message = await message.channel.fetch_message(message.reference.message_id)
            reply_context = {
                'text': replied_message.content[:200],
                'name': replied_message.author.display_name,
                'type': 'official_reply'
            }
            logger.info(f"✅ Found Discord reply to: '{reply_context['name']}'")
        except:
            pass
    
    message_content = message.content or ""
    
    if message.attachments:
        attachment_info = []
        for attachment in message.attachments:
            if attachment.content_type and attachment.content_type.startswith('image/'):
                attachment_info.append("[Image]")
            else:
                attachment_info.append(f"[Attached: {attachment.filename}]")
        
        if attachment_info:
            if message_content:
                message_content = f"{message_content} {' '.join(attachment_info)}"
            else:
                message_content = ' '.join(attachment_info)
    
    if message_content.strip():
        message_data = {
            'text': message_content,
            'name': discord_nickname,
            'user_id': str(message.author.id),
            'id': str(message.id),
            'timestamp': time.time()
        }
        
        # Check for duplicates (only serious ones)
        is_duplicate, reason = duplicate_detector.check_message_duplicate(message_data, "discord", skip_content_check=True)
        if is_duplicate and reason in ["message_id_duplicate", "rate_limit_exceeded"]:
            logger.info(f"🚫 Skipping duplicate Discord message: {reason}")
            await bot.process_commands(message)
            return
        
        if duplicate_detector.check_queue_duplicate(message_data):
            logger.info(f"🚫 Message already queued for processing")
            await bot.process_commands(message)
            return
        
        await message_queue.put({
            'send_func': send_to_groupme,
            'kwargs': {
                'text': message_content,
                'author_name': discord_nickname,
                'reply_context': reply_context
            },
            'type': 'discord_to_groupme',
            'retries': 0,
            'original_data': message_data
        })
        logger.info(f"📬 Queued message for GroupMe from '{discord_nickname}'")
    
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID):
        return
    
    emoji = str(reaction.emoji)
    discord_nickname = user.display_name
    
    logger.info(f"😀 Processing reaction {emoji} from '{discord_nickname}'")
    
    original_content = reaction.message.content[:50] if reaction.message.content else "a message"
    reaction_text = f"{discord_nickname} reacted {emoji} to '{original_content}...'"
    
    reaction_data = {
        'text': reaction_text,
        'name': discord_nickname,
        'user_id': str(user.id),
        'id': f"reaction_{reaction.message.id}_{user.id}_{emoji}",
        'timestamp': time.time()
    }
    
    is_duplicate, reason = duplicate_detector.check_message_duplicate(reaction_data, "discord_reaction")
    if not is_duplicate and not duplicate_detector.check_queue_duplicate(reaction_data):
        await message_queue.put({
            'send_func': send_to_groupme,
            'kwargs': {
                'text': reaction_text,
                'author_name': discord_nickname
            },
            'type': 'discord_reaction',
            'retries': 0,
            'original_data': reaction_data
        })

# Health monitoring task
async def health_monitor():
    """Monitor bridge health and log statistics"""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            
            with health_stats_lock:
                stats = health_stats.copy()
            
            dup_stats = duplicate_detector.get_stats()
            
            total_messages = stats["messages_sent"] + stats["messages_failed"]
            if total_messages > 0:
                success_rate = (stats["messages_sent"] / total_messages) * 100
            else:
                success_rate = 100
            
            logger.info("📊 BRIDGE HEALTH REPORT:")
            logger.info(f"✅ Messages sent: {stats['messages_sent']}")
            logger.info(f"❌ Messages failed: {stats['messages_failed']}")
            logger.info(f"📈 Success rate: {success_rate:.1f}%")
            logger.info(f"📬 Queue size: {stats['queue_size']}")
            logger.info(f"💾 Failed messages: {len(failed_messages)}")
            logger.info(f"⏰ Last success: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stats['last_success']))}")
            
            logger.info("🛡️ DUPLICATE DETECTION STATS:")
            logger.info(f"📊 Total checked: {dup_stats['total_checked']}")
            logger.info(f"🚫 Duplicates blocked: {dup_stats['duplicates_blocked']} ({dup_stats['duplicate_rate']:.1f}%)")
            logger.info(f"🔗 ID duplicates: {dup_stats['id_duplicates']}")
            logger.info(f"📝 Content duplicates: {dup_stats['content_duplicates']}")
            logger.info(f"⏱️ Rate limited: {dup_stats['rate_limited']}")
            logger.info(f"🌐 Webhook duplicates: {dup_stats['webhook_duplicates']}")
            logger.info(f"📬 Queue duplicates: {dup_stats['queue_duplicates']}")
            logger.info(f"⚡ System overloaded: {'YES' if dup_stats['system_overloaded'] else 'NO'}")
            logger.info(f"🔓 Temporarily disabled: {'YES' if dup_stats['temporarily_disabled'] else 'NO'}")
            
            if success_rate < 90 and total_messages > 10:
                logger.warning(f"⚠️ High failure rate detected: {100-success_rate:.1f}%")
            
            if dup_stats["duplicate_rate"] > 20 and dup_stats["total_checked"] > 50:
                logger.warning(f"⚠️ High duplicate rate detected: {dup_stats['duplicate_rate']:.1f}%")
            
            save_failed_messages()
            
        except Exception as e:
            logger.error(f"Health monitor error: {e}")

# Enhanced cleanup task
async def enhanced_cleanup():
    """Enhanced cleanup with persistence"""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour
            
            with mapping_lock:
                if len(message_mapping) > 500:
                    old_keys = list(message_mapping.keys())[:-500]
                    for key in old_keys:
                        message_mapping.pop(key, None)
            
            with cache_lock:
                if len(reply_context_cache) > 200:
                    old_keys = list(reply_context_cache.keys())[:-200]
                    for key in old_keys:
                        reply_context_cache.pop(key, None)
            
            save_failed_messages()
            logger.info("🧹 Cleanup completed")
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Bot Commands
@bot.command(name='status')
async def status(ctx):
    """Enhanced status command"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    with sync_tracking_lock:
        last_sync = sync_tracking.get('last_sync_check', 0)
        sync_history = list(sync_tracking['verification_history'])
    
    dup_stats = duplicate_detector.get_stats()
    
    total_messages = stats["messages_sent"] + stats["messages_failed"]
    success_rate = (stats["messages_sent"] / total_messages * 100) if total_messages > 0 else 100
    
    latest_sync_rate = "N/A"
    if sync_history:
        latest_sync_rate = f"{sync_history[-1]['sync_rate']:.1f}%"
    
    status_msg = f"""🟢 **Enhanced Bridge Status**
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🌐 Webhook Server: ✅

**📊 Health Statistics:**
✅ Messages sent: {stats["messages_sent"]}
❌ Messages failed: {stats["messages_failed"]}
📈 Success rate: {success_rate:.1f}%
📬 Queue size: {stats["queue_size"]}
💾 Failed messages: {len(failed_messages)}

**🛡️ Duplicate Detection:**
📊 Total checked: {dup_stats["total_checked"]}
🚫 Duplicates blocked: {dup_stats["duplicates_blocked"]} ({dup_stats["duplicate_rate"]:.1f}%)
⚡ System status: {"Overloaded" if dup_stats["system_overloaded"] else "Normal"}
🔓 Temporarily disabled: {"YES" if dup_stats["temporarily_disabled"] else "NO"}

**🔄 Sync Verification:**
📊 Latest sync rate: {latest_sync_rate}
⏰ Last check: {time.strftime('%H:%M:%S', time.localtime(last_sync)) if last_sync else 'Never'}
🔍 Next check: {time.strftime('%H:%M:%S', time.localtime(last_sync + SYNC_CHECK_INTERVAL)) if last_sync else 'Soon'}
🔄 Auto-requeue: {"✅" if AUTO_REQUEUE_MISSING else "❌"}

**🔧 Enhanced Features:**
📬 Message Queue: ✅
🔄 Retry Logic: ✅ (max {MAX_RETRIES} attempts)
💾 Persistence: ✅
🔒 Thread Safety: ✅
📊 Health Monitoring: ✅
🛡️ Robust Duplicate Detection: ✅
🔍 Sync Verification: ✅
🏷️ Discord Nicknames: ✅
🔗 Mention Conversion: ✅

📝 Message Mappings: {len(message_mapping)}
🔗 Reply Cache: {len(reply_context_cache)}"""
    
    await ctx.send(status_msg)

@bot.command(name='duplicates')
async def duplicate_stats(ctx):
    """Show detailed duplicate detection statistics"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    stats = duplicate_detector.get_stats()
    
    embed = discord.Embed(
        title="🛡️ Duplicate Detection Statistics",
        color=discord.Color.blue() if not stats["system_overloaded"] else discord.Color.orange()
    )
    
    embed.add_field(name="📊 Total Checked", value=stats['total_checked'], inline=True)
    embed.add_field(name="🚫 Duplicates Blocked", value=stats['duplicates_blocked'], inline=True)
    embed.add_field(name="📈 Block Rate", value=f"{stats['duplicate_rate']:.1f}%", inline=True)
    
    embed.add_field(name="🆔 ID Duplicates", value=stats['id_duplicates'], inline=True)
    embed.add_field(name="📝 Content Duplicates", value=stats['content_duplicates'], inline=True)
    embed.add_field(name="⏱️ Rate Limited", value=stats['rate_limited'], inline=True)
    
    embed.add_field(name="🌐 Webhook Duplicates", value=stats['webhook_duplicates'], inline=True)
    embed.add_field(name="📬 Queue Duplicates", value=stats['queue_duplicates'], inline=True)
    embed.add_field(name="⚡ System Status", value="Overloaded" if stats['system_overloaded'] else "Normal", inline=True)
    
    embed.add_field(name="🔓 Disabled", value="YES" if stats['temporarily_disabled'] else "NO", inline=True)
    
    embed.add_field(name="💾 Tracking Data", 
                   value=f"IDs: {stats['processed_ids_count']}\nContent: {stats['content_hashes_count']}\nQueue: {stats['queued_hashes_count']}\nWebhook: {stats['webhook_requests_count']}", 
                   inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='sync')
async def check_sync(ctx):
    """Manually run sync verification"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await ctx.send("🔍 Running sync verification...")
    
    report, missing_gm, missing_dc = await verify_message_sync()
    
    if report:
        embed = discord.Embed(
            title="🔄 Sync Verification Report",
            color=discord.Color.green() if report['sync_rate'] > 95 else discord.Color.orange()
        )
        
        embed.add_field(name="📊 Sync Rate", value=f"{report['sync_rate']:.1f}%", inline=True)
        embed.add_field(name="💬 Discord Messages", value=report['discord_total'], inline=True)
        embed.add_field(name="💬 GroupMe Messages", value=report['groupme_total'], inline=True)
        
        if report['missing_on_groupme'] > 0:
            embed.add_field(name="⚠️ Missing on GroupMe", value=report['missing_on_groupme'], inline=True)
        if report['missing_on_discord'] > 0:
            embed.add_field(name="⚠️ Missing on Discord", value=report['missing_on_discord'], inline=True)
        
        status_emoji = "✅" if report['sync_rate'] > 95 else "⚠️"
        embed.add_field(
            name=f"{status_emoji} Status", 
            value="All messages synced!" if report['sync_rate'] == 100 else "Some messages missing",
            inline=False
        )
        
        embed.add_field(
            name="🔄 Auto-requeue",
            value="✅ Enabled" if AUTO_REQUEUE_MISSING else "❌ Disabled (use !syncfix)",
            inline=False
        )
        
        await ctx.send(embed=embed)
    else:
        await ctx.send("❌ Failed to run sync verification")

@bot.command(name='syncfix')
async def sync_fix(ctx):
    """Run sync verification and re-send missing messages"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await ctx.send("🔍 Running sync verification with manual fix...")
    
    report, missing_gm, missing_dc = await verify_message_sync()
    
    if report:
        total_missing = len(missing_gm) + len(missing_dc)
        
        if total_missing == 0:
            await ctx.send("✅ All messages are properly synced!")
            return
        
        embed = discord.Embed(
            title="🔍 Sync Check Results",
            color=discord.Color.orange()
        )
        embed.add_field(name="📊 Sync Rate", value=f"{report['sync_rate']:.1f}%", inline=True)
        embed.add_field(name="⚠️ Missing on GroupMe", value=len(missing_gm), inline=True)
        embed.add_field(name="⚠️ Missing on Discord", value=len(missing_dc), inline=True)
        
        if missing_gm:
            sample_gm = missing_gm[:3]
            gm_list = "\n".join([f"- {msg['author']}: {msg['content'][:30]}..." for msg in sample_gm])
            embed.add_field(name="Sample Missing on GroupMe", value=gm_list, inline=False)
        
        if missing_dc:
            sample_dc = missing_dc[:3]
            dc_list = "\n".join([f"- {msg['author']}: {msg['content'][:30]}..." for msg in sample_dc])
            embed.add_field(name="Sample Missing on Discord", value=dc_list, inline=False)
        
        await ctx.send(embed=embed)
        
        if total_missing <= 5:
            await ctx.send(f"Would you like to resync these {total_missing} messages? React with ✅ to proceed or ❌ to cancel.")
            
            logger.info(f"Manual sync fix requested for {total_missing} messages")
            
            # Only resync very recent messages that are clearly missing
            recent_missing_gm = [msg for msg in missing_gm if time.time() - msg['timestamp'] > 60 and time.time() - msg['timestamp'] < 300]
            recent_missing_dc = [msg for msg in missing_dc if time.time() - msg['timestamp'] > 60 and time.time() - msg['timestamp'] < 300]
            
            if recent_missing_gm or recent_missing_dc:
                await resync_missing_messages(recent_missing_gm, recent_missing_dc)
                await ctx.send(f"🔄 Resynced {len(recent_missing_gm)} to GroupMe and {len(recent_missing_dc)} to Discord")
            else:
                await ctx.send("ℹ️ No recent messages needed resyncing (messages were too old or too new)")
        else:
            await ctx.send(f"⚠️ Too many missing messages ({total_missing}). Check logs and use !clear_cache if needed.")
    else:
        await ctx.send("❌ Failed to run sync verification")

@bot.command(name='synchistory')
async def sync_history(ctx):
    """Show sync verification history"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with sync_tracking_lock:
        history = list(sync_tracking['verification_history'])
    
    if not history:
        await ctx.send("📊 No sync verification history available yet")
        return
    
    embed = discord.Embed(
        title="📈 Sync Verification History",
        description=f"Last {len(history)} sync checks",
        color=discord.Color.blue()
    )
    
    avg_sync_rate = sum(h['sync_rate'] for h in history) / len(history)
    embed.add_field(name="📊 Average Sync Rate", value=f"{avg_sync_rate:.1f}%", inline=False)
    
    for i, check in enumerate(reversed(history[-5:])):
        time_str = time.strftime('%H:%M', time.localtime(check['timestamp']))
        status = "✅" if check['sync_rate'] > 95 else "⚠️"
        
        embed.add_field(
            name=f"{status} {time_str}",
            value=f"Rate: {check['sync_rate']:.1f}% | Missing: {check['missing_on_groupme'] + check['missing_on_discord']}",
            inline=True
        )
    
    await ctx.send(embed=embed)

@bot.command(name='retry')
async def retry_failed(ctx):
    """Retry all failed messages"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with failed_messages_lock:
        count = len(failed_messages)
        if count == 0:
            await ctx.send("✅ No failed messages to retry")
            return
        
        for msg in list(failed_messages):
            msg['retries'] = 0
            await message_queue.put(msg)
        
        failed_messages.clear()
    
    await ctx.send(f"🔄 Queued {count} failed messages for retry")

@bot.command(name='health')
async def health_report(ctx):
    """Detailed health report"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    dup_stats = duplicate_detector.get_stats()
    
    embed = discord.Embed(
        title="🏥 Bridge Health Report",
        color=discord.Color.green() if stats["messages_failed"] < stats["messages_sent"] * 0.1 else discord.Color.orange()
    )
    
    total = stats["messages_sent"] + stats["messages_failed"]
    success_rate = (stats["messages_sent"] / total * 100) if total > 0 else 100
    
    embed.add_field(name="✅ Success Rate", value=f"{success_rate:.1f}%", inline=True)
    embed.add_field(name="📬 Queue Size", value=stats["queue_size"], inline=True)
    embed.add_field(name="💾 Failed Messages", value=len(failed_messages), inline=True)
    
    embed.add_field(name="🛡️ Duplicate Rate", value=f"{dup_stats['duplicate_rate']:.1f}%", inline=True)
    embed.add_field(name="⚡ System Status", value="Overloaded" if dup_stats['system_overloaded'] else "Normal", inline=True)
    embed.add_field(name="🔓 Dup Detection", value="Disabled" if dup_stats['temporarily_disabled'] else "Enabled", inline=True)
    
    if stats["last_failure"]:
        last_fail = time.strftime('%H:%M:%S', time.localtime(stats["last_failure"]))
        embed.add_field(name="❌ Last Failure", value=last_fail, inline=True)
    
    embed.add_field(name="📊 Total Processed", value=f"{total} messages", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='resetstats')
async def reset_stats(ctx):
    """Reset duplicate detection statistics"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    duplicate_detector.reset_stats()
    await ctx.send("📊 Duplicate detection statistics have been reset")

@bot.command(name='disable_dup')
async def disable_duplicates(ctx, seconds: int = 60):
    """Temporarily disable duplicate detection for debugging"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    if seconds > 300:
        seconds = 300
    
    duplicate_detector.temporarily_disable(seconds)
    await ctx.send(f"🔓 Duplicate detection disabled for {seconds} seconds")

@bot.command(name='clear_cache')
async def clear_cache(ctx):
    """Clear duplicate detection cache"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    old_counts = duplicate_detector.reset_all_caches()
    total_cleared = sum(old_counts.values())
    
    await ctx.send(f"🧹 Duplicate detection cache cleared!\n"
                  f"Removed: {total_cleared} total entries\n"
                  f"Details: {old_counts}")

@bot.command(name='debug_dup')
async def debug_duplicates(ctx):
    """Show current duplicate detection state"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    stats = duplicate_detector.get_stats()
    
    embed = discord.Embed(
        title="🔍 Duplicate Detection Debug Info",
        color=discord.Color.blue()
    )
    
    embed.add_field(name="📊 Current Tracking", 
                   value=f"Message IDs: {stats['processed_ids_count']}\n"
                         f"Content Hashes: {stats['content_hashes_count']}\n"
                         f"Queue Hashes: {stats['queued_hashes_count']}\n"
                         f"Webhook Requests: {stats['webhook_requests_count']}", 
                   inline=True)
    
    embed.add_field(name="🚫 Block Counts",
                   value=f"ID Duplicates: {stats['id_duplicates']}\n"
                         f"Content Duplicates: {stats['content_duplicates']}\n"
                         f"Rate Limited: {stats['rate_limited']}\n"
                         f"Webhook Duplicates: {stats['webhook_duplicates']}", 
                   inline=True)
    
    embed.add_field(name="⚡ Status",
                   value=f"Disabled: {'YES' if stats['temporarily_disabled'] else 'NO'}\n"
                         f"Overloaded: {'YES' if stats['system_overloaded'] else 'NO'}\n"
                         f"Block Rate: {stats['duplicate_rate']:.1f}%", 
                   inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='debug_msg')
async def debug_message(ctx):
    """Send a test message to check flow"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    test_msg = f"Debug test message at {time.strftime('%H:%M:%S')}"
    await ctx.send(f"Sending test message: {test_msg}")
    
    await message_queue.put({
        'send_func': send_to_groupme,
        'kwargs': {
            'text': test_msg,
            'author_name': 'DEBUG'
        },
        'type': 'debug_test',
        'retries': 0
    })

@bot.command(name='force_msg')
async def force_message(ctx, *, message_text):
    """Force send a message bypassing duplicate detection"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    # Temporarily disable duplicate detection
    duplicate_detector.temporarily_disable(10)
    
    await ctx.send(f"🔓 Force sending: {message_text}")
    
    await message_queue.put({
        'send_func': send_to_groupme,
        'kwargs': {
            'text': f"FORCE: {message_text}",
            'author_name': ctx.author.display_name
        },
        'type': 'force_test',
        'retries': 0
    })

# Main Function
def main():
    """Enhanced main entry point"""
    if not DISCORD_BOT_TOKEN:
        logger.error("❌ DISCORD_BOT_TOKEN required!")
        return
    
    if not GROUPME_BOT_ID:
        logger.error("❌ GROUPME_BOT_ID required!")
        return
    
    if DISCORD_CHANNEL_ID == 0:
        logger.error("❌ DISCORD_CHANNEL_ID required!")
        return
    
    logger.info("🚀 Starting ENHANCED GroupMe-Discord Bridge...")
    logger.info("📬 Message queue enabled!")
    logger.info("🔄 Retry logic enabled!")
    logger.info("💾 Persistence enabled!")
    logger.info("🔒 Thread safety enabled!")
    logger.info("📊 Health monitoring enabled!")
    logger.info("🛡️ Robust multi-layered duplicate detection enabled!")
    logger.info(f"🔍 Sync verification enabled! (every {SYNC_CHECK_INTERVAL//60} minutes)")
    logger.info(f"🔄 Auto re-queue missing: {'✅' if AUTO_REQUEUE_MISSING else '❌ (DISABLED - prevents spam)'}")
    
    # Start webhook server
    webhook_thread = start_webhook_server()
    time.sleep(2)
    
    # Start bot with tasks
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create tasks
        loop.create_task(enhanced_cleanup())
        loop.create_task(health_monitor())
        
        # Run bot
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")
    finally:
        save_failed_messages()

if __name__ == "__main__":
    main()
