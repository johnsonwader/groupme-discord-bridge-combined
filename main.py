#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge with Reliability Features
Features: Message queue, retry logic, health monitoring, persistence, thread safety
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
from typing import Dict, Any, Optional
import pickle

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("🔥 ENHANCED BIDIRECTIONAL BRIDGE WITH RELIABILITY FEATURES STARTING!")

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
message_mapping = {}  # Discord message ID -> GroupMe message ID
reply_context_cache = {}  # Store messages for reply detection
recent_discord_messages = deque(maxlen=20)  # Store Discord messages with nicknames
recent_groupme_messages = deque(maxlen=20)  # Store GroupMe messages

# Thread-safe locks
mapping_lock = threading.Lock()
cache_lock = threading.Lock()
discord_messages_lock = threading.Lock()
groupme_messages_lock = threading.Lock()

# Message Queue System
message_queue = asyncio.Queue(maxsize=1000)
failed_messages = deque(maxlen=100)
failed_messages_lock = threading.Lock()

# Rate limiting with more lenient settings
processed_message_ids = deque(maxlen=100)
last_message_time = {}
RATE_LIMIT_SECONDS = 0.5  # Increased from 0.1 to 0.5 seconds

# Persistence file
FAILED_MESSAGES_FILE = "failed_messages.json"

# Health monitoring stats
health_stats = {
    "messages_sent": 0,
    "messages_failed": 0,
    "last_success": time.time(),
    "last_failure": None,
    "queue_size": 0,
    "last_sync_check": None,
    "sync_discrepancies": 0
}
health_stats_lock = threading.Lock()

# Message reconciliation tracking
message_history = {
    "discord": deque(maxlen=100),  # Store recent Discord messages
    "groupme": deque(maxlen=100)   # Store recent GroupMe messages
}
history_lock = threading.Lock()

# Message retry configuration
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # Exponential backoff base

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
                    # Convert deque to list for JSON serialization
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

# Enhanced duplicate detection with adjustable rate limiting
def is_simple_duplicate(data):
    """Enhanced duplicate detection with configurable rate limiting"""
    try:
        msg_id = data.get('id')
        if msg_id and msg_id in processed_message_ids:
            logger.info(f"🚫 Exact duplicate message ID: {msg_id}")
            return True
        
        if msg_id:
            processed_message_ids.append(msg_id)
        
        # Configurable rate limiting
        user_id = data.get('user_id', '')
        current_time = time.time()
        
        if user_id in last_message_time:
            time_diff = current_time - last_message_time[user_id]
            if time_diff < RATE_LIMIT_SECONDS:
                logger.info(f"🚫 Rate limited message from {data.get('name', 'Unknown')} (waited {time_diff:.2f}s)")
                return True
        
        last_message_time[user_id] = current_time
        return False
        
    except Exception as e:
        logger.error(f"Error in duplicate detection: {e}")
        return False

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
                
                # If we get here, request failed
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

async def get_discord_messages(channel_id, limit=20):
    """Get recent Discord messages from channel"""
    try:
        channel = bot.get_channel(channel_id)
        if not channel:
            return []
        
        messages = []
        async for message in channel.history(limit=limit):
            if not message.author.bot:  # Skip bot messages
                messages.append({
                    'id': str(message.id),
                    'content': message.content,
                    'author': message.author.display_name,
                    'timestamp': message.created_at.timestamp(),
                    'has_attachments': len(message.attachments) > 0
                })
        return messages
    except Exception as e:
        logger.error(f"Error fetching Discord messages: {e}")
        return []

def normalize_message_content(content):
    """Normalize message content for comparison"""
    if not content:
        return ""
    
    # Remove formatting and normalize
    normalized = content.lower().strip()
    
    # Remove common bridge prefixes
    patterns = [
        r'^[\w\s]+:\s*',  # "Username: " prefix
        r'^\*\*[\w\s]+\*\*:\s*',  # "**Username**: " prefix
        r'^↪️\s*\*\*[\w\s]+.*?\*\*.*?\n',  # Reply formatting
    ]
    
    for pattern in patterns:
        normalized = re.sub(pattern, '', normalized, flags=re.MULTILINE)
    
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    
    return normalized

def messages_match(msg1_content, msg2_content, threshold=0.85):
    """Check if two messages are likely the same using fuzzy matching"""
    # Normalize both messages
    norm1 = normalize_message_content(msg1_content)
    norm2 = normalize_message_content(msg2_content)
    
    if not norm1 or not norm2:
        return False
    
    # Exact match after normalization
    if norm1 == norm2:
        return True
    
    # Check if one contains the other (for truncated messages)
    if norm1 in norm2 or norm2 in norm1:
        return True
    
    # Simple similarity check (Jaccard similarity)
    words1 = set(norm1.split())
    words2 = set(norm2.split())
    
    if not words1 or not words2:
        return False
    
    intersection = words1.intersection(words2)
    union = words1.union(words2)
    
    similarity = len(intersection) / len(union) if union else 0
    
    return similarity >= threshold

async def check_message_sync(lookback_minutes=10, limit=50):
    """Check if messages are properly synced between platforms"""
    try:
        logger.info(f"🔍 Starting message sync check (last {lookback_minutes} minutes)")
        
        # Get recent messages from both platforms
        groupme_messages = await get_groupme_messages(GROUPME_GROUP_ID, limit=limit)
        discord_messages = await get_discord_messages(DISCORD_CHANNEL_ID, limit=limit)
        
        # Filter messages within time window
        current_time = time.time()
        time_window = lookback_minutes * 60
        
        recent_groupme = [
            msg for msg in groupme_messages 
            if current_time - msg.get('created_at', 0) < time_window
        ]
        
        recent_discord = [
            msg for msg in discord_messages
            if current_time - msg.get('timestamp', 0) < time_window
        ]
        
        # Track unmatched messages
        unmatched_groupme = []
        unmatched_discord = []
        matched_pairs = []
        
        # Check GroupMe messages that should be in Discord
        for gm_msg in recent_groupme:
            # Skip bot messages
            if gm_msg.get('sender_type') == 'bot':
                continue
                
            gm_content = gm_msg.get('text', '')
            gm_author = gm_msg.get('name', '')
            found_match = False
            
            for disc_msg in recent_discord:
                disc_content = disc_msg.get('content', '')
                
                # Check if content matches
                if messages_match(gm_content, disc_content):
                    found_match = True
                    matched_pairs.append((gm_msg, disc_msg))
                    break
            
            if not found_match and gm_content:
                unmatched_groupme.append(gm_msg)
                logger.warning(f"📍 GroupMe message not found in Discord: '{gm_author}: {gm_content[:50]}...'")
        
        # Check Discord messages that should be in GroupMe
        for disc_msg in recent_discord:
            disc_content = disc_msg.get('content', '')
            disc_author = disc_msg.get('author', '')
            found_match = False
            
            # Skip if already matched
            if any(disc_msg == pair[1] for pair in matched_pairs):
                continue
            
            for gm_msg in recent_groupme:
                gm_content = gm_msg.get('text', '')
                
                if messages_match(disc_content, gm_content):
                    found_match = True
                    break
            
            if not found_match and disc_content:
                unmatched_discord.append(disc_msg)
                logger.warning(f"📍 Discord message not found in GroupMe: '{disc_author}: {disc_content[:50]}...'")
        
        # Update health stats
        with health_stats_lock:
            health_stats["last_sync_check"] = current_time
            health_stats["sync_discrepancies"] = len(unmatched_groupme) + len(unmatched_discord)
        
        # Prepare report
        report = {
            "checked_at": current_time,
            "time_window_minutes": lookback_minutes,
            "groupme_messages": len(recent_groupme),
            "discord_messages": len(recent_discord),
            "matched_messages": len(matched_pairs),
            "unmatched_groupme": unmatched_groupme,
            "unmatched_discord": unmatched_discord,
            "sync_rate": len(matched_pairs) / max(len(recent_groupme), len(recent_discord), 1) * 100
        }
        
        logger.info(f"✅ Sync check complete: {report['sync_rate']:.1f}% sync rate")
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Error during sync check: {e}")
        return None

async def resync_missing_messages(report):
    """Attempt to resync any missing messages found during sync check"""
    try:
        resynced_count = 0
        
        # Resync GroupMe messages missing from Discord
        for gm_msg in report.get('unmatched_groupme', []):
            logger.info(f"🔄 Resyncing GroupMe message to Discord: {gm_msg.get('name')}: {gm_msg.get('text', '')[:50]}...")
            
            # Queue the message for Discord
            await message_queue.put({
                'send_func': send_to_discord,
                'kwargs': {'message': gm_msg, 'reply_context': None},
                'type': 'resync_groupme_to_discord',
                'retries': 0
            })
            resynced_count += 1
            
            # Small delay to avoid overwhelming
            await asyncio.sleep(0.5)
        
        # Resync Discord messages missing from GroupMe
        for disc_msg in report.get('unmatched_discord', []):
            logger.info(f"🔄 Resyncing Discord message to GroupMe: {disc_msg.get('author')}: {disc_msg.get('content', '')[:50]}...")
            
            # Queue the message for GroupMe
            await message_queue.put({
                'send_func': send_to_groupme,
                'kwargs': {
                    'text': disc_msg.get('content', ''),
                    'author_name': disc_msg.get('author', 'Discord User'),
                    'reply_context': None
                },
                'type': 'resync_discord_to_groupme',
                'retries': 0
            })
            resynced_count += 1
            
            await asyncio.sleep(0.5)
        
        logger.info(f"✅ Queued {resynced_count} messages for resync")
        return resynced_count
        
    except Exception as e:
        logger.error(f"❌ Error during resync: {e}")
        return 0

async def periodic_sync_check():
    """Periodically check message sync between platforms"""
    await asyncio.sleep(300)  # Wait 5 minutes after startup
    
    while True:
        try:
            # Run sync check
            report = await check_message_sync(lookback_minutes=15, limit=50)
            
            if report:
                sync_rate = report['sync_rate']
                discrepancies = len(report['unmatched_groupme']) + len(report['unmatched_discord'])
                
                if discrepancies > 0:
                    logger.warning(f"⚠️  Found {discrepancies} unsynced messages (sync rate: {sync_rate:.1f}%)")
                    
                    # Auto-resync if enabled
                    if discrepancies <= 10:  # Only auto-resync if reasonable number
                        logger.info("🔄 Auto-resyncing missing messages...")
                        await resync_missing_messages(report)
                    else:
                        logger.warning(f"⚠️  Too many discrepancies ({discrepancies}), manual intervention may be needed")
                else:
                    logger.info(f"✅ All messages synced properly (100% sync rate)")
            
            # Wait before next check (default 15 minutes)
            await asyncio.sleep(900)
            
        except Exception as e:
            logger.error(f"❌ Periodic sync check error: {e}")
            await asyncio.sleep(900)

# Enhanced reply detection
async def detect_reply_context(data):
    """Enhanced reply detection with better Discord nickname matching"""
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
    
    # Method 2: Enhanced @mention detection with nickname support
    if not reply_context and data.get('text'):
        text = data['text']
        mention_match = re.search(r'@(\w+)', text, re.IGNORECASE)
        if mention_match:
            mentioned_name = mention_match.group(1).lower()
            
            # Check recent Discord messages with enhanced matching
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
                    
                    logger.info(f"🏷️  Converted mention: {user_id} → @{display_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error converting mention for user ID {user_id}: {e}")
                continue
        
        return text
        
    except Exception as e:
        logger.error(f"❌ Error in mention conversion: {e}")
        return text

# Message sending functions with queue integration
async def send_to_groupme(text, author_name=None, reply_context=None):
    """Send message to GroupMe with retry logic"""
    try:
        # Convert Discord mentions
        text = await convert_discord_mentions_to_nicknames(text)
        
        # Format reply context
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
            
            # Track in message history for sync checking
            with history_lock:
                message_history["discord"].append({
                    'content': text,
                    'author': author_name,
                    'timestamp': time.time(),
                    'synced_to_groupme': True
                })
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
        
        # Format reply context
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
        
        # Send message
        formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
        sent_message = await discord_channel.send(formatted_content, embeds=embeds)
        
        # Store mapping with thread safety
        if message.get('id'):
            with mapping_lock:
                message_mapping[sent_message.id] = message['id']
            with cache_lock:
                reply_context_cache[message['id']] = {
                    **message,
                    'discord_message_id': sent_message.id,
                    'processed_timestamp': time.time()
                }
        
        # Track in message history for sync checking
        with history_lock:
            message_history["groupme"].append({
                'id': message.get('id'),
                'content': message.get('text', ''),
                'author': author,
                'timestamp': message.get('created_at', time.time()),
                'synced_to_discord': True
            })
        
        update_health_stats(True)
        logger.info(f"✅ Message sent to Discord: {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        update_health_stats(False)
        return False

# Message queue processor
async def message_queue_processor():
    """Process messages from the queue with retry logic"""
    logger.info("📬 Starting message queue processor...")
    
    while True:
        try:
            # Update queue size for health monitoring
            with health_stats_lock:
                health_stats["queue_size"] = message_queue.qsize()
            
            # Get message from queue
            msg_data = await message_queue.get()
            
            # Extract message details
            send_func = msg_data['send_func']
            kwargs = msg_data['kwargs']
            retries = msg_data.get('retries', 0)
            msg_type = msg_data.get('type', 'unknown')
            
            logger.info(f"📤 Processing {msg_type} message (attempt {retries + 1}/{MAX_RETRIES})")
            
            # Try to send the message
            success = await send_func(**kwargs)
            
            if not success and retries < MAX_RETRIES - 1:
                # Retry with exponential backoff
                msg_data['retries'] = retries + 1
                wait_time = RETRY_DELAY_BASE ** (retries + 1)
                logger.warning(f"⏳ Message failed, retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
                await message_queue.put(msg_data)
            elif not success:
                # Max retries reached, add to failed messages
                with failed_messages_lock:
                    failed_messages.append(msg_data)
                logger.error(f"❌ Message failed after {MAX_RETRIES} attempts")
                save_failed_messages()
            else:
                logger.info(f"✅ Message sent successfully")
                
        except Exception as e:
            logger.error(f"❌ Queue processor error: {e}")
            await asyncio.sleep(1)

# Enhanced webhook server
async def run_webhook_server():
    """Enhanced webhook server with queue integration"""
    
    async def health_check(request):
        with health_stats_lock:
            stats = health_stats.copy()
        
        # Format last sync check
        last_sync_formatted = "Never"
        if stats.get("last_sync_check"):
            last_sync_formatted = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stats["last_sync_check"]))
        
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "features": {
                "message_queue": True,
                "retry_logic": True,
                "health_monitoring": True,
                "persistence": True,
                "thread_safety": True,
                "sync_checking": True,
                "discord_nicknames_only": True,
                "discord_mention_conversion": True
            },
            "health_stats": stats,
            "sync_status": {
                "last_check": last_sync_formatted,
                "discrepancies": stats.get("sync_discrepancies", 0),
                "check_interval_minutes": 15
            },
            "processed_messages": len(processed_message_ids),
            "reply_cache_size": len(reply_context_cache),
            "failed_messages": len(failed_messages),
            "message_history": {
                "discord": len(message_history.get("discord", [])),
                "groupme": len(message_history.get("groupme", []))
            }
        })
    
    async def groupme_webhook(request):
        """Enhanced webhook handler with queue integration"""
        try:
            data = await request.json()
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            logger.info(f"📨 GroupMe webhook: {sender_info} - {data.get('text', '')[:50]}...")
            
            # Bot message filter
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
                return web.json_response({"status": "ignored", "reason": "bot_message"})
            
            # Duplicate check
            if is_simple_duplicate(data):
                return web.json_response({"status": "ignored", "reason": "duplicate"})
            
            logger.info(f"✅ Processing message from {data.get('name', 'Unknown')}")
            
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
                # Wait a bit for bot to be ready
                ready = await wait_for_bot_ready(timeout=5)
                if not ready:
                    logger.error("❌ Bot still not ready after timeout")
            
            # Detect reply context
            reply_context = await detect_reply_context(data)
            
            # Queue the message for Discord
            await message_queue.put({
                'send_func': send_to_discord,
                'kwargs': {'message': data, 'reply_context': reply_context},
                'type': 'groupme_to_discord',
                'retries': 0
            })
            
            return web.json_response({"status": "queued"})
            
        except Exception as e:
            logger.error(f"❌ Error handling GroupMe webhook: {e}")
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
    logger.info(f'🔍 Sync checking: ✅')
    
    # Start the message queue processor
    asyncio.create_task(message_queue_processor())
    
    # Start periodic sync checker
    asyncio.create_task(periodic_sync_check())
    
    # Load and retry any failed messages
    failed = load_failed_messages()
    if failed:
        for msg in failed:
            await message_queue.put(msg)
        # Clear the file after loading
        try:
            os.remove(FAILED_MESSAGES_FILE)
        except:
            pass

@bot.event
async def on_message(message):
    """Enhanced message handler with queue integration"""
    # Basic filters
    if message.author.bot or message.channel.id != DISCORD_CHANNEL_ID:
        await bot.process_commands(message)
        return
    
    # Skip commands
    if message.content.startswith('!'):
        await bot.process_commands(message)
        return
    
    # Get Discord display name
    discord_nickname = message.author.display_name
    discord_username = message.author.name
    
    logger.info(f"📨 Processing Discord message from '{discord_nickname}' (username: {discord_username})")
    
    # Store in recent messages with thread safety
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
    
    # Detect reply context
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
    
    # Build message content
    message_content = message.content or ""
    
    # Handle attachments
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
    
    # Queue message for GroupMe
    if message_content.strip():
        await message_queue.put({
            'send_func': send_to_groupme,
            'kwargs': {
                'text': message_content,
                'author_name': discord_nickname,
                'reply_context': reply_context
            },
            'type': 'discord_to_groupme',
            'retries': 0
        })
        logger.info(f"📬 Queued message for GroupMe from '{discord_nickname}'")
    
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions with queue integration"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID):
        return
    
    emoji = str(reaction.emoji)
    discord_nickname = user.display_name
    
    logger.info(f"😀 Processing reaction {emoji} from '{discord_nickname}'")
    
    # Queue reaction for GroupMe
    original_content = reaction.message.content[:50] if reaction.message.content else "a message"
    reaction_text = f"{discord_nickname} reacted {emoji} to '{original_content}...'"
    
    await message_queue.put({
        'send_func': send_to_groupme,
        'kwargs': {
            'text': reaction_text,
            'author_name': discord_nickname
        },
        'type': 'discord_reaction',
        'retries': 0
    })

# Health monitoring task
async def health_monitor():
    """Monitor bridge health and log statistics"""
    while True:
        try:
            await asyncio.sleep(300)  # Every 5 minutes
            
            with health_stats_lock:
                stats = health_stats.copy()
            
            total_messages = stats["messages_sent"] + stats["messages_failed"]
            if total_messages > 0:
                success_rate = (stats["messages_sent"] / total_messages) * 100
            else:
                success_rate = 100
            
            # Format last sync check
            sync_status = "Never checked"
            if stats.get("last_sync_check"):
                time_since = int(time.time() - stats["last_sync_check"])
                sync_status = f"{time_since // 60}m ago, {stats.get('sync_discrepancies', 0)} discrepancies"
            
            logger.info(f"""
📊 BRIDGE HEALTH REPORT:
✅ Messages sent: {stats["messages_sent"]}
❌ Messages failed: {stats["messages_failed"]}
📈 Success rate: {success_rate:.1f}%
📬 Queue size: {stats["queue_size"]}
💾 Failed messages: {len(failed_messages)}
🔍 Last sync check: {sync_status}
⏰ Last success: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stats["last_success"]))}
            """)
            
            # Alert if failure rate is high
            if success_rate < 90 and total_messages > 10:
                logger.warning(f"⚠️  High failure rate detected: {100-success_rate:.1f}%")
            
            # Alert if sync discrepancies are high
            if stats.get("sync_discrepancies", 0) > 5:
                logger.warning(f"⚠️  High sync discrepancies: {stats['sync_discrepancies']} messages out of sync")
            
            # Save failed messages periodically
            save_failed_messages()
            
        except Exception as e:
            logger.error(f"Health monitor error: {e}")

# Enhanced cleanup task
async def enhanced_cleanup():
    """Enhanced cleanup with persistence"""
    while True:
        try:
            await asyncio.sleep(3600)  # Every hour
            
            # Clean old mappings with thread safety
            with mapping_lock:
                if len(message_mapping) > 500:
                    old_keys = list(message_mapping.keys())[:-500]
                    for key in old_keys:
                        message_mapping.pop(key, None)
            
            # Clean old reply cache
            with cache_lock:
                if len(reply_context_cache) > 200:
                    old_keys = list(reply_context_cache.keys())[:-200]
                    for key in old_keys:
                        reply_context_cache.pop(key, None)
            
            # Save any pending failed messages
            save_failed_messages()
            
            logger.info("🧹 Cleanup completed")
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

# Bot Commands
@bot.command(name='status')
async def status(ctx):
    """Enhanced status command with health and sync stats"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    total_messages = stats["messages_sent"] + stats["messages_failed"]
    success_rate = (stats["messages_sent"] / total_messages * 100) if total_messages > 0 else 100
    
    # Format last sync check time
    last_sync = "Never"
    if stats["last_sync_check"]:
        last_sync = time.strftime('%H:%M:%S', time.localtime(stats["last_sync_check"]))
    
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

**🔍 Sync Status:**
⏰ Last sync check: {last_sync}
⚠️  Discrepancies found: {stats["sync_discrepancies"]}

**🔧 Enhanced Features:**
📬 Message Queue: ✅
🔄 Retry Logic: ✅ (max {MAX_RETRIES} attempts)
💾 Persistence: ✅
🔒 Thread Safety: ✅
📊 Health Monitoring: ✅
🔍 Sync Checking: ✅ (every 15 min)
🏷️ Discord Nicknames: ✅
🔗 Mention Conversion: ✅

📝 Recent Messages: {len(processed_message_ids)}
💬 Message Mappings: {len(message_mapping)}
🔗 Reply Cache: {len(reply_context_cache)}"""
    
    await ctx.send(status_msg)

@bot.command(name='sync')
async def manual_sync_check(ctx, minutes: int = 10):
    """Manually check message sync"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    if minutes < 1 or minutes > 60:
        await ctx.send("❌ Please specify minutes between 1 and 60")
        return
    
    await ctx.send(f"🔍 Checking message sync for the last {minutes} minutes...")
    
    # Run sync check
    report = await check_message_sync(lookback_minutes=minutes, limit=100)
    
    if not report:
        await ctx.send("❌ Sync check failed. Check logs for details.")
        return
    
    # Create detailed report
    embed = discord.Embed(
        title="🔍 Message Sync Report",
        color=discord.Color.green() if report['sync_rate'] > 95 else discord.Color.orange()
    )
    
    embed.add_field(
        name="📊 Overview",
        value=f"Sync Rate: **{report['sync_rate']:.1f}%**\n"
              f"Time Window: {report['time_window_minutes']} minutes\n"
              f"Matched Messages: {report['matched_messages']}",
        inline=False
    )
    
    embed.add_field(
        name="📈 Platform Messages",
        value=f"GroupMe: {report['groupme_messages']}\n"
              f"Discord: {report['discord_messages']}",
        inline=True
    )
    
    embed.add_field(
        name="⚠️  Discrepancies",
        value=f"Missing from Discord: {len(report['unmatched_groupme'])}\n"
              f"Missing from GroupMe: {len(report['unmatched_discord'])}",
        inline=True
    )
    
    # List some unmatched messages
    if report['unmatched_groupme']:
        missing_gm = []
        for msg in report['unmatched_groupme'][:3]:  # Show first 3
            text = msg.get('text', '[No text]')[:50]
            author = msg.get('name', 'Unknown')
            missing_gm.append(f"• {author}: {text}...")
        
        embed.add_field(
            name="❌ Missing from Discord",
            value="\n".join(missing_gm) or "None",
            inline=False
        )
    
    if report['unmatched_discord']:
        missing_dc = []
        for msg in report['unmatched_discord'][:3]:  # Show first 3
            text = msg.get('content', '[No text]')[:50]
            author = msg.get('author', 'Unknown')
            missing_dc.append(f"• {author}: {text}...")
        
        embed.add_field(
            name="❌ Missing from GroupMe",
            value="\n".join(missing_dc) or "None",
            inline=False
        )
    
    await ctx.send(embed=embed)
    
    # Ask about resyncing if there are discrepancies
    total_missing = len(report['unmatched_groupme']) + len(report['unmatched_discord'])
    if total_missing > 0:
        await ctx.send(f"Found {total_missing} unsynced messages. Use `!resync` to attempt to sync them.")

@bot.command(name='resync')
async def manual_resync(ctx):
    """Manually resync missing messages from last sync check"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await ctx.send("🔄 Starting resync process...")
    
    # Run a fresh sync check first
    report = await check_message_sync(lookback_minutes=15, limit=100)
    
    if not report:
        await ctx.send("❌ Sync check failed. Cannot resync.")
        return
    
    total_missing = len(report['unmatched_groupme']) + len(report['unmatched_discord'])
    
    if total_missing == 0:
        await ctx.send("✅ No missing messages found. Everything is already synced!")
        return
    
    if total_missing > 20:
        await ctx.send(f"⚠️  Found {total_missing} missing messages. This is a lot! Proceeding with caution...")
    
    # Perform resync
    resynced = await resync_missing_messages(report)
    
    await ctx.send(f"✅ Queued {resynced} messages for resync. Check status in a few moments.")

@bot.command(name='syncstats')
async def sync_statistics(ctx):
    """Show detailed sync statistics"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    # Run a quick sync check
    report = await check_message_sync(lookback_minutes=5, limit=50)
    
    if not report:
        await ctx.send("❌ Unable to generate sync statistics")
        return
    
    with health_stats_lock:
        last_check = health_stats.get("last_sync_check")
        total_discrepancies = health_stats.get("sync_discrepancies", 0)
    
    embed = discord.Embed(
        title="📊 Sync Statistics",
        color=discord.Color.blue()
    )
    
    # Current sync status
    embed.add_field(
        name="🔍 Current Status",
        value=f"Sync Rate: **{report['sync_rate']:.1f}%**\n"
              f"Last 5 min: {report['matched_messages']}/{report['groupme_messages']} messages",
        inline=False
    )
    
    # Historical data
    if last_check:
        time_since = int(time.time() - last_check)
        mins = time_since // 60
        secs = time_since % 60
        embed.add_field(
            name="⏰ Last Full Check",
            value=f"{mins}m {secs}s ago\n"
                  f"Found {total_discrepancies} discrepancies",
            inline=True
        )
    
    # Recommendations
    if report['sync_rate'] < 90:
        embed.add_field(
            name="⚠️  Recommendations",
            value="• Check network connectivity\n"
                  "• Verify API credentials\n"
                  "• Run `!resync` to fix missing messages\n"
                  "• Check logs for errors",
            inline=False
        )
    else:
        embed.add_field(
            name="✅ Health",
            value="Bridge is operating normally\n"
                  f"Automatic sync checks every 15 minutes",
            inline=False
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
        
        # Queue all failed messages for retry
        for msg in list(failed_messages):
            msg['retries'] = 0  # Reset retry count
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
    
    embed = discord.Embed(
        title="🏥 Bridge Health Report",
        color=discord.Color.green() if stats["messages_failed"] < stats["messages_sent"] * 0.1 else discord.Color.orange()
    )
    
    total = stats["messages_sent"] + stats["messages_failed"]
    success_rate = (stats["messages_sent"] / total * 100) if total > 0 else 100
    
    embed.add_field(name="✅ Success Rate", value=f"{success_rate:.1f}%", inline=True)
    embed.add_field(name="📬 Queue Size", value=stats["queue_size"], inline=True)
    embed.add_field(name="💾 Failed Messages", value=len(failed_messages), inline=True)
    
    if stats["last_failure"]:
        last_fail = time.strftime('%H:%M:%S', time.localtime(stats["last_failure"]))
        embed.add_field(name="❌ Last Failure", value=last_fail, inline=True)
    
    embed.add_field(name="📊 Total Processed", value=f"{total} messages", inline=False)
    
    await ctx.send(embed=embed)

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
    logger.info("🔍 Sync checking enabled! (every 15 minutes)")
    logger.info("🏷️ Discord nicknames enabled!")
    logger.info("🔗 Discord mention conversion enabled!")
    
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
        # Save any failed messages before exit
        save_failed_messages()

if __name__ == "__main__":
    main()
