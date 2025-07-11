#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge with Comprehensive Debugging
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
import uuid
from datetime import datetime, timedelta
from collections import defaultdict, deque
from discord.ext import commands
from aiohttp import web
import logging
from typing import Dict, Any, Optional, List, Tuple, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Generate unique instance ID to detect multiple instances
BOT_INSTANCE_ID = str(uuid.uuid4())[:8]
print(f"🔥 ENHANCED BRIDGE WITH DEBUGGING - Instance: {BOT_INSTANCE_ID}")

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

# Global State
bot_status = {"ready": False, "start_time": time.time()}
reply_context_cache = {}
recent_discord_messages = deque(maxlen=20)
recent_groupme_messages = deque(maxlen=20)

# Thread-safe locks
cache_lock = threading.Lock()
discord_messages_lock = threading.Lock()
groupme_messages_lock = threading.Lock()

# Enhanced message tracking with timestamps
processed_discord_ids = {}  # Now stores {message_id: timestamp}
processed_groupme_ids = {}  # Now stores {message_id: timestamp}
tracking_lock = threading.Lock()

# Currently processing messages (race condition prevention)
currently_processing = set()
processing_lock = threading.Lock()

# Audit log for debugging
audit_log = deque(maxlen=200)
audit_lock = threading.Lock()

# Message Queue - ONLY for GroupMe->Discord
groupme_to_discord_queue = asyncio.Queue(maxsize=100)

# Health stats
health_stats = {
    "discord_to_groupme_sent": 0,
    "groupme_to_discord_sent": 0,
    "last_discord_to_groupme": None,
    "last_groupme_to_discord": None,
    "duplicates_blocked": 0,
    "slow_requests": 0,
    "failed_requests": 0
}
health_stats_lock = threading.Lock()

# Emergency stop switch
DISCORD_TO_GROUPME_ENABLED = True

# ============================================================================
# AUDIT & DEBUG FUNCTIONS
# ============================================================================

def audit_message(event_type: str, message_id: str, details: str):
    """Add entry to audit log"""
    with audit_lock:
        audit_log.append({
            'timestamp': time.time(),
            'instance': BOT_INSTANCE_ID,
            'event': event_type,
            'message_id': str(message_id),
            'details': details,
            'thread': threading.current_thread().name
        })
        logger.debug(f"AUDIT [{BOT_INSTANCE_ID}] {event_type}: {message_id} - {details}")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

async def wait_for_bot_ready(timeout=30):
    """Wait for bot to be ready with timeout"""
    start_time = time.time()
    while not bot.is_ready() and time.time() - start_time < timeout:
        await asyncio.sleep(0.5)
    return bot.is_ready()

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

# HTTP request helper - WITH RETRIES FOR GET, NO RETRIES FOR GROUPME POST
async def make_http_request(url, method='GET', data=None, headers=None, retries=3, is_groupme_post=False):
    """HTTP request helper - no retries for GroupMe posts"""
    # GroupMe posts should never retry to avoid duplicates
    if is_groupme_post:
        retries = 1
    
    for attempt in range(retries):
        async with aiohttp.ClientSession() as session:
            try:
                start_time = time.time()
                
                if method.upper() == 'POST':
                    async with session.post(url, json=data, headers=headers, timeout=10) as response:
                        duration = time.time() - start_time
                        
                        # Log slow requests
                        if duration > 2.0:
                            logger.warning(f"⚠️ Slow request to {url}: {duration:.2f}s")
                            with health_stats_lock:
                                health_stats["slow_requests"] += 1
                        
                        result = {
                            'status': response.status,
                            'data': await response.json() if response.status in [200, 202] else None,
                            'text': await response.text(),
                            'headers': dict(response.headers),
                            'duration': duration
                        }
                        
                        # Log GroupMe responses in detail
                        if is_groupme_post:
                            logger.info(f"GroupMe Response: Status={response.status}, Duration={duration:.3f}s, Headers={result['headers']}")
                        
                        if response.status in [200, 202]:
                            return result
                else:
                    async with session.get(url, headers=headers, timeout=10) as response:
                        result = {
                            'status': response.status,
                            'data': await response.json() if response.status == 200 else None,
                            'text': await response.text()
                        }
                        if response.status == 200:
                            return result
                
                if attempt < retries - 1 and not is_groupme_post:
                    wait_time = 2 ** attempt
                    logger.warning(f"⏳ HTTP {response.status}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    with health_stats_lock:
                        health_stats["failed_requests"] += 1
                    return result
                    
            except asyncio.TimeoutError:
                logger.error(f"❌ HTTP timeout for {url}")
                with health_stats_lock:
                    health_stats["failed_requests"] += 1
                return {'status': 408, 'data': None, 'text': 'Timeout', 'duration': 10.0}
            except Exception as e:
                if attempt < retries - 1 and not is_groupme_post:
                    wait_time = 2 ** attempt
                    logger.warning(f"⏳ HTTP request error: {e}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ HTTP request failed: {e}")
                    with health_stats_lock:
                        health_stats["failed_requests"] += 1
                    return {'status': 500, 'data': None, 'text': str(e), 'duration': 0}

# Reply detection
async def detect_reply_context(data):
    """Detect if GroupMe message is a reply"""
    reply_context = None
    
    # Check for official GroupMe reply attachments
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
    
    # Check for @mentions
    if not reply_context and data.get('text'):
        text = data['text']
        mention_match = re.search(r'@(\w+)', text, re.IGNORECASE)
        if mention_match:
            mentioned_name = mention_match.group(1).lower()
            
            with discord_messages_lock:
                for discord_msg in reversed(recent_discord_messages):
                    display_name = discord_msg['author'].lower()
                    username = discord_msg.get('username', '').lower()
                    
                    if mentioned_name in display_name or mentioned_name in username:
                        reply_context = {
                            'text': discord_msg['content'],
                            'name': discord_msg['author'],
                            'type': 'mention_reply'
                        }
                        logger.info(f"✅ Found @mention reply to {reply_context['name']}")
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

# ============================================================================
# MESSAGE SENDING FUNCTIONS
# ============================================================================

async def send_to_groupme(text, author_name=None, reply_context=None, add_instance_id=False):
    """Send message to GroupMe - NO RETRIES"""
    start_time = time.time()
    
    try:
        if not GROUPME_BOT_ID:
            logger.error("❌ GROUPME_BOT_ID not configured")
            return False
        
        # Convert Discord mentions
        text = await convert_discord_mentions_to_nicknames(text)
        
        # Add instance ID for debugging if requested
        if add_instance_id:
            text = f"[{BOT_INSTANCE_ID}] {text}"
        
        # Format message
        if reply_context:
            quoted_text = reply_context.get('text', 'previous message')
            reply_author = reply_context.get('name', 'Someone')
            preview = quoted_text[:100] + '...' if len(quoted_text) > 100 else quoted_text
            preview = await convert_discord_mentions_to_nicknames(preview)
            text = f"↪️ **{author_name} replying to {reply_author}:**\n> {preview}\n\n{text}"
        else:
            if author_name and not text.startswith(author_name):
                text = f"{author_name}: {text}" if text.strip() else f"{author_name} sent content"
        
        # GroupMe message length limit
        if len(text) > 1000:
            text = text[:997] + "..."
        
        payload = {"bot_id": GROUPME_BOT_ID, "text": text}
        
        audit_message('groupme_send_start', 'N/A', f"Sending: {text[:50]}...")
        
        # NO RETRIES for GroupMe posts
        response = await make_http_request(GROUPME_POST_URL, 'POST', payload, is_groupme_post=True)
        
        duration = time.time() - start_time
        success = response['status'] == 202
        
        audit_message('groupme_send_result', 'N/A', 
                     f"Status={response['status']}, Duration={duration:.3f}s, Success={success}")
        
        with health_stats_lock:
            if success:
                health_stats["discord_to_groupme_sent"] += 1
                health_stats["last_discord_to_groupme"] = time.time()
            
        if success:
            logger.info(f"✅ Message sent to GroupMe in {duration:.3f}s: {text[:50]}...")
        else:
            logger.error(f"❌ Failed to send to GroupMe: HTTP {response['status']} in {duration:.3f}s")
            logger.error(f"Response body: {response.get('text', 'No response')[:200]}")
            
        return success
            
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ Error sending to GroupMe after {duration:.3f}s: {e}")
        audit_message('groupme_send_error', 'N/A', str(e))
        return False

async def send_to_discord(message, reply_context=None):
    """Send message to Discord"""
    try:
        if not bot.is_ready():
            logger.warning("Bot not ready, cannot send to Discord")
            return False
            
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
        try:
            if message.get('attachments'):
                for attachment in message['attachments']:
                    if attachment.get('type') == 'image' and attachment.get('url'):
                        embed = discord.Embed()
                        embed.set_image(url=attachment['url'])
                        embeds.append(embed)
        except Exception as e:
            logger.warning(f"Error processing attachments: {e}")
        
        # Send message
        try:
            formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
            
            if len(formatted_content) > 2000:
                formatted_content = formatted_content[:1997] + "..."
            
            sent_message = await asyncio.wait_for(
                discord_channel.send(formatted_content, embeds=embeds),
                timeout=10.0
            )
            
            # Store in cache for reply detection
            if message.get('id'):
                with cache_lock:
                    reply_context_cache[message['id']] = {
                        **message,
                        'discord_message_id': sent_message.id,
                        'processed_timestamp': time.time()
                    }
            
            with health_stats_lock:
                health_stats["groupme_to_discord_sent"] += 1
                health_stats["last_groupme_to_discord"] = time.time()
            
            audit_message('discord_send_success', message.get('id', 'N/A'), f"From {author}")
            logger.info(f"✅ Message sent to Discord: {content[:50]}...")
            return True
            
        except asyncio.TimeoutError:
            logger.error("❌ Discord send timeout after 10 seconds")
            audit_message('discord_send_timeout', message.get('id', 'N/A'), "Timeout")
            return False
        except Exception as e:
            logger.error(f"❌ Discord send error: {e}")
            audit_message('discord_send_error', message.get('id', 'N/A'), str(e))
            return False
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        return False

# ============================================================================
# QUEUE PROCESSOR - ONLY FOR GROUPME->DISCORD
# ============================================================================

async def groupme_to_discord_processor():
    """Process GroupMe->Discord messages from queue"""
    logger.info("📬 Starting GroupMe->Discord queue processor...")
    
    while True:
        try:
            msg_data = await groupme_to_discord_queue.get()
            
            message = msg_data['message']
            reply_context = msg_data.get('reply_context')
            
            logger.info(f"📤 Processing queued GroupMe message from {message.get('name', 'Unknown')}")
            
            success = await send_to_discord(message, reply_context)
            
            if not success:
                logger.error(f"❌ Failed to send queued message to Discord")
                
        except Exception as e:
            logger.error(f"❌ Queue processor error: {e}")
            await asyncio.sleep(1)

# ============================================================================
# WEBHOOK SERVER
# ============================================================================

async def run_webhook_server():
    """Webhook server for receiving GroupMe messages"""
    
    async def health_check(request):
        with health_stats_lock:
            stats = health_stats.copy()
        
        with tracking_lock:
            tracking_info = {
                "discord_messages_tracked": len(processed_discord_ids),
                "groupme_messages_tracked": len(processed_groupme_ids)
            }
        
        with audit_lock:
            recent_audit = list(audit_log)[-10:]
        
        return web.json_response({
            "status": "healthy",
            "instance_id": BOT_INSTANCE_ID,
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "health_stats": stats,
            "tracking": tracking_info,
            "discord_to_groupme_enabled": DISCORD_TO_GROUPME_ENABLED,
            "recent_audit": [
                {
                    "time": time.strftime('%H:%M:%S', time.localtime(a['timestamp'])),
                    "event": a['event'],
                    "message_id": a['message_id'],
                    "details": a['details']
                } for a in recent_audit
            ]
        })
    
    async def groupme_webhook(request):
        """Handle incoming GroupMe messages"""
        try:
            data = await request.json()
            message_id = data.get('id')
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            
            logger.info(f"📨 GroupMe webhook: {sender_info} - ID: {message_id}")
            audit_message('groupme_webhook_received', message_id, sender_info)
            
            # Enhanced duplicate check with timestamp
            current_time = time.time()
            with tracking_lock:
                if message_id in processed_groupme_ids:
                    last_time = processed_groupme_ids[message_id]
                    time_diff = current_time - last_time
                    logger.warning(f"🚫 Duplicate GroupMe message blocked: {message_id} (last seen {time_diff:.3f}s ago)")
                    audit_message('groupme_duplicate_blocked', message_id, f"After {time_diff:.3f}s")
                    with health_stats_lock:
                        health_stats["duplicates_blocked"] += 1
                    return web.json_response({"status": "ignored", "reason": "duplicate"})
                processed_groupme_ids[message_id] = current_time
                
                # Cleanup old entries
                if len(processed_groupme_ids) > 1000:
                    sorted_ids = sorted(processed_groupme_ids.items(), key=lambda x: x[1])
                    for old_id, _ in sorted_ids[:200]:
                        del processed_groupme_ids[old_id]
            
            # Bot message filter
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
                audit_message('groupme_bot_ignored', message_id, "Bot message")
                return web.json_response({"status": "ignored", "reason": "bot_message"})
            
            logger.info(f"✅ Processing GroupMe message: {message_id}")
            
            # Store in recent messages
            if message_id:
                with groupme_messages_lock:
                    recent_groupme_messages.append({
                        'id': message_id,
                        'text': data.get('text', ''),
                        'name': data.get('name', ''),
                        'created_at': data.get('created_at', time.time())
                    })
                with cache_lock:
                    reply_context_cache[message_id] = data
            
            # Check if bot is ready
            if not bot.is_ready():
                logger.warning("⏳ Bot not ready, waiting...")
                ready = await wait_for_bot_ready(timeout=3)
                if not ready:
                    logger.error("❌ Bot still not ready")
                    audit_message('groupme_bot_not_ready', message_id, "Bot not ready")
                    return web.json_response({"status": "error", "reason": "bot_not_ready"})
            
            # Detect reply context
            try:
                reply_context = await asyncio.wait_for(
                    detect_reply_context(data),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.warning("Reply context detection timed out")
                reply_context = None
            except Exception as e:
                logger.warning(f"Reply context detection error: {e}")
                reply_context = None
            
            # Queue for processing
            await groupme_to_discord_queue.put({
                'message': data,
                'reply_context': reply_context
            })
            
            audit_message('groupme_queued', message_id, "Queued for Discord")
            return web.json_response({"status": "queued"})
            
        except Exception as e:
            logger.error(f"❌ Error handling GroupMe webhook: {e}")
            audit_message('groupme_webhook_error', 'N/A', str(e))
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
    
    logger.info(f"🌐 Webhook server running on 0.0.0.0:{PORT}")
    logger.info(f"🆔 Instance ID: {BOT_INSTANCE_ID}")
    
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

# ============================================================================
# DISCORD BOT EVENTS
# ============================================================================

@bot.event
async def on_ready():
    """Bot ready event"""
    global bot_status
    bot_status["ready"] = True
    
    logger.info(f'🤖 {bot.user} connected to Discord!')
    logger.info(f'🆔 Instance ID: {BOT_INSTANCE_ID}')
    logger.info(f'📺 Channel ID: {DISCORD_CHANNEL_ID}')
    logger.info(f'✅ Discord→GroupMe: DIRECT (no queue, no retries)')
    logger.info(f'✅ GroupMe→Discord: Simple queue')
    logger.info(f'✅ Enhanced debugging features enabled')
    
    # Start the GroupMe->Discord processor
    asyncio.create_task(groupme_to_discord_processor())

@bot.event
async def on_message(message):
    """Handle Discord messages with enhanced debugging"""
    try:
        # Generate request ID for tracking
        request_id = f"{message.id}_{time.time()}"
        logger.info(f"🔍 START processing Discord message {message.id} - Request: {request_id}")
        audit_message('discord_message_start', message.id, f"Request: {request_id}")
        
        # Basic filters
        if message.author.bot:
            audit_message('discord_bot_ignored', message.id, "Bot author")
            await bot.process_commands(message)
            return
            
        if message.channel.id != DISCORD_CHANNEL_ID:
            audit_message('discord_wrong_channel', message.id, f"Channel: {message.channel.id}")
            await bot.process_commands(message)
            return
        
        if message.content.startswith('!'):
            audit_message('discord_command', message.id, "Command")
            await bot.process_commands(message)
            return
        
        # Emergency stop check
        if not DISCORD_TO_GROUPME_ENABLED:
            logger.info("Discord→GroupMe forwarding is disabled")
            audit_message('discord_forwarding_disabled', message.id, "Forwarding disabled")
            await bot.process_commands(message)
            return
        
        message_id = str(message.id)
        current_time = time.time()
        
        # Check if already processing (race condition prevention)
        with processing_lock:
            if message_id in currently_processing:
                logger.warning(f"🚫 Already processing {message_id}")
                audit_message('discord_already_processing', message_id, "Race condition")
                return
            currently_processing.add(message_id)
        
        try:
            # Enhanced duplicate check with timestamp
            with tracking_lock:
                if message_id in processed_discord_ids:
                    last_time = processed_discord_ids[message_id]
                    time_diff = current_time - last_time
                    logger.warning(f"🚫 Duplicate Discord message blocked: {message_id} (last seen {time_diff:.3f}s ago)")
                    audit_message('discord_duplicate_blocked', message_id, f"After {time_diff:.3f}s")
                    with health_stats_lock:
                        health_stats["duplicates_blocked"] += 1
                    return
                processed_discord_ids[message_id] = current_time
                
                # Cleanup old entries
                if len(processed_discord_ids) > 1000:
                    sorted_ids = sorted(processed_discord_ids.items(), key=lambda x: x[1])
                    for old_id, _ in sorted_ids[:200]:
                        del processed_discord_ids[old_id]
            
            # Get message info
            discord_nickname = message.author.display_name
            message_content = message.content or ""
            
            # Handle attachments
            if message.attachments:
                if message_content:
                    message_content += " [Attachment]"
                else:
                    message_content = "[Attachment]"
            
            # Skip empty messages
            if not message_content.strip():
                logger.info(f"⏩ Skipping empty message {message_id}")
                audit_message('discord_empty_skipped', message_id, "Empty message")
                return
            
            # Simple reply detection
            reply_context = None
            if message.reference and message.reference.message_id:
                try:
                    replied_message = await message.channel.fetch_message(message.reference.message_id)
                    reply_context = {
                        'text': replied_message.content[:100],
                        'name': replied_message.author.display_name,
                        'type': 'reply'
                    }
                    audit_message('discord_reply_detected', message_id, f"Reply to {replied_message.author.display_name}")
                except:
                    pass
            
            # Store in recent messages
            with discord_messages_lock:
                recent_discord_messages.append({
                    'content': message.content,
                    'author': discord_nickname,
                    'username': message.author.name,
                    'author_id': message.author.id,
                    'timestamp': time.time(),
                    'message_id': message.id
                })
            
            # Send DIRECTLY to GroupMe
            logger.info(f"📤 Sending Discord message {message_id} directly to GroupMe")
            audit_message('discord_sending_to_groupme', message_id, f"From {discord_nickname}")
            
            success = await send_to_groupme(
                text=message_content,
                author_name=discord_nickname,
                reply_context=reply_context
            )
            
            if success:
                logger.info(f"✅ Discord->GroupMe success for message {message_id}")
                audit_message('discord_sent_success', message_id, "Sent to GroupMe")
            else:
                logger.error(f"❌ Discord->GroupMe failed for message {message_id}")
                audit_message('discord_sent_failed', message_id, "Failed to send")
            
        finally:
            # Always remove from processing set
            with processing_lock:
                currently_processing.discard(message_id)
            
            logger.info(f"🔍 END processing Discord message {message_id} - Request: {request_id}")
            audit_message('discord_message_end', message_id, f"Request: {request_id}")
        
        await bot.process_commands(message)
        
    except Exception as e:
        logger.error(f"❌ Error in Discord message handler: {e}")
        audit_message('discord_handler_error', str(message.id), str(e))
        await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID):
        return
    
    if not DISCORD_TO_GROUPME_ENABLED:
        return
    
    emoji = str(reaction.emoji)
    discord_nickname = user.display_name
    
    logger.info(f"😀 Processing reaction {emoji} from '{discord_nickname}'")
    
    original_content = reaction.message.content[:50] if reaction.message.content else "a message"
    reaction_text = f"{discord_nickname} reacted {emoji} to '{original_content}...'"
    
    # Send directly to GroupMe
    await send_to_groupme(
        text=reaction_text,
        author_name=discord_nickname
    )

# ============================================================================
# BOT COMMANDS
# ============================================================================

@bot.command(name='status')
async def status(ctx):
    """Bridge status command"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    with tracking_lock:
        discord_tracked = len(processed_discord_ids)
        groupme_tracked = len(processed_groupme_ids)
    
    with processing_lock:
        currently_proc = len(currently_processing)
    
    status_msg = f"""🟢 **Bridge Status**
🆔 Instance: `{BOT_INSTANCE_ID}`
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🌐 Webhook Server: ✅
🚦 Discord→GroupMe: {'✅ ENABLED' if DISCORD_TO_GROUPME_ENABLED else '🛑 DISABLED'}

**📊 Message Statistics:**
Discord→GroupMe sent: {stats["discord_to_groupme_sent"]}
GroupMe→Discord sent: {stats["groupme_to_discord_sent"]}
Duplicates blocked: {stats["duplicates_blocked"]}
Slow requests (>2s): {stats["slow_requests"]}
Failed requests: {stats["failed_requests"]}

**🔍 Tracking:**
Discord messages: {discord_tracked}
GroupMe messages: {groupme_tracked}
Currently processing: {currently_proc}

**⏰ Last Activity:**
Discord→GroupMe: {time.strftime('%H:%M:%S', time.localtime(stats['last_discord_to_groupme'])) if stats['last_discord_to_groupme'] else 'Never'}
GroupMe→Discord: {time.strftime('%H:%M:%S', time.localtime(stats['last_groupme_to_discord'])) if stats['last_groupme_to_discord'] else 'Never'}"""
    
    await ctx.send(status_msg)

@bot.command(name='audit')
async def show_audit(ctx, count: int = 10):
    """Show audit log"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    if count > 50:
        count = 50
    
    with audit_lock:
        recent = list(audit_log)[-count:]
    
    if not recent:
        await ctx.send("📊 No audit entries yet")
        return
    
    embed = discord.Embed(
        title=f"📊 Last {len(recent)} Audit Entries",
        color=discord.Color.blue()
    )
    
    for entry in recent:
        time_str = time.strftime('%H:%M:%S', time.localtime(entry['timestamp']))
        field_name = f"{time_str} - {entry['event']}"
        field_value = f"ID: {entry['message_id'][:8]}...\n{entry['details'][:50]}"
        if entry['instance'] != BOT_INSTANCE_ID:
            field_value += f"\n⚠️ Different instance: {entry['instance']}"
        embed.add_field(name=field_name, value=field_value, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name='debug_discord')
async def debug_discord(ctx):
    """Show last Discord messages processed"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with tracking_lock:
        # Get last 10 Discord messages with timestamps
        sorted_msgs = sorted(processed_discord_ids.items(), key=lambda x: x[1], reverse=True)[:10]
    
    embed = discord.Embed(
        title="🔍 Last 10 Discord Messages Processed",
        color=discord.Color.blue()
    )
    
    current_time = time.time()
    for msg_id, timestamp in sorted_msgs:
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp))
        age = current_time - timestamp
        embed.add_field(
            name=f"Message {msg_id}",
            value=f"Time: {time_str}\nAge: {age:.1f}s ago",
            inline=True
        )
    
    await ctx.send(embed=embed)

@bot.command(name='stop_forward')
async def stop_forward(ctx):
    """Stop Discord→GroupMe forwarding"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    global DISCORD_TO_GROUPME_ENABLED
    DISCORD_TO_GROUPME_ENABLED = False
    
    audit_message('forwarding_stopped', 'N/A', f"Stopped by {ctx.author}")
    await ctx.send("🛑 Discord→GroupMe forwarding STOPPED")

@bot.command(name='start_forward')
async def start_forward(ctx):
    """Start Discord→GroupMe forwarding"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    global DISCORD_TO_GROUPME_ENABLED
    DISCORD_TO_GROUPME_ENABLED = True
    
    audit_message('forwarding_started', 'N/A', f"Started by {ctx.author}")
    await ctx.send("✅ Discord→GroupMe forwarding STARTED")

@bot.command(name='test_instance')
async def test_instance(ctx):
    """Test with instance ID"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    test_msg = f"Test from instance {BOT_INSTANCE_ID} at {time.strftime('%H:%M:%S')}"
    await ctx.send(f"🧪 Sending with instance ID: {test_msg}")
    
    success = await send_to_groupme(
        text=test_msg,
        author_name="INSTANCE_TEST",
        add_instance_id=True
    )
    
    if success:
        await ctx.send(f"✅ Test sent with instance ID: {BOT_INSTANCE_ID}")
    else:
        await ctx.send("❌ Test failed")

@bot.command(name='find_duplicates')
async def find_duplicates(ctx):
    """Check for multiple bot instances"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await ctx.send(f"🔍 Current instance: `{BOT_INSTANCE_ID}`\n"
                  f"Sending test messages to detect other instances...")
    
    # Send a few test messages
    for i in range(3):
        test_msg = f"Instance detection {i+1}/3 from {BOT_INSTANCE_ID}"
        await send_to_groupme(
            text=test_msg,
            author_name="DUPLICATE_CHECK",
            add_instance_id=True
        )
        await asyncio.sleep(1)
    
    await ctx.send("Check GroupMe for messages. If you see messages from different instance IDs, "
                  "you have multiple bots running!")

@bot.command(name='reset_tracking')
async def reset_tracking(ctx):
    """Reset message tracking"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with tracking_lock:
        discord_count = len(processed_discord_ids)
        groupme_count = len(processed_groupme_ids)
        
        processed_discord_ids.clear()
        processed_groupme_ids.clear()
    
    with processing_lock:
        proc_count = len(currently_processing)
        currently_processing.clear()
    
    audit_message('tracking_reset', 'N/A', f"Reset by {ctx.author}")
    
    await ctx.send(f"🧹 Tracking reset!\n"
                  f"Cleared {discord_count} Discord IDs\n"
                  f"Cleared {groupme_count} GroupMe IDs\n"
                  f"Cleared {proc_count} processing locks")

@bot.command(name='health')
async def health_report(ctx):
    """Detailed health report"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    embed = discord.Embed(
        title="🏥 Bridge Health Report",
        color=discord.Color.green()
    )
    
    embed.add_field(name="🆔 Instance", value=f"`{BOT_INSTANCE_ID}`", inline=True)
    embed.add_field(name="⏱️ Uptime", value=f"{(time.time() - bot_status['start_time']) / 3600:.1f}h", inline=True)
    embed.add_field(name="🚦 Forwarding", value="✅" if DISCORD_TO_GROUPME_ENABLED else "🛑", inline=True)
    
    total_sent = stats["discord_to_groupme_sent"] + stats["groupme_to_discord_sent"]
    
    embed.add_field(name="📤 Total Messages", value=total_sent, inline=True)
    embed.add_field(name="🚫 Duplicates Blocked", value=stats["duplicates_blocked"], inline=True)
    
    if total_sent > 0:
        duplicate_rate = (stats["duplicates_blocked"] / (total_sent + stats["duplicates_blocked"])) * 100
        embed.add_field(name="📊 Duplicate Rate", value=f"{duplicate_rate:.1f}%", inline=True)
    
    embed.add_field(name="⚠️ Slow Requests", value=stats["slow_requests"], inline=True)
    embed.add_field(name="❌ Failed Requests", value=stats["failed_requests"], inline=True)
    
    # Calculate success rate
    total_attempts = total_sent + stats["failed_requests"]
    if total_attempts > 0:
        success_rate = (total_sent / total_attempts) * 100
        embed.add_field(name="✅ Success Rate", value=f"{success_rate:.1f}%", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='clear_all')
async def clear_all(ctx):
    """Clear all caches and tracking"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    # Clear tracking
    with tracking_lock:
        processed_discord_ids.clear()
        processed_groupme_ids.clear()
    
    # Clear processing
    with processing_lock:
        currently_processing.clear()
    
    # Clear caches
    with cache_lock:
        reply_context_cache.clear()
    
    with discord_messages_lock:
        recent_discord_messages.clear()
    
    with groupme_messages_lock:
        recent_groupme_messages.clear()
    
    # Clear audit log
    with audit_lock:
        audit_log.clear()
    
    audit_message('clear_all', 'N/A', f"Cleared by {ctx.author}")
    
    await ctx.send("🧹 All caches and tracking cleared!")

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main entry point"""
    if not DISCORD_BOT_TOKEN:
        logger.error("❌ DISCORD_BOT_TOKEN required!")
        return
    
    if not GROUPME_BOT_ID:
        logger.error("❌ GROUPME_BOT_ID required!")
        return
    
    if DISCORD_CHANNEL_ID == 0:
        logger.error("❌ DISCORD_CHANNEL_ID required!")
        return
    
    logger.info("🚀 Starting Enhanced Bridge with Debugging...")
    logger.info(f"🆔 Instance ID: {BOT_INSTANCE_ID}")
    logger.info("✅ Discord→GroupMe: DIRECT (no queue, no retries)")
    logger.info("✅ GroupMe→Discord: Simple queue")
    logger.info("✅ Enhanced duplicate detection with timestamps")
    logger.info("✅ Audit logging enabled")
    logger.info("✅ Race condition prevention")
    logger.info("✅ Emergency stop switch")
    
    # Start webhook server
    webhook_thread = start_webhook_server()
    time.sleep(2)
    
    # Start bot
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_bot():
            """Run bot"""
            try:
                await bot.start(DISCORD_BOT_TOKEN)
            except Exception as e:
                logger.error(f"❌ Bot error: {e}")
                raise
        
        try:
            loop.run_until_complete(run_bot())
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
        except Exception as e:
            logger.error(f"❌ Runtime error: {e}")
        finally:
            if not bot.is_closed():
                loop.run_until_complete(bot.close())
            loop.close()
            logger.info("🔚 Shutdown complete")
            
    except Exception as e:
        logger.error(f"❌ Critical error: {e}")

if __name__ == "__main__":
    main()
