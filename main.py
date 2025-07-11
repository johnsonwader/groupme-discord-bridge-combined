#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge with Fixed Discord->GroupMe Flow
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("🔥 FIXED BIDIRECTIONAL BRIDGE - NO DISCORD->GROUPME DUPLICATES!")

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

# SIMPLE Message Tracking - Just IDs, nothing fancy
processed_discord_ids = set()
processed_groupme_ids = set()
tracking_lock = threading.Lock()

# Message Queue - ONLY for GroupMe->Discord now
groupme_to_discord_queue = asyncio.Queue(maxsize=100)

# Health stats
health_stats = {
    "discord_to_groupme_sent": 0,
    "groupme_to_discord_sent": 0,
    "last_discord_to_groupme": None,
    "last_groupme_to_discord": None,
    "duplicates_blocked": 0
}
health_stats_lock = threading.Lock()

# Disable all sync verification to prevent duplicates
SYNC_VERIFICATION_ENABLED = False

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

# HTTP request helper
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
                    wait_time = 2 ** attempt
                    logger.warning(f"⏳ HTTP {response.status}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    return result
                    
            except Exception as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"⏳ HTTP request error: {e}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"❌ HTTP request failed after {retries} attempts: {e}")
                    return {'status': 500, 'data': None, 'text': str(e)}

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
# MESSAGE SENDING FUNCTIONS - SIMPLIFIED
# ============================================================================

async def send_to_groupme(text, author_name=None, reply_context=None):
    """Send message to GroupMe - DIRECT, NO QUEUE"""
    try:
        if not GROUPME_BOT_ID:
            logger.error("❌ GROUPME_BOT_ID not configured")
            return False
        
        # Convert Discord mentions
        text = await convert_discord_mentions_to_nicknames(text)
        
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
        
        logger.debug(f"Sending to GroupMe: {payload}")
        response = await make_http_request(GROUPME_POST_URL, 'POST', payload)
        
        # GroupMe bot API returns 202 for success
        success = response['status'] == 202
        
        with health_stats_lock:
            if success:
                health_stats["discord_to_groupme_sent"] += 1
                health_stats["last_discord_to_groupme"] = time.time()
            
        if success:
            logger.info(f"✅ Message sent to GroupMe: {text[:50]}...")
        else:
            logger.error(f"❌ Failed to send to GroupMe: HTTP {response['status']}")
            
        return success
            
    except Exception as e:
        logger.error(f"❌ Error sending to GroupMe: {e}")
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
            
            logger.info(f"✅ Message sent to Discord: {content[:50]}...")
            return True
            
        except asyncio.TimeoutError:
            logger.error("❌ Discord send timeout after 10 seconds")
            return False
        except Exception as e:
            logger.error(f"❌ Discord send error: {e}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        return False

# ============================================================================
# SIMPLE QUEUE PROCESSOR - ONLY FOR GROUPME->DISCORD
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
        
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "health_stats": stats,
            "tracking": tracking_info,
            "sync_verification": "DISABLED",
            "features": {
                "discord_to_groupme": "DIRECT (no queue)",
                "groupme_to_discord": "QUEUED",
                "duplicate_detection": "SIMPLE (ID only)",
                "sync_verification": "DISABLED"
            }
        })
    
    async def groupme_webhook(request):
        """Handle incoming GroupMe messages"""
        try:
            data = await request.json()
            message_id = data.get('id')
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            
            logger.info(f"📨 GroupMe webhook: {sender_info} - ID: {message_id}")
            
            # Simple duplicate check
            with tracking_lock:
                if message_id in processed_groupme_ids:
                    logger.warning(f"🚫 Duplicate GroupMe message blocked: {message_id}")
                    with health_stats_lock:
                        health_stats["duplicates_blocked"] += 1
                    return web.json_response({"status": "ignored", "reason": "duplicate"})
                processed_groupme_ids.add(message_id)
                
                # Keep set size manageable
                if len(processed_groupme_ids) > 1000:
                    # Remove oldest ~200 entries
                    excess = list(processed_groupme_ids)[:200]
                    for old_id in excess:
                        processed_groupme_ids.discard(old_id)
            
            # Bot message filter
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
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
    
    logger.info(f"🌐 Webhook server running on 0.0.0.0:{PORT}")
    
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
    logger.info(f'📺 Channel ID: {DISCORD_CHANNEL_ID}')
    logger.info(f'✅ FIXED: Discord->GroupMe is now DIRECT (no queue)')
    logger.info(f'✅ FIXED: Sync verification DISABLED')
    logger.info(f'✅ FIXED: Simple duplicate detection only')
    
    # Start the GroupMe->Discord processor
    asyncio.create_task(groupme_to_discord_processor())

@bot.event
async def on_message(message):
    """Handle Discord messages - ULTRA SIMPLE"""
    try:
        # Basic filters
        if message.author.bot:
            await bot.process_commands(message)
            return
            
        if message.channel.id != DISCORD_CHANNEL_ID:
            await bot.process_commands(message)
            return
        
        if message.content.startswith('!'):
            await bot.process_commands(message)
            return
        
        message_id = str(message.id)
        
        # Simple duplicate check
        with tracking_lock:
            if message_id in processed_discord_ids:
                logger.warning(f"🚫 Duplicate Discord message blocked: {message_id}")
                with health_stats_lock:
                    health_stats["duplicates_blocked"] += 1
                await bot.process_commands(message)
                return
            processed_discord_ids.add(message_id)
            
            # Keep set size manageable
            if len(processed_discord_ids) > 1000:
                excess = list(processed_discord_ids)[:200]
                for old_id in excess:
                    processed_discord_ids.discard(old_id)
        
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
            await bot.process_commands(message)
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
            except:
                pass
        
        # Store in recent messages (for reply detection from GroupMe)
        with discord_messages_lock:
            recent_discord_messages.append({
                'content': message.content,
                'author': discord_nickname,
                'username': message.author.name,
                'author_id': message.author.id,
                'timestamp': time.time(),
                'message_id': message.id
            })
        
        # Send DIRECTLY to GroupMe - NO QUEUE, NO COMPLEX LOGIC
        logger.info(f"📤 Sending Discord message {message_id} directly to GroupMe")
        
        success = await send_to_groupme(
            text=message_content,
            author_name=discord_nickname,
            reply_context=reply_context
        )
        
        if success:
            logger.info(f"✅ Discord->GroupMe success for message {message_id}")
        else:
            logger.error(f"❌ Discord->GroupMe failed for message {message_id}")
        
        await bot.process_commands(message)
        
    except Exception as e:
        logger.error(f"❌ Error in Discord message handler: {e}")
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
    
    status_msg = f"""🟢 **Bridge Status (FIXED)**
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🌐 Webhook Server: ✅

**📊 Message Statistics:**
Discord→GroupMe sent: {stats["discord_to_groupme_sent"]}
GroupMe→Discord sent: {stats["groupme_to_discord_sent"]}
Duplicates blocked: {stats["duplicates_blocked"]}

**🔍 Tracking:**
Discord messages: {discord_tracked}
GroupMe messages: {groupme_tracked}

**✅ FIXES APPLIED:**
• Discord→GroupMe: DIRECT (no queue)
• Sync verification: DISABLED
• Duplicate detection: SIMPLE (ID only)
• Message queue: REMOVED for Discord→GroupMe

**⏰ Last Activity:**
Discord→GroupMe: {time.strftime('%H:%M:%S', time.localtime(stats['last_discord_to_groupme'])) if stats['last_discord_to_groupme'] else 'Never'}
GroupMe→Discord: {time.strftime('%H:%M:%S', time.localtime(stats['last_groupme_to_discord'])) if stats['last_groupme_to_discord'] else 'Never'}"""
    
    await ctx.send(status_msg)

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
    
    await ctx.send(f"🧹 Tracking reset!\nCleared {discord_count} Discord and {groupme_count} GroupMe message IDs")

@bot.command(name='health')
async def health_report(ctx):
    """Health report"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with health_stats_lock:
        stats = health_stats.copy()
    
    embed = discord.Embed(
        title="🏥 Bridge Health Report",
        color=discord.Color.green()
    )
    
    total_sent = stats["discord_to_groupme_sent"] + stats["groupme_to_discord_sent"]
    
    embed.add_field(name="📤 Total Messages", value=total_sent, inline=True)
    embed.add_field(name="🚫 Duplicates Blocked", value=stats["duplicates_blocked"], inline=True)
    
    if total_sent > 0:
        duplicate_rate = (stats["duplicates_blocked"] / (total_sent + stats["duplicates_blocked"])) * 100
        embed.add_field(name="📊 Duplicate Rate", value=f"{duplicate_rate:.1f}%", inline=True)
    
    embed.add_field(name="Discord→GroupMe", value=stats["discord_to_groupme_sent"], inline=True)
    embed.add_field(name="GroupMe→Discord", value=stats["groupme_to_discord_sent"], inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name='test_discord_send')
async def test_discord_send(ctx):
    """Test sending a message to GroupMe"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    test_time = time.strftime('%H:%M:%S')
    test_msg = f"Test message to GroupMe at {test_time}"
    
    await ctx.send(f"🧪 Sending test: {test_msg}")
    
    success = await send_to_groupme(
        text=test_msg,
        author_name="TEST"
    )
    
    if success:
        await ctx.send("✅ Test successful - check GroupMe for EXACTLY ONE message")
    else:
        await ctx.send("❌ Test failed")

@bot.command(name='clear_cache')
async def clear_cache(ctx):
    """Clear all caches"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    with cache_lock:
        cache_size = len(reply_context_cache)
        reply_context_cache.clear()
    
    with discord_messages_lock:
        discord_size = len(recent_discord_messages)
        recent_discord_messages.clear()
    
    with groupme_messages_lock:
        groupme_size = len(recent_groupme_messages)
        recent_groupme_messages.clear()
    
    await ctx.send(f"🧹 Caches cleared!\nReply cache: {cache_size}\nRecent Discord: {discord_size}\nRecent GroupMe: {groupme_size}")

@bot.command(name='queue_status')
async def queue_status(ctx):
    """Show queue status"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    queue_size = groupme_to_discord_queue.qsize()
    
    await ctx.send(f"📬 GroupMe→Discord queue size: {queue_size}\n"
                  f"Note: Discord→GroupMe messages are sent DIRECTLY (no queue)")

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
    
    logger.info("🚀 Starting FIXED GroupMe-Discord Bridge...")
    logger.info("✅ Discord→GroupMe: DIRECT (no queue, no duplicates)")
    logger.info("✅ GroupMe→Discord: Simple queue")
    logger.info("✅ Sync verification: DISABLED")
    logger.info("✅ Duplicate detection: SIMPLE (ID tracking only)")
    
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
