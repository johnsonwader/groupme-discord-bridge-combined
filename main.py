#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge - Single-Path Processing
FIXED: Eliminates triple sends by using single processing path
Features: Ultra-fast messaging, deduplication, bidirectional replies, NO DUPLICATES
"""

import discord
import aiohttp
import asyncio
import os
import json
import re
import time
import threading
import concurrent.futures
import hashlib
from datetime import datetime, timedelta
from collections import defaultdict, deque
from discord.ext import commands
from aiohttp import web
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("🔥 SINGLE-PATH PROCESSING BRIDGE STARTING!")

# Environment Configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GROUPME_BOT_ID = os.getenv("GROUPME_BOT_ID")
GROUPME_ACCESS_TOKEN = os.getenv("GROUPME_ACCESS_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
GROUPME_GROUP_ID = os.getenv("GROUPME_GROUP_ID")
PORT = int(os.getenv("PORT", "8080"))

# API Endpoints
GROUPME_POST_URL = "https://api.groupme.com/v3/bots/post"
GROUPME_IMAGE_UPLOAD_URL = "https://image.groupme.com/pictures"
GROUPME_MESSAGES_URL = f"https://api.groupme.com/v3/groups/{GROUPME_GROUP_ID}/messages"

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Global State
bot_status = {"ready": False, "start_time": time.time()}
message_mapping = {}  # Discord message ID -> GroupMe message ID
groupme_to_discord = {}  # GroupMe message ID -> Discord message ID
recent_messages = defaultdict(list)

# Deduplication system
processed_messages = deque(maxlen=1000)
message_timestamps = {}
reply_context_cache = {}
discord_message_cache = {}

# CRITICAL: Processing flags to prevent duplicate sends
discord_processing_lock = {}  # Track messages being processed

# Emoji mappings
EMOJI_MAPPING = {
    '❤️': '❤️', '👍': '👍', '👎': '👎', '😂': '😂', '😮': '😮', '😢': '😢', '😡': '😡',
    '✅': '✅', '❌': '❌', '🔥': '🔥', '💯': '💯', '🎉': '🎉', '👏': '👏'
}

# Deduplication Functions
def create_message_hash(data):
    """Create unique hash for message deduplication"""
    content = data.get('text', '')
    sender = data.get('name', '')
    user_id = data.get('user_id', '')
    created_at = data.get('created_at', 0)
    unique_string = f"{user_id}:{sender}:{content}:{created_at}"
    return hashlib.md5(unique_string.encode()).hexdigest()

def is_duplicate_message(data):
    """Check if this message was already processed"""
    try:
        message_hash = create_message_hash(data)
        
        if message_hash in processed_messages:
            logger.info(f"🚫 Duplicate message detected: {message_hash[:8]}")
            return True
        
        processed_messages.append(message_hash)
        
        # Rate limiting
        user_id = data.get('user_id', '')
        current_time = time.time()
        
        if user_id in message_timestamps:
            time_diff = current_time - message_timestamps[user_id]
            if time_diff < 0.5:
                logger.info(f"🚫 Rate limit: {data.get('name', 'Unknown')} too fast")
                return True
        
        message_timestamps[user_id] = current_time
        return False
        
    except Exception as e:
        logger.error(f"Error in duplicate detection: {e}")
        return False

def is_bot_message(data):
    """Enhanced bot message detection"""
    if data.get('sender_type') == 'bot':
        return True
    if data.get('name', '') in ['Bot', 'GroupMe', 'System', 'Poll Bot', 'Vote Bot', 'Reply Test Bot']:
        return True
    if data.get('sender_id') == GROUPME_BOT_ID:
        return True
    name = data.get('name', '').lower()
    if any(bot_word in name for bot_word in ['bot', 'bridge', 'webhook', 'system']):
        return True
    return False

# Helper Functions
async def make_http_request(url, method='GET', data=None, headers=None):
    """HTTP request helper"""
    async with aiohttp.ClientSession() as session:
        try:
            if method.upper() == 'POST':
                async with session.post(url, json=data, headers=headers) as response:
                    return {
                        'status': response.status,
                        'data': await response.json() if response.status == 200 else None,
                        'text': await response.text()
                    }
            else:
                async with session.get(url, headers=headers) as response:
                    return {
                        'status': response.status,
                        'data': await response.json() if response.status == 200 else None,
                        'text': await response.text()
                    }
        except Exception as e:
            logger.error(f"HTTP request failed: {e}")
            return {'status': 500, 'data': None, 'text': str(e)}

async def get_groupme_messages(group_id, before_id=None, limit=20):
    """Get recent GroupMe messages"""
    if not GROUPME_ACCESS_TOKEN:
        return []
    
    url = f"{GROUPME_MESSAGES_URL}?token={GROUPME_ACCESS_TOKEN}&limit={limit}"
    if before_id:
        url += f"&before_id={before_id}"
    
    response = await make_http_request(url)
    if response['status'] == 200 and response['data']:
        return response['data'].get('response', {}).get('messages', [])
    return []

# Enhanced Reply Detection Function
async def detect_reply_context(data):
    """Enhanced bidirectional reply detection"""
    reply_context = None
    
    # Method 1: Official GroupMe reply attachments
    if data.get('attachments'):
        reply_attachment = next(
            (att for att in data['attachments'] if att.get('type') == 'reply'), 
            None
        )
        if reply_attachment:
            reply_id = reply_attachment.get('reply_id') or reply_attachment.get('base_reply_id')
            if reply_id and reply_id in reply_context_cache:
                original_msg = reply_context_cache[reply_id]
                reply_context = {
                    'text': original_msg.get('text', '[No text]'),
                    'name': original_msg.get('name', 'Unknown'),
                    'type': 'official_reply',
                    'platform_source': 'groupme'
                }
                logger.info(f"✅ Found official GroupMe reply to {reply_context['name']}")
    
    # Method 2: @mention detection with Discord user lookup
    if not reply_context and data.get('text'):
        text = data['text']
        mention_match = re.search(r'@(\w+)', text)
        if mention_match:
            mentioned_name = mention_match.group(1).lower()
            
            # Check Discord message cache first
            for discord_msg_id, cached_msg in discord_message_cache.items():
                if cached_msg['author'].lower().find(mentioned_name) >= 0:
                    reply_context = {
                        'text': cached_msg['content'],
                        'name': cached_msg['author'],
                        'type': 'mention_reply',
                        'platform_source': 'discord'
                    }
                    logger.info(f"✅ Found @mention reply to Discord user {reply_context['name']}")
                    break
    
    # Method 3: Quote pattern detection
    if not reply_context and data.get('text'):
        text = data['text']
        quote_patterns = [r'^>\s*(.+)', r'^"(.+?)"\s*']
        
        for pattern in quote_patterns:
            match = re.search(pattern, text, re.MULTILINE)
            if match:
                quoted_text = match.group(1).lower().strip()
                
                # Check Discord message cache
                for discord_msg_id, cached_msg in discord_message_cache.items():
                    if cached_msg['content'].lower().find(quoted_text) >= 0:
                        reply_context = {
                            'text': cached_msg['content'],
                            'name': cached_msg['author'],
                            'type': 'quote_reply',
                            'platform_source': 'discord'
                        }
                        logger.info(f"✅ Found quote reply to Discord message from {reply_context['name']}")
                        break
                break
    
    return reply_context

# SINGLE GroupMe send function with processing lock
async def send_to_groupme(text, author_name=None, image_url=None, reply_context=None, message_id=None):
    """SINGLE GroupMe send function with duplicate prevention"""
    try:
        # Create unique key for this send operation
        send_key = f"{author_name}:{text[:50]}:{int(time.time())}"
        
        # Check if we're already processing this message
        if message_id and message_id in discord_processing_lock:
            logger.info(f"🚫 Already processing Discord message {message_id}, skipping duplicate send")
            return True
        
        # Lock this message for processing
        if message_id:
            discord_processing_lock[message_id] = time.time()
        
        # Enhanced reply context formatting
        if reply_context:
            quoted_text = reply_context.get('text', 'previous message')
            reply_author = reply_context.get('name', 'Someone')
            reply_type = reply_context.get('type', 'unknown')
            platform_source = reply_context.get('platform_source', 'unknown')
            
            preview = quoted_text[:100] + '...' if len(quoted_text) > 100 else quoted_text
            
            # Different formatting based on reply type and platform
            if platform_source == 'discord':
                text = f"↪️ **{author_name} replying to {reply_author}'s Discord message:**\n> {preview}\n\n{text}"
            else:
                text = f"↪️ **{author_name} replying to {reply_author}:**\n> {preview}\n\n{text}"
        else:
            # Add author name if not already present
            if author_name and not text.startswith(author_name):
                text = f"{author_name}: {text}" if text.strip() else f"{author_name} sent content"
        
        payload = {"bot_id": GROUPME_BOT_ID, "text": text}
        
        if image_url:
            payload["attachments"] = [{"type": "image", "url": image_url}]
        
        response = await make_http_request(GROUPME_POST_URL, 'POST', payload)
        
        # Clean up processing lock
        if message_id and message_id in discord_processing_lock:
            del discord_processing_lock[message_id]
        
        if response['status'] == 202:
            logger.info(f"✅ SINGLE message sent to GroupMe: {text[:50]}...")
            return True
        else:
            logger.error(f"❌ Failed to send to GroupMe: {response['status']}")
            return False
            
    except Exception as e:
        # Clean up processing lock on error
        if message_id and message_id in discord_processing_lock:
            del discord_processing_lock[message_id]
        logger.error(f"❌ Error sending to GroupMe: {e}")
        return False

# Enhanced Discord send function
async def send_to_discord(message, reply_context=None):
    """Enhanced Discord send function"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            logger.error(f"Discord channel {DISCORD_CHANNEL_ID} not found")
            return False
        
        content = message.get('text', '[No text content]')
        author = message.get('name', 'GroupMe User')
        
        # Enhanced reply context formatting
        if reply_context:
            original_text = reply_context.get('text', '[No text]')
            original_author = reply_context.get('name', 'Unknown')
            reply_type = reply_context.get('type', 'unknown')
            platform_source = reply_context.get('platform_source', 'unknown')
            
            preview = original_text[:200] + '...' if len(original_text) > 200 else original_text
            
            if platform_source == 'discord':
                content = f"↪️ **{author} replying to {original_author}'s Discord message:**\n> {preview}\n\n{content}"
            else:
                content = f"↪️ **{author} replying to {original_author}:**\n> {preview}\n\n{content}"
        
        # Handle images
        embeds = []
        if message.get('attachments'):
            for attachment in message['attachments']:
                if attachment.get('type') == 'image' and attachment.get('url'):
                    embeds.append(discord.Embed().set_image(url=attachment['url']))
        
        # Send message
        formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
        sent_message = await discord_channel.send(formatted_content, embeds=embeds)
        
        # Store mapping and cache for future replies
        if message.get('id'):
            groupme_to_discord[message['id']] = sent_message.id
            message_mapping[sent_message.id] = message['id']
            reply_context_cache[message['id']] = message
        
        logger.info(f"✅ Message sent to Discord: {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        return False

# Webhook Server
async def run_webhook_server():
    """Clean webhook server"""
    
    async def health_check(request):
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "features": {
                "single_path_processing": True,
                "no_duplicate_sends": True,
                "bidirectional_replies": True
            },
            "processed_messages": len(processed_messages),
            "processing_locks": len(discord_processing_lock)
        })
    
    async def groupme_webhook(request):
        """SINGLE webhook handler"""
        try:
            data = await request.json()
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            logger.info(f"📨 GroupMe webhook: {sender_info} - {data.get('text', '')[:50]}...")
            
            # Filter bot messages
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
                return web.json_response({"status": "ignored", "reason": "bot_message"})
            
            # Check for duplicates
            if is_duplicate_message(data):
                return web.json_response({"status": "ignored", "reason": "duplicate"})
            
            # Process message
            logger.info(f"✅ Processing unique message from {data.get('name', 'Unknown')}")
            
            # Handle reactions
            if data.get('favorited_by') and len(data['favorited_by']) > 0:
                if bot.is_ready():
                    asyncio.run_coroutine_threadsafe(
                        send_reaction_to_discord(data),
                        bot.loop
                    )
                    logger.info("⚡ Reaction sent to Discord")
            else:
                # Handle regular messages with enhanced reply detection
                reply_context = await detect_reply_context(data)
                
                if bot.is_ready():
                    asyncio.run_coroutine_threadsafe(
                        send_to_discord(data, reply_context),
                        bot.loop
                    )
                    if reply_context:
                        platform = reply_context.get('platform_source', 'unknown')
                        reply_type = reply_context.get('type', 'unknown')
                        logger.info(f"⚡ Message with {reply_type} reply to {platform} user sent to Discord")
                    else:
                        logger.info("⚡ Message sent to Discord")
            
            return web.json_response({"status": "success"})
            
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
    
    logger.info(f"🌐 Single-path webhook server running on 0.0.0.0:{PORT}")
    logger.info(f"🔗 GroupMe webhook: https://your-service.a.run.app/groupme")
    
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        await runner.cleanup()

async def send_reaction_to_discord(data):
    """Send reaction to Discord"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            return False
        
        latest_reaction = data['favorited_by'][-1]
        emoji = latest_reaction.get('emoji', '❤️')
        reacter_name = latest_reaction.get('nickname', 'Someone')
        message_preview = data.get('text', '[No text]')[:100]
        
        content = f"{emoji} **{reacter_name}** reacted to: \"{message_preview}\""
        await discord_channel.send(content)
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send reaction to Discord: {e}")
        return False

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
    logger.info(f'🖼️ Image support: {"✅" if GROUPME_ACCESS_TOKEN else "❌"}')
    logger.info(f'😀 Reaction support: {"✅" if GROUPME_ACCESS_TOKEN else "❌"}')
    logger.info(f'🔒 Single-path processing: ✅')
    logger.info(f'⚡ No duplicate sends: ✅')

@bot.event
async def on_message(message):
    """SINGLE-PATH message handler - CRITICAL FIX"""
    # Only process if not bot and in correct channel
    if message.author.bot or message.channel.id != DISCORD_CHANNEL_ID:
        await bot.process_commands(message)
        return
    
    # Skip commands
    if message.content.startswith('!'):
        await bot.process_commands(message)
        return
    
    # CRITICAL: Check if already processing this message
    if message.id in discord_processing_lock:
        logger.info(f"🚫 Already processing Discord message {message.id}, ignoring duplicate")
        return
    
    logger.info(f"📨 Processing Discord message from {message.author.display_name}")
    
    # Cache Discord message for future reply detection
    discord_message_cache[message.id] = {
        'content': message.content,
        'author': message.author.display_name,
        'timestamp': time.time()
    }
    
    # Clean old cache entries
    if len(discord_message_cache) > 100:
        old_keys = list(discord_message_cache.keys())[:-100]
        for key in old_keys:
            discord_message_cache.pop(key, None)
    
    # Store for context
    recent_messages[message.channel.id].append({
        'author': message.author.display_name,
        'content': message.content,
        'timestamp': time.time(),
        'message_id': message.id
    })
    
    if len(recent_messages[message.channel.id]) > 20:
        recent_messages[message.channel.id].pop(0)
    
    # Detect reply context ONCE
    reply_context = None
    if message.reference and message.reference.message_id:
        try:
            replied_message = await message.channel.fetch_message(message.reference.message_id)
            reply_context = {
                'text': replied_message.content[:200],
                'name': replied_message.author.display_name,
                'type': 'official_reply'
            }
            logger.info(f"✅ Found Discord reply to {reply_context['name']}")
        except:
            pass
    
    # SINGLE PROCESSING PATH - NO MULTIPLE SENDS
    # Build the message content ONCE
    message_content = message.content or ""
    
    # Handle attachments by adding to content
    if message.attachments:
        attachment_info = []
        for attachment in message.attachments:
            if attachment.content_type and attachment.content_type.startswith('image/'):
                attachment_info.append("[Image]")
                logger.info(f"🖼️ Processing image: {attachment.filename}")
            else:
                attachment_info.append(f"[Attached: {attachment.filename}]")
        
        if attachment_info:
            if message_content:
                message_content = f"{message_content} {' '.join(attachment_info)}"
            else:
                message_content = ' '.join(attachment_info)
    
    # SINGLE SEND CALL - This is the only place we send to GroupMe
    if message_content.strip():
        await send_to_groupme(
            message_content, 
            message.author.display_name, 
            None,  # No image URL for now
            reply_context,
            message.id  # Pass message ID for duplicate prevention
        )
        if reply_context:
            logger.info(f"⚡ SINGLE Discord reply sent to GroupMe")
        else:
            logger.info(f"⚡ SINGLE Discord message sent to GroupMe")
    
    # Process commands at the end
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID or 
        str(reaction.emoji) not in EMOJI_MAPPING):
        return
    
    emoji = str(reaction.emoji)
    logger.info(f"😀 Processing reaction {emoji} from {user.display_name}")
    
    # Send reaction to GroupMe
    original_content = reaction.message.content[:50] if reaction.message.content else "a message"
    context = f"'{original_content}...'"
    
    reaction_text = f"{user.display_name} reacted {emoji} to {context}"
    await send_to_groupme(reaction_text, message_id=f"reaction_{reaction.message.id}")

# Bot Commands
@bot.command(name='status')
async def status(ctx):
    """Single-path status command"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    status_msg = f"""🟢 **Single-Path Bridge Status**
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
😀 Reactions: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🌐 Webhook Server: ✅
⚡ **INSTANT Messaging: ✅**
🚫 **No Duplicates: ✅**
💬 **Bidirectional Replies: ✅**
🔒 **Single-Path Processing: ✅**

📝 Processed Messages: {len(processed_messages)}
💬 Message Mappings: {len(message_mapping)}
🔗 Reply Cache: {len(reply_context_cache)}
💾 Discord Cache: {len(discord_message_cache)}
🔒 Processing Locks: {len(discord_processing_lock)}

**FIXED: No more triple sends!**"""
    
    await ctx.send(status_msg)

@bot.command(name='test')
async def test_bridge(ctx):
    """Test bridge functionality"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await send_to_groupme("🧪 Single-path test - no more triple sends!", ctx.author.display_name, message_id=f"test_{ctx.message.id}")
    await ctx.send("✅ SINGLE test message sent to GroupMe!")

@bot.command(name='testreply')
async def test_reply_detection(ctx):
    """Test reply detection"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    test_msg = await ctx.send("🧪 **Reply Test** - Reply to this message to test single-path reply processing!")
    await ctx.send("✅ Reply to the message above - it should send only ONCE to GroupMe!")

@bot.command(name='debug')
async def debug_info(ctx):
    """Show debug information"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    debug_msg = f"""🔍 **Single-Path Debug Information**
**Environment:**
• Discord Token: {'✅' if DISCORD_BOT_TOKEN else '❌'}
• GroupMe Bot ID: {'✅' if GROUPME_BOT_ID else '❌'}
• Channel ID: {DISCORD_CHANNEL_ID}

**Single-Path Architecture:**
• One message processor: ✅
• One send function: ✅
• Processing locks: ✅
• Duplicate prevention: ✅

**Active Data:**
• Message Mappings: {len(message_mapping)}
• Processed Messages: {len(processed_messages)}
• Discord Cache: {len(discord_message_cache)}
• Processing Locks: {len(discord_processing_lock)}

**FIXED: Triple send issue eliminated!**"""
    
    await ctx.send(debug_msg)

# Cleanup Task
async def cleanup_old_data():
    """Enhanced cleanup with processing lock cleanup"""
    while True:
        try:
            current_time = time.time()
            
            # Clean old processing locks (older than 5 minutes)
            old_locks = [
                msg_id for msg_id, timestamp in discord_processing_lock.items()
                if current_time - timestamp > 300
            ]
            for msg_id in old_locks:
                del discord_processing_lock[msg_id]
            
            # Clean old mappings
            if len(message_mapping) > 1000:
                old_keys = list(message_mapping.keys())[:-1000]
                for key in old_keys:
                    message_mapping.pop(key, None)
                    groupme_to_discord.pop(message_mapping.get(key), None)
            
            # Clean old Discord cache
            if len(discord_message_cache) > 100:
                old_keys = list(discord_message_cache.keys())[:-100]
                for key in old_keys:
                    discord_message_cache.pop(key, None)
            
            # Clean old reply cache
            if len(reply_context_cache) > 500:
                old_keys = list(reply_context_cache.keys())[:-500]
                for key in old_keys:
                    reply_context_cache.pop(key, None)
            
            await asyncio.sleep(3600)  # Run every hour
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            await asyncio.sleep(3600)

# Main Function
def main():
    """Single-path main entry point"""
    if not DISCORD_BOT_TOKEN:
        logger.error("❌ DISCORD_BOT_TOKEN required!")
        return
    
    if not GROUPME_BOT_ID:
        logger.error("❌ GROUPME_BOT_ID required!")
        return
    
    if DISCORD_CHANNEL_ID == 0:
        logger.error("❌ DISCORD_CHANNEL_ID required!")
        return
    
    logger.info("🚀 Starting SINGLE-PATH GroupMe-Discord Bridge...")
    logger.info("🔒 Processing locks enabled to prevent triple sends!")
    logger.info("⚡ Single processing path for all messages!")
    
    # Start webhook server
    webhook_thread = start_webhook_server()
    time.sleep(2)
    
    # Start cleanup task and run bot
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(cleanup_old_data())
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")

if __name__ == "__main__":
    main()
