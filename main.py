#!/usr/bin/env python3
"""
Simplified GroupMe-Discord Bridge - Enhanced with Discord Nickname Support
Features: Fast messaging, bidirectional replies, minimal filtering, Discord nicknames only
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("🔥 SIMPLIFIED BIDIRECTIONAL BRIDGE WITH DISCORD NICKNAMES STARTING!")

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

# Simplified Global State
bot_status = {"ready": False, "start_time": time.time()}
message_mapping = {}  # Discord message ID -> GroupMe message ID
reply_context_cache = {}  # Store messages for reply detection
recent_discord_messages = deque(maxlen=20)  # Store Discord messages with nicknames
recent_groupme_messages = deque(maxlen=20)  # Store GroupMe messages

# SIMPLIFIED deduplication - much less aggressive
processed_message_ids = deque(maxlen=100)  # Just track recent message IDs
last_message_time = {}  # Simple rate limiting per user

# Simplified duplicate detection
def is_simple_duplicate(data):
    """Simplified duplicate detection - only blocks obvious duplicates"""
    try:
        # Only check GroupMe message ID for exact duplicates
        msg_id = data.get('id')
        if msg_id and msg_id in processed_message_ids:
            logger.info(f"🚫 Exact duplicate message ID: {msg_id}")
            return True
        
        if msg_id:
            processed_message_ids.append(msg_id)
        
        # Very basic rate limiting - only 0.1 seconds (much more lenient)
        user_id = data.get('user_id', '')
        current_time = time.time()
        
        if user_id in last_message_time:
            time_diff = current_time - last_message_time[user_id]
            if time_diff < 0.1:  # Reduced from 0.5 to 0.1 seconds
                logger.info(f"🚫 Very fast message from {data.get('name', 'Unknown')}")
                return True
        
        last_message_time[user_id] = current_time
        return False
        
    except Exception as e:
        logger.error(f"Error in duplicate detection: {e}")
        return False

def is_bot_message(data):
    """Simplified bot detection"""
    if data.get('sender_type') == 'bot':
        return True
    if data.get('sender_id') == GROUPME_BOT_ID:
        return True
    # Simplified bot name detection
    name = data.get('name', '').lower()
    if name in ['bot', 'groupme', 'system']:
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

async def get_groupme_messages(group_id, limit=20):
    """Get recent GroupMe messages"""
    if not GROUPME_ACCESS_TOKEN:
        return []
    
    url = f"{GROUPME_MESSAGES_URL}?token={GROUPME_ACCESS_TOKEN}&limit={limit}"
    response = await make_http_request(url)
    if response['status'] == 200 and response['data']:
        return response['data'].get('response', {}).get('messages', [])
    return []

# Enhanced reply detection with better nickname matching
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
            if reply_id and reply_id in reply_context_cache:
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
            for discord_msg in reversed(recent_discord_messages):
                # Check against display name (nickname) first, then username
                display_name = discord_msg['author'].lower()
                username = discord_msg.get('username', '').lower()
                
                if (mentioned_name in display_name or 
                    mentioned_name in username or
                    mentioned_name == display_name or
                    mentioned_name == username):
                    
                    reply_context = {
                        'text': discord_msg['content'],
                        'name': discord_msg['author'],  # This is the display_name (nickname)
                        'type': 'mention_reply'
                    }
                    logger.info(f"✅ Found @mention reply to {reply_context['name']} (Discord nickname)")
                    break
    
    # Method 3: Enhanced contextual reply detection
    if not reply_context and data.get('text'):
        text = data['text'].lower()
        
        # Look for common reply patterns mentioning Discord users
        for discord_msg in reversed(list(recent_discord_messages)[-5:]):  # Check last 5 messages
            display_name = discord_msg['author'].lower()
            username = discord_msg.get('username', '').lower()
            
            # Check if the GroupMe message seems to be responding to this Discord user
            if (display_name in text or username in text or
                any(word in text for word in [display_name.split()[0], username.split()[0]] if word)):
                
                reply_context = {
                    'text': discord_msg['content'],
                    'name': discord_msg['author'],  # Display name (nickname)
                    'type': 'contextual_reply'
                }
                logger.info(f"✅ Found contextual reply to {reply_context['name']} (Discord nickname)")
                break
    
    return reply_context

# ENHANCED send_to_groupme function - ALWAYS use Discord nicknames, never usernames
async def send_to_groupme(text, author_name=None, reply_context=None):
    """Enhanced GroupMe send function - ensures ONLY Discord nicknames are shown"""
    try:
        # Enhanced reply context formatting - ensure we ONLY show Discord nicknames
        if reply_context:
            quoted_text = reply_context.get('text', 'previous message')
            reply_author = reply_context.get('name', 'Someone')  # This MUST be display_name only
            preview = quoted_text[:100] + '...' if len(quoted_text) > 100 else quoted_text
            
            # Clean formatting for GroupMe - just show the Discord nickname
            text = f"↪️ **{author_name} replying to {reply_author}:**\n> {preview}\n\n{text}"
            logger.info(f"✅ Formatted GroupMe reply showing Discord nickname: {reply_author}")
        else:
            # Add author name if not already present - use ONLY the display_name (nickname)
            if author_name and not text.startswith(author_name):
                text = f"{author_name}: {text}" if text.strip() else f"{author_name} sent content"
        
        payload = {"bot_id": GROUPME_BOT_ID, "text": text}
        response = await make_http_request(GROUPME_POST_URL, 'POST', payload)
        
        if response['status'] == 202:
            logger.info(f"✅ Message sent to GroupMe using nickname '{author_name}': {text[:50]}...")
            return True
        else:
            logger.error(f"❌ Failed to send to GroupMe: {response['status']}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error sending to GroupMe: {e}")
        return False

# Enhanced send_to_discord function with better reply formatting
async def send_to_discord(message, reply_context=None):
    """Enhanced Discord send function with nickname-aware reply formatting"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            logger.error(f"Discord channel {DISCORD_CHANNEL_ID} not found")
            return False
        
        content = message.get('text', '[No text content]')
        author = message.get('name', 'GroupMe User')
        
        # Enhanced reply context formatting - ONLY show Discord nicknames, never usernames
        if reply_context:
            original_text = reply_context.get('text', '[No text]')
            original_author = reply_context.get('name', 'Unknown')  # This should be display_name only
            preview = original_text[:200] + '...' if len(original_text) > 200 else original_text
            
            # Clean formatting - just show the nickname without extra labels
            reply_type = reply_context.get('type', 'reply')
            content = f"↪️ **{author}** replying to **{original_author}**:\n> {preview}\n\n{content}"
            
            logger.info(f"✅ Formatted reply to Discord user nickname: {original_author}")
        
        # Handle images
        embeds = []
        if message.get('attachments'):
            for attachment in message['attachments']:
                if attachment.get('type') == 'image' and attachment.get('url'):
                    embeds.append(discord.Embed().set_image(url=attachment['url']))
        
        # Send message
        formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
        sent_message = await discord_channel.send(formatted_content, embeds=embeds)
        
        # Enhanced mapping storage with Discord user tracking - store ONLY display_name
        if message.get('id'):
            message_mapping[sent_message.id] = message['id']
            # Store with enhanced metadata for better reply tracking
            reply_context_cache[message['id']] = {
                **message,
                'discord_message_id': sent_message.id,
                'processed_timestamp': time.time()
            }
        
        logger.info(f"✅ Message sent to Discord: {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        return False

# Simplified webhook server
async def run_webhook_server():
    """Simplified webhook server"""
    
    async def health_check(request):
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "features": {
                "simplified_processing": True,
                "minimal_duplicate_prevention": True,
                "bidirectional_replies": True,
                "discord_nicknames_only": True
            },
            "processed_messages": len(processed_message_ids),
            "reply_cache_size": len(reply_context_cache)
        })
    
    async def groupme_webhook(request):
        """Simplified webhook handler"""
        try:
            data = await request.json()
            sender_info = f"{data.get('name', 'Unknown')} ({data.get('sender_type', 'unknown')})"
            logger.info(f"📨 GroupMe webhook: {sender_info} - {data.get('text', '')[:50]}...")
            
            # Simple bot message filter
            if is_bot_message(data):
                logger.info(f"🤖 Ignoring bot message from {data.get('name', 'Unknown')}")
                return web.json_response({"status": "ignored", "reason": "bot_message"})
            
            # Simplified duplicate check
            if is_simple_duplicate(data):
                return web.json_response({"status": "ignored", "reason": "duplicate"})
            
            logger.info(f"✅ Processing message from {data.get('name', 'Unknown')}")
            
            # Store in recent messages for context (simplified)
            if data.get('id'):
                recent_groupme_messages.append({
                    'id': data['id'],
                    'text': data.get('text', ''),
                    'name': data.get('name', ''),
                    'created_at': data.get('created_at', time.time())
                })
                reply_context_cache[data['id']] = data
            
            # Handle reactions (simplified)
            if data.get('favorited_by') and len(data['favorited_by']) > 0:
                if bot.is_ready():
                    asyncio.run_coroutine_threadsafe(
                        send_reaction_to_discord(data),
                        bot.loop
                    )
                    logger.info("⚡ Reaction sent to Discord")
            else:
                # Handle regular messages
                reply_context = await detect_reply_context(data)
                
                if bot.is_ready():
                    asyncio.run_coroutine_threadsafe(
                        send_to_discord(data, reply_context),
                        bot.loop
                    )
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
    
    logger.info(f"🌐 Simplified webhook server running on 0.0.0.0:{PORT}")
    
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
    logger.info(f'🔒 Simplified processing: ✅')
    logger.info(f'⚡ Minimal duplicate prevention: ✅')
    logger.info(f'🔄 Bidirectional replies: ✅')
    logger.info(f'🏷️  Discord nicknames only: ✅')

# ENHANCED Discord message handler - ALWAYS use nicknames for GroupMe
@bot.event
async def on_message(message):
    """ENHANCED message handler - ensures ONLY Discord nicknames appear on GroupMe"""
    # Basic filters only
    if message.author.bot or message.channel.id != DISCORD_CHANNEL_ID:
        await bot.process_commands(message)
        return
    
    # Skip commands
    if message.content.startswith('!'):
        await bot.process_commands(message)
        return
    
    # Get the Discord display name (nickname) - this is what will appear on GroupMe
    discord_nickname = message.author.display_name
    discord_username = message.author.name
    
    logger.info(f"📨 Processing Discord message from '{discord_nickname}' (username: {discord_username})")
    logger.info(f"🏷️  Will appear on GroupMe as: '{discord_nickname}'")
    
    # Enhanced Discord message tracking - store ONLY the display_name for GroupMe use
    discord_msg_data = {
        'content': message.content,
        'author': discord_nickname,  # PRIMARY: Display name (nickname) for GroupMe
        'username': discord_username,        # INTERNAL: Username for matching only
        'author_id': message.author.id,         # For precise matching
        'timestamp': time.time(),
        'message_id': message.id
    }
    recent_discord_messages.append(discord_msg_data)
    
    # Simple reply detection - ensure we use display_name in reply context
    reply_context = None
    if message.reference and message.reference.message_id:
        try:
            replied_message = await message.channel.fetch_message(message.reference.message_id)
            reply_context = {
                'text': replied_message.content[:200],
                'name': replied_message.author.display_name,  # ONLY display_name (nickname)
                'type': 'official_reply'
            }
            logger.info(f"✅ Found Discord reply to nickname: '{reply_context['name']}'")
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
    
    # Send to GroupMe using ONLY the display_name (nickname) - never username
    if message_content.strip():
        await send_to_groupme(message_content, discord_nickname, reply_context)
        logger.info(f"⚡ Message sent to GroupMe showing nickname: '{discord_nickname}'")
        logger.info(f"   (NOT showing username: '{discord_username}')")
    
    await bot.process_commands(message)

# ENHANCED reaction handling - ALWAYS use Discord nicknames on GroupMe
@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions - ensures ONLY Discord nicknames appear on GroupMe"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID):
        return
    
    emoji = str(reaction.emoji)
    discord_nickname = user.display_name
    discord_username = user.name
    
    logger.info(f"😀 Processing reaction {emoji} from '{discord_nickname}' (username: {discord_username})")
    logger.info(f"🏷️  Will appear on GroupMe as: '{discord_nickname}'")
    
    # Send reaction to GroupMe using ONLY display_name (nickname) - never username
    original_content = reaction.message.content[:50] if reaction.message.content else "a message"
    reaction_text = f"{discord_nickname} reacted {emoji} to '{original_content}...'"
    
    await send_to_groupme(reaction_text, discord_nickname)
    logger.info(f"✅ Reaction sent to GroupMe showing nickname: '{discord_nickname}'")
    logger.info(f"   (NOT showing username: '{discord_username}')")

# Enhanced Bot Commands
@bot.command(name='status')
async def status(ctx):
    """Enhanced status command"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    status_msg = f"""🟢 **Enhanced Bridge Status**
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🌐 Webhook Server: ✅
⚡ **Minimal Filtering: ✅**
🔄 **Bidirectional Replies: ✅**
🏷️  **Discord Nicknames Only: ✅**

📝 Recent Message IDs: {len(processed_message_ids)}
💬 Message Mappings: {len(message_mapping)}
🔗 Reply Cache: {len(reply_context_cache)}

**Enhanced - Discord nicknames (not usernames) appear on GroupMe!**"""
    
    await ctx.send(status_msg)

@bot.command(name='test')
async def test_send(ctx, *, message="Test message from Discord"):
    """Test sending a message to GroupMe - uses Discord nickname"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        await ctx.send("❌ This command only works in the bridged channel")
        return
    
    discord_nickname = ctx.author.display_name
    discord_username = ctx.author.name
    
    logger.info(f"🧪 Test command from '{discord_nickname}' (username: {discord_username})")
    logger.info(f"🏷️  Will appear on GroupMe as: '{discord_nickname}'")
    
    success = await send_to_groupme(message, discord_nickname)
    if success:
        await ctx.send(f"✅ Test message sent to GroupMe as '{discord_nickname}'!")
    else:
        await ctx.send("❌ Failed to send test message to GroupMe")

# Simple cleanup task
async def simple_cleanup():
    """Simple cleanup task"""
    while True:
        try:
            # Clean old mappings (keep it simple)
            if len(message_mapping) > 500:
                old_keys = list(message_mapping.keys())[:-500]
                for key in old_keys:
                    message_mapping.pop(key, None)
            
            # Clean old reply cache
            if len(reply_context_cache) > 200:
                old_keys = list(reply_context_cache.keys())[:-200]
                for key in old_keys:
                    reply_context_cache.pop(key, None)
            
            await asyncio.sleep(3600)  # Run every hour
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            await asyncio.sleep(3600)

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
    logger.info("⚡ Minimal duplicate prevention!")
    logger.info("🔄 Simple bidirectional replies!")
    logger.info("🏷️  Discord nicknames only on GroupMe!")
    
    # Start webhook server
    webhook_thread = start_webhook_server()
    time.sleep(2)
    
    # Start cleanup task and run bot
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(simple_cleanup())
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")

if __name__ == "__main__":
    main()
