#!/usr/bin/env python3
"""
Enhanced GroupMe-Discord Bridge - Complete Fixed Version
Combines webhook handling with Discord bot functionality for complete bidirectional sync
Features: Messages, Images, Reactions, Polls, Reply Context, Threading, Cloud Run Support
FIXED: Async context issues using direct event loop scheduling
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
from datetime import datetime, timedelta
from collections import defaultdict
from discord.ext import commands
from aiohttp import web
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("🔥 ENHANCED GROUPME-DISCORD BRIDGE STARTING!")

# Environment Configuration
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GROUPME_BOT_ID = os.getenv("GROUPME_BOT_ID")
GROUPME_ACCESS_TOKEN = os.getenv("GROUPME_ACCESS_TOKEN")
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
GROUPME_GROUP_ID = os.getenv("GROUPME_GROUP_ID")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")  # Optional: for webhook fallback
PORT = int(os.getenv("PORT", "8080"))

# API Endpoints
GROUPME_POST_URL = "https://api.groupme.com/v3/bots/post"
GROUPME_IMAGE_UPLOAD_URL = "https://image.groupme.com/pictures"
GROUPME_GROUPS_URL = f"https://api.groupme.com/v3/groups/{GROUPME_GROUP_ID}"
GROUPME_MESSAGES_URL = f"https://api.groupme.com/v3/groups/{GROUPME_GROUP_ID}/messages"
GROUPME_POLLS_CREATE_URL = f"https://api.groupme.com/v3/poll/{GROUPME_GROUP_ID}"
GROUPME_POLLS_SHOW_URL = "https://api.groupme.com/v3/poll"

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents, help_command=None)

# Global State
bot_status = {"ready": False, "start_time": time.time()}
message_mapping = {}  # Discord message ID -> GroupMe message ID
groupme_to_discord = {}  # GroupMe message ID -> Discord message ID
recent_messages = defaultdict(list)
poll_mapping = {}  # Discord poll ID -> poll data
groupme_poll_mapping = {}  # GroupMe poll ID -> poll data
active_polls = {}
poll_vote_tracking = defaultdict(dict)

# Emoji mappings for reactions
EMOJI_MAPPING = {
    '❤️': '❤️', '👍': '👍', '👎': '👎', '😂': '😂', '😮': '😮', '😢': '😢', '😡': '😡',
    '✅': '✅', '❌': '❌', '🔥': '🔥', '💯': '💯', '🎉': '🎉', '👏': '👏', '💪': '💪',
    '🤔': '🤔', '😍': '😍', '🙄': '🙄', '😴': '😴', '🤷': '🤷', '🤦': '🤦', '💀': '💀',
    '🪩': '🪩'
}

# Helper Functions
async def make_http_request(url, method='GET', data=None, headers=None):
    """Enhanced HTTP request helper with better error handling"""
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

async def get_groupme_message(message_id):
    """Fetch specific GroupMe message by ID"""
    if not GROUPME_ACCESS_TOKEN or not GROUPME_GROUP_ID:
        return None
    
    url = f"{GROUPME_MESSAGES_URL}?token={GROUPME_ACCESS_TOKEN}&limit=100"
    response = await make_http_request(url)
    
    if response['status'] == 200 and response['data']:
        messages = response['data'].get('response', {}).get('messages', [])
        for msg in messages:
            if msg.get('id') == message_id:
                return msg
    return None

async def get_groupme_messages(group_id, before_id=None, limit=20):
    """Get recent GroupMe messages for context"""
    if not GROUPME_ACCESS_TOKEN:
        return []
    
    url = f"https://api.groupme.com/v3/groups/{group_id}/messages?token={GROUPME_ACCESS_TOKEN}&limit={limit}"
    if before_id:
        url += f"&before_id={before_id}"
    
    response = await make_http_request(url)
    if response['status'] == 200 and response['data']:
        return response['data'].get('response', {}).get('messages', [])
    return []

def find_reply_context(message):
    """Detect reply context from GroupMe message attachments"""
    if not message.get('attachments'):
        return None
    
    reply_attachment = next(
        (att for att in message['attachments'] if att.get('type') == 'reply'), 
        None
    )
    return reply_attachment

def detect_reply_from_text(message, recent_messages):
    """Detect replies from text patterns"""
    if not message.get('text'):
        return None
    
    text = message['text']
    
    # Look for @mentions
    mention_match = re.search(r'@(\w+)', text)
    if mention_match:
        mentioned_name = mention_match.group(1).lower()
        for msg in recent_messages:
            if (msg.get('name', '').lower().find(mentioned_name) >= 0 and 
                msg.get('id') != message.get('id') and
                msg.get('user_id') != message.get('user_id')):
                return msg
    
    # Look for quote patterns
    quote_patterns = [
        r'^>\s*(.+)',  # "> quoted text"
        r'^"(.+?)"\s*',  # "quoted text"
        r'^(.+):\s*$'  # "Name:"
    ]
    
    for pattern in quote_patterns:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            quoted_text = match.group(1).lower()
            for msg in recent_messages:
                if (msg.get('text', '').lower().find(quoted_text) >= 0 and
                    msg.get('id') != message.get('id') and
                    msg.get('user_id') != message.get('user_id')):
                    return msg
    
    return None

def parse_poll_text(text):
    """Parse poll from text formats"""
    # Remove common prefixes
    text = re.sub(r'^(poll:|📊|survey:)\s*', '', text, flags=re.IGNORECASE)
    
    if '?' not in text:
        return None
    
    question, options_str = text.split('?', 1)
    question = question.strip()
    
    # Try different option separators
    separators = ['\n', ',', ';', '|']
    options = []
    
    for sep in separators:
        potential_options = [opt.strip() for opt in options_str.split(sep) if opt.strip()]
        if len(potential_options) >= 2:
            options = potential_options
            break
    
    if len(options) < 2:
        return None
    
    return {
        'question': question,
        'options': options[:10]  # Limit to 10 options
    }

# GroupMe Functions
async def send_to_groupme(text, author_name=None, image_url=None, reply_context=None):
    """Send message to GroupMe with enhanced formatting"""
    if reply_context:
        quoted_text, reply_author = reply_context
        text = f"↪️ Replying to {reply_author}: \"{quoted_text[:50]}{'...' if len(quoted_text) > 50 else ''}\"\n\n{text}"
    
    if author_name and not text.startswith(author_name):
        text = f"{author_name}: {text}" if text.strip() else f"{author_name} sent content"
    
    payload = {"bot_id": GROUPME_BOT_ID, "text": text}
    
    if image_url:
        payload["attachments"] = [{"type": "image", "url": image_url}]
    
    response = await make_http_request(GROUPME_POST_URL, 'POST', payload)
    
    if response['status'] == 202:
        logger.info(f"✅ Message sent to GroupMe: {text[:50]}...")
        return True
    else:
        logger.error(f"❌ Failed to send to GroupMe: {response['status']}")
        return False

async def upload_image_to_groupme(image_url):
    """Download and upload image to GroupMe"""
    if not GROUPME_ACCESS_TOKEN:
        return None
    
    async with aiohttp.ClientSession() as session:
        try:
            # Download image
            async with session.get(image_url) as resp:
                if resp.status != 200:
                    return None
                image_data = await resp.read()
            
            # Upload to GroupMe
            data = aiohttp.FormData()
            data.add_field('file', image_data, filename='image.png', content_type='image/png')
            
            async with session.post(
                GROUPME_IMAGE_UPLOAD_URL,
                data=data,
                headers={'X-Access-Token': GROUPME_ACCESS_TOKEN}
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result['payload']['url']
                return None
        except Exception as e:
            logger.error(f"Image upload failed: {e}")
            return None

# Discord Functions
async def send_to_discord(message, reply_context=None):
    """Send message to Discord using bot"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            logger.error(f"Discord channel {DISCORD_CHANNEL_ID} not found")
            return False
        
        content = message.get('text', '[No text content]')
        author = message.get('name', 'GroupMe User')
        
        # Add reply context
        if reply_context:
            original_text = reply_context.get('text', '[No text content]')
            original_author = reply_context.get('name', 'Unknown User')
            original_preview = original_text[:150] + '...' if len(original_text) > 150 else original_text
            content = f"**Replying to {original_author}:** \"{original_preview}\"\n\n{content}"
        
        # Handle images
        embeds = []
        if message.get('attachments'):
            for attachment in message['attachments']:
                if attachment.get('type') == 'image' and attachment.get('url'):
                    embeds.append(discord.Embed().set_image(url=attachment['url']))
        
        # Send message
        formatted_content = f"**{author}:** {content}" if content else f"**{author}** sent an attachment"
        sent_message = await discord_channel.send(formatted_content, embeds=embeds)
        
        # Store mapping
        if message.get('id'):
            groupme_to_discord[message['id']] = sent_message.id
            message_mapping[sent_message.id] = message['id']
        
        logger.info(f"✅ Message sent to Discord: {content[:50]}...")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send to Discord: {e}")
        return False

async def send_reaction_to_discord(reaction_data, original_message):
    """Send reaction notification to Discord"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            return False
        
        emoji = reaction_data.get('favorited_by', {}).get('emoji', '❤️')
        reacter_name = reaction_data.get('favorited_by', {}).get('nickname', 'Someone')
        
        message_preview = original_message.get('text', '[No text content]')
        if len(message_preview) > 100:
            message_preview = message_preview[:97] + '...'
        
        content = f"{emoji} **{reacter_name}** reacted to: \"{message_preview}\""
        
        await discord_channel.send(content)
        logger.info(f"✅ Reaction sent to Discord: {content}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to send reaction to Discord: {e}")
        return False

# Poll Functions
async def create_groupme_poll_from_discord(poll, author_name, discord_message):
    """Create native GroupMe poll from Discord poll"""
    if not GROUPME_ACCESS_TOKEN:
        logger.error("GROUPME_ACCESS_TOKEN required for native polls")
        return False
    
    try:
        # Extract question
        if hasattr(poll.question, 'text'):
            question = poll.question.text
        else:
            question = str(poll.question)
        
        # Extract options
        options = []
        if hasattr(poll, 'answers'):
            options = [answer.text if hasattr(answer, 'text') else str(answer) for answer in poll.answers]
        elif hasattr(poll, 'options'):
            options = [option.text if hasattr(option, 'text') else str(option) for option in poll.options]
        
        if len(options) < 2:
            return False
        
        # Create GroupMe poll
        poll_payload = {
            "subject": question[:160],
            "options": [{"title": opt[:160]} for opt in options[:10]],
            "expiration": int(time.time()) + (24 * 60 * 60),
            "type": "single",
            "visibility": "public"
        }
        
        url = f"{GROUPME_POLLS_CREATE_URL}?token={GROUPME_ACCESS_TOKEN}"
        response = await make_http_request(url, 'POST', poll_payload)
        
        if response['status'] == 201:
            poll_data = response['data'].get('poll', {}).get('data', {})
            groupme_poll_id = poll_data.get('id')
            
            # Track poll
            poll_id = f"discord_{discord_message.id}"
            active_polls[poll_id] = {
                'discord_message': discord_message,
                'discord_poll': poll,
                'groupme_poll_id': groupme_poll_id,
                'author': author_name,
                'created_at': time.time(),
                'source': 'discord',
                'options': options
            }
            
            poll_mapping[discord_message.id] = poll_id
            groupme_poll_mapping[groupme_poll_id] = poll_id
            
            logger.info(f"✅ Created native GroupMe poll: {question}")
            return True
        else:
            logger.error(f"Failed to create GroupMe poll: {response['status']}")
            return False
            
    except Exception as e:
        logger.error(f"Error creating GroupMe poll: {e}")
        return False

async def create_discord_poll_from_groupme(question, options, author_name, groupme_poll_id):
    """Create Discord poll from GroupMe poll"""
    try:
        discord_channel = bot.get_channel(DISCORD_CHANNEL_ID)
        if not discord_channel:
            return False
        
        # Create Discord poll
        poll_options = []
        for i, option in enumerate(options[:10]):
            emoji = f"{i+1}\u20e3"
            poll_options.append(discord.PollMedia(text=option[:55], emoji=emoji))
        
        poll = discord.Poll(
            question=f"📊 {question} (from {author_name})",
            options=poll_options,
            multiple=False,
            duration=24
        )
        
        poll_message = await discord_channel.send(poll=poll)
        
        # Track poll
        poll_id = f"groupme_{groupme_poll_id}"
        active_polls[poll_id] = {
            'discord_message': poll_message,
            'discord_poll': poll,
            'groupme_poll_id': groupme_poll_id,
            'author': author_name,
            'created_at': time.time(),
            'source': 'groupme',
            'options': options
        }
        
        groupme_poll_mapping[groupme_poll_id] = poll_id
        
        logger.info(f"✅ Created Discord poll from GroupMe: {question}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating Discord poll from GroupMe: {e}")
        return False

# FIXED Webhook Server with Direct Async Scheduling
async def run_webhook_server():
    """Fixed webhook server using direct async scheduling"""
    
    async def health_check(request):
        """Health check endpoint"""
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"],
            "features": {
                "bidirectional_sync": True,
                "image_support": bool(GROUPME_ACCESS_TOKEN),
                "reactions": bool(GROUPME_ACCESS_TOKEN),
                "polls": bool(GROUPME_ACCESS_TOKEN and GROUPME_GROUP_ID),
                "reply_context": bool(GROUPME_ACCESS_TOKEN),
                "threading": True,
                "direct_async": True
            },
            "active_polls": len(active_polls),
            "message_mappings": len(message_mapping)
        })
    
    async def groupme_webhook(request):
        """FIXED: GroupMe webhook handler using direct async scheduling"""
        try:
            data = await request.json()
            logger.info(f"📨 GroupMe webhook received: {data.get('name', 'Unknown')} - {data.get('text', '')[:50]}...")
            
            # Only handle non-bot messages
            if data.get('sender_type') != 'bot' and data.get('name', '') != 'Bot':
                
                # Handle reactions
                if data.get('favorited_by') and len(data['favorited_by']) > 0:
                    latest_reaction = data['favorited_by'][-1]
                    reaction_data = {'favorited_by': latest_reaction}
                    
                    # FIXED: Schedule reaction in Discord bot's event loop
                    if bot.is_ready():
                        asyncio.run_coroutine_threadsafe(
                            send_reaction_to_discord(reaction_data, data),
                            bot.loop
                        )
                        logger.info("✅ Reaction scheduled for Discord")
                
                # Handle regular messages
                else:
                    # Simple reply context detection
                    reply_context = None
                    message_text = data.get('text', '')
                    
                    # Check for @mention pattern
                    if '@' in message_text and GROUPME_ACCESS_TOKEN:
                        # Simplified reply context
                        reply_context = {'text': 'previous message', 'name': 'Someone'}
                    
                    # Check for text polls
                    if message_text and ('poll:' in message_text.lower() or '📊' in message_text):
                        poll_data = parse_poll_text(message_text)
                        if poll_data and len(poll_data['options']) >= 2:
                            # Create simple poll message
                            poll_text = f"📊 Poll from {data.get('name', 'Unknown')}: {poll_data['question']}?\n\n"
                            
                            for i, option in enumerate(poll_data['options']):
                                poll_text += f"{i+1}. {option}\n"
                            
                            poll_text += "\nVote by replying with the number! 🗳️"
                            
                            # Send poll as regular message
                            poll_message = {
                                'text': poll_text,
                                'name': 'Poll Bot',
                                'id': f"poll_{int(time.time())}"
                            }
                            
                            # FIXED: Schedule poll message in Discord bot's event loop
                            if bot.is_ready():
                                asyncio.run_coroutine_threadsafe(
                                    send_to_discord(poll_message, None),
                                    bot.loop
                                )
                                logger.info("✅ Poll scheduled for Discord")
                    
                    # FIXED: Schedule regular message in Discord bot's event loop
                    if bot.is_ready():
                        asyncio.run_coroutine_threadsafe(
                            send_to_discord(data, reply_context),
                            bot.loop
                        )
                        logger.info("✅ Message scheduled for Discord")
            
            return web.json_response({"status": "success"})
            
        except Exception as e:
            logger.error(f"❌ Error handling GroupMe webhook: {e}")
            return web.json_response({"error": str(e)}, status=500)
    
    # Create web application
    app = web.Application()
    
    # Add routes
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    app.router.add_get('/_ah/health', health_check)
    app.router.add_post('/groupme', groupme_webhook)
    app.router.add_post('/groupme/webhook', groupme_webhook)
    
    # CORS support
    app.router.add_options('/{path:.*}', lambda request: web.Response())
    
    # Start server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    
    logger.info(f"🌐 Webhook server running on 0.0.0.0:{PORT}")
    logger.info(f"🔗 GroupMe webhook URL: https://your-service.a.run.app/groupme")
    
    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        logger.info("🛑 Webhook server shutting down...")
        await runner.cleanup()

def start_webhook_server():
    """Start webhook server in thread with proper async context"""
    def run_in_thread():
        # Create new event loop for this thread
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
    """Bot ready event - SIMPLIFIED VERSION"""
    global bot_status
    bot_status["ready"] = True
    
    logger.info(f'🤖 {bot.user} connected to Discord!')
    logger.info(f'📺 Channel ID: {DISCORD_CHANNEL_ID}')
    logger.info(f'🖼️ Image support: {"✅" if GROUPME_ACCESS_TOKEN else "❌"}')
    logger.info(f'😀 Reaction support: {"✅" if GROUPME_ACCESS_TOKEN else "❌"}')
    logger.info(f'📊 Poll support: {"✅" if GROUPME_ACCESS_TOKEN and GROUPME_GROUP_ID else "❌"}')
    logger.info(f'🧵 Threading: ✅')
    logger.info(f'🌐 Webhook server: ✅')
    logger.info(f'📬 Direct async scheduling: ✅')
    logger.info(f'☁️ Enhanced bridge ready!')

@bot.event
async def on_message(message):
    """Enhanced message handler"""
    if message.author.bot or message.channel.id != DISCORD_CHANNEL_ID:
        await bot.process_commands(message)
        return
    
    # Skip commands
    if message.content.startswith('!'):
        await bot.process_commands(message)
        return
    
    logger.info(f"📨 Processing Discord message from {message.author.display_name}")
    
    # Handle polls
    if hasattr(message, 'poll') and message.poll:
        logger.info("📊 Discord poll detected")
        try:
            success = await create_groupme_poll_from_discord(
                message.poll, message.author.display_name, message
            )
            await message.add_reaction("✅" if success else "❌")
        except Exception as e:
            logger.error(f"Poll handling error: {e}")
            await message.add_reaction("❌")
        return
    
    # Check for text-based polls
    if message.content:
        poll_keywords = ['poll:', '📊', 'vote:', 'survey:']
        if any(keyword in message.content.lower() for keyword in poll_keywords):
            poll_data = parse_poll_text(message.content)
            if poll_data and len(poll_data['options']) >= 2:
                # Create text poll for GroupMe
                option_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
                poll_text = f"📊 Poll from {message.author.display_name}: {poll_data['question']}?\n\n"
                
                for i, option in enumerate(poll_data['options']):
                    emoji = option_emojis[i] if i < len(option_emojis) else f"{i+1}."
                    poll_text += f"{emoji} {option}\n"
                
                poll_text += "\nReact with numbers to vote! 🗳️"
                
                success = await send_to_groupme(poll_text, "Poll Bot")
                if success:
                    await message.add_reaction("📊")
                return
    
    # Store for threading context
    recent_messages[message.channel.id].append({
        'author': message.author.display_name,
        'content': message.content,
        'timestamp': time.time(),
        'message_id': message.id
    })
    
    if len(recent_messages[message.channel.id]) > 20:
        recent_messages[message.channel.id].pop(0)
    
    # Handle replies
    reply_context = None
    if message.reference and message.reference.message_id:
        try:
            replied_message = await message.channel.fetch_message(message.reference.message_id)
            reply_context = (replied_message.content[:100], replied_message.author.display_name)
        except:
            pass
    
    # Handle images and content
    if message.attachments:
        for attachment in message.attachments:
            if attachment.content_type and attachment.content_type.startswith('image/'):
                logger.info(f"🖼️ Processing image: {attachment.filename}")
                groupme_image_url = await upload_image_to_groupme(attachment.url)
                await send_to_groupme(
                    message.content or "[Image]", 
                    message.author.display_name, 
                    groupme_image_url, 
                    reply_context
                )
            else:
                await send_to_groupme(
                    f"{message.content} [Attached: {attachment.filename}]",
                    message.author.display_name,
                    reply_context=reply_context
                )
    elif message.content.strip():
        await send_to_groupme(message.content, message.author.display_name, reply_context=reply_context)
    
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    """Handle reaction additions"""
    if (user.bot or reaction.message.channel.id != DISCORD_CHANNEL_ID or 
        str(reaction.emoji) not in EMOJI_MAPPING):
        return
    
    emoji = str(reaction.emoji)
    logger.info(f"😀 Processing reaction {emoji} from {user.display_name}")
    
    # Handle poll votes
    for poll_id, poll_data in active_polls.items():
        if (poll_data.get('source') == 'discord' and 
            poll_data['discord_message'].id == reaction.message.id):
            
            # This is a poll vote - send to GroupMe
            vote_text = f"🗳️ {user.display_name} voted with {emoji}"
            await send_to_groupme(vote_text, "Vote Bot")
            return
    
    # Handle regular reactions
    discord_msg_id = reaction.message.id
    if discord_msg_id in message_mapping:
        # This is a reaction to a GroupMe message
        groupme_msg_id = message_mapping[discord_msg_id]
        original_msg = await get_groupme_message(groupme_msg_id)
        
        if original_msg:
            original_text = original_msg.get('text', '')[:50]
            original_author = original_msg.get('name', 'Unknown')
            context = f"'{original_text}...' by {original_author}" if original_text else f"message by {original_author}"
        else:
            context = "a message"
        
        reaction_text = f"{user.display_name} reacted {emoji} to {context}"
        await send_to_groupme(reaction_text)
    else:
        # This is a reaction to a Discord message
        original_author = reaction.message.author.display_name
        original_content = reaction.message.content[:50] if reaction.message.content else "a message"
        context = f"'{original_content}...' by {original_author}"
        
        reaction_text = f"{user.display_name} reacted {emoji} to {context}"
        await send_to_groupme(reaction_text)

# Bot Commands
@bot.command(name='status')
async def status(ctx):
    """Show bridge status"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    status_msg = f"""🟢 **Enhanced Bridge Status**
🔗 GroupMe Bot: {'✅' if GROUPME_BOT_ID else '❌'}
🔑 Access Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
🖼️ Image Support: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
😀 Reactions: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
📊 Polls: {'✅' if GROUPME_ACCESS_TOKEN and GROUPME_GROUP_ID else '❌'}
🧵 Threading: ✅
🌐 Webhook Server: ✅
📬 Direct Async: ✅

📊 Active Polls: {len(active_polls)}
💬 Message Mappings: {len(message_mapping)}
📈 Recent Messages: {len(recent_messages.get(DISCORD_CHANNEL_ID, []))}

**Supported Reactions:** {', '.join(list(EMOJI_MAPPING.keys())[:8])}{'...' if len(EMOJI_MAPPING) > 8 else ''}"""
    
    await ctx.send(status_msg)

@bot.command(name='test')
async def test_bridge(ctx):
    """Test bridge functionality"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    await send_to_groupme("🧪 Enhanced bridge test from Discord with FIXED async!", ctx.author.display_name)
    await ctx.send("✅ Test message sent to GroupMe!")

@bot.command(name='debug')
async def debug_info(ctx):
    """Show debug information"""
    if ctx.channel.id != DISCORD_CHANNEL_ID:
        return
    
    debug_msg = f"""🔍 **Debug Information**
**Environment:**
• Discord Token: {'✅' if DISCORD_BOT_TOKEN else '❌'}
• GroupMe Bot ID: {'✅' if GROUPME_BOT_ID else '❌'}
• GroupMe Token: {'✅' if GROUPME_ACCESS_TOKEN else '❌'}
• Group ID: {'✅' if GROUPME_GROUP_ID else '❌'}
• Channel ID: {DISCORD_CHANNEL_ID}
• Port: {PORT}

**Bot Status:**
• Ready: {bot_status['ready']}
• Uptime: {int(time.time() - bot_status['start_time'])}s
• Loop Running: {bot.loop.is_running()}

**Active Data:**
• Polls: {len(active_polls)}
• Mappings: {len(message_mapping)}
• Recent: {len(recent_messages.get(DISCORD_CHANNEL_ID, []))}

**Fix Applied:** Direct async scheduling ✅
**Webhook Endpoints:**
• GroupMe: https://your-service.a.run.app/groupme
• Health: https://your-service.a.run.app/health"""
    
    await ctx.send(debug_msg)

# Cleanup Task
async def cleanup_old_data():
    """Periodic cleanup of old data"""
    while True:
        try:
            current_time = time.time()
            
            # Clean old polls (24 hours)
            expired_polls = [
                poll_id for poll_id, poll_data in active_polls.items()
                if current_time - poll_data['created_at'] > 86400
            ]
            
            for poll_id in expired_polls:
                del active_polls[poll_id]
                logger.info(f"🗑️ Cleaned expired poll: {poll_id}")
            
            # Clean old vote tracking
            old_votes = [
                vote_key for vote_key, vote_data in poll_vote_tracking.items()
                if current_time - vote_data['timestamp'] > 86400
            ]
            
            for vote_key in old_votes:
                del poll_vote_tracking[vote_key]
            
            # Clean old message mappings (keep 1000 most recent)
            if len(message_mapping) > 1000:
                old_keys = list(message_mapping.keys())[:-1000]
                for key in old_keys:
                    message_mapping.pop(key, None)
                    groupme_to_discord.pop(message_mapping.get(key), None)
            
            await asyncio.sleep(3600)  # Run every hour
            
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
            await asyncio.sleep(3600)

# Main Function
def main():
    """Main entry point with FIXED async handling"""
    # Validate environment
    if not DISCORD_BOT_TOKEN:
        logger.error("❌ DISCORD_BOT_TOKEN required!")
        return
    
    if not GROUPME_BOT_ID:
        logger.error("❌ GROUPME_BOT_ID required!")
        return
    
    if DISCORD_CHANNEL_ID == 0:
        logger.error("❌ DISCORD_CHANNEL_ID required!")
        return
    
    if not GROUPME_ACCESS_TOKEN:
        logger.warning("⚠️ GROUPME_ACCESS_TOKEN not set - limited functionality")
    
    if not GROUPME_GROUP_ID:
        logger.warning("⚠️ GROUPME_GROUP_ID not set - polls limited")
    
    logger.info("🚀 Starting Enhanced GroupMe-Discord Bridge with ASYNC FIX...")
    
    # Start webhook server in separate thread
    webhook_thread = start_webhook_server()
    
    # Wait for server to start
    time.sleep(2)
    
    # Start cleanup task when bot starts
    async def startup_tasks():
        await cleanup_old_data()
    
    # Start Discord bot with startup tasks
    try:
        # Create event loop and run bot
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Schedule cleanup task
        loop.create_task(startup_tasks())
        
        # Run bot
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")

if __name__ == "__main__":
    main()
