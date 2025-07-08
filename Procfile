# Add this to the very end of your main.py file, replacing the current main() section

# Create app object for Buildpacks (web server part)
def create_app():
    """Create the web application for Buildpacks"""
    import asyncio
    from aiohttp import web
    
    async def health_check(request):
        return web.json_response({
            "status": "healthy",
            "bot_ready": bot_status["ready"],
            "uptime": time.time() - bot_status["start_time"]
        })
    
    app = web.Application()
    app.router.add_get('/', health_check)
    app.router.add_get('/health', health_check)
    return app

# For Buildpacks compatibility
app = create_app()

# Main function (unchanged)
def main():
    """Main entry point"""
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
    
    logger.info("🚀 Starting Enhanced GroupMe-Discord Bridge...")
    
    # Start webhook server in thread
    webhook_thread = threading.Thread(target=start_webhook_server, daemon=True)
    webhook_thread.start()
    
    # Wait for server to start
    time.sleep(2)
    
    # Start cleanup task
    loop = asyncio.get_event_loop()
    loop.create_task(cleanup_old_data())
    
    # Start Discord bot
    try:
        bot.run(DISCORD_BOT_TOKEN)
    except Exception as e:
        logger.error(f"❌ Bot failed to start: {e}")

if __name__ == "__main__":
    main()
