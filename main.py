import asyncio
import logging
import os
import random
import re
import requests
import time
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from pymongo import MongoClient
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from aiohttp import web

# ============= LOGGING SETUP =============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= CONFIGURATION =============
API_ID = 6435225
API_HASH = "4e984ea35f854762dcde906dce426c2d"

TELETHON_STRING = "1BVtsOHYBu3DbdfZS4E-mZ0VB7e_XMAimR_Ly-ZvFwJ4YhMyH-_Rg41uwOzsmJ6_U74HvcYMRBWZZundC8QDi7oybJNXv4tO_QFU8Kh1YSzw7K2Ye4RDpHuEpgrtBJwLudmvYX7isF09Qralk1t_8zcEFu8Bt-oSHMo7cqrsQGP-QSkJuG17Z3YtOtkJI8fcdlBwF8AfsMspfBkU_WL8wtOLuPlLPNN02NbViaghhOlFrh4DQnTlUby6BkvZd5PnKLIjl0tpqAOYKUPABD9ScAteHZ679p7S8r_ROyGCI-iw5nlszm6A5DwxzPgpyrPT48up5Qn-T6Y-bbTyByPxUjnxON4wF5kY="

BOT_TOKEN = "8601781316:AAGDCDP2R57XyeCMi7xuwM4FTTMfKys1fls"

# Bot usernames
VIRAL_HEAVEN_BOT = ""
DESI_BOT = "@nightfallhubbot"
JAPANESE_BOT = ""

# Source channels
VIRAL_SOURCE = ""
JAPANESE_SOURCE = ""

# Target channels
VIRAL_TARGET = ""
DESI_TARGET = -1003717217563
JAPANESE_TARGET = ""

# DB Channel for storing scraped videos
DB_CHANNEL = -1003784476163

# Image channel for thumbnails
IMAGE_CHANNEL = -1003911151489

# Links
TUTORIAL_LINK = "https://t.me/linkopenchannel1/2"
DISABLE_ADS_LINK = "https://t.me/Crystalar"

# Video counts per batch
VIRAL_VIDEO_COUNT = (10, 15)
DESI_VIDEO_COUNT = (10, 15)
JAPANESE_VIDEO_COUNT = (10, 15)

# MongoDB
MONGO_URI = "mongodb://bronaing371:musicbotmyanmar@ac-qy84yob-shard-00-00.nc4wqzb.mongodb.net:27017,ac-qy84yob-shard-00-01.nc4wqzb.mongodb.net:27017,ac-qy84yob-shard-00-02.nc4wqzb.mongodb.net:27017/?authSource=admin&replicaSet=atlas-uk3oy7-shard-0&tls=true"

# Scraper configuration
SCRAPER_CONFIG = {
    'base_url': 'https://lol49.org',
    'headers': {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'referer': 'https://www.google.com/',
        'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36',
    },
    'video_headers': {
        'Referer': 'https://lol49.org/',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36',
    }
}

# Global lock
execution_lock = asyncio.Lock()
is_running = False

# MongoDB connection
mongo_client = MongoClient(MONGO_URI)
db = mongo_client['telegram_automation']

# Collections
used_videos_collection = db['used_videos_scraped']
channel_cache_collection = db['channel_cache']
posted_history_collection = db['posted_history']
batch_counters_collection = db['batch_counters']
images_collection = db['used_images_viral']

# Telegram clients
session_client = TelegramClient(StringSession(TELETHON_STRING), API_ID, API_HASH)
bot_client = TelegramClient('bot', API_ID, API_HASH)


# ============= HELPER FUNCTIONS =============
async def rate_limit(delay: float = 1.0):
    await asyncio.sleep(delay)


async def download_video(url: str, filename: str) -> Optional[str]:
    try:
        response = requests.get(url, headers=SCRAPER_CONFIG['video_headers'], stream=True, timeout=60)
        if response.status_code == 200:
            filepath = f"/tmp/{filename}"
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return filepath
    except Exception as e:
        logger.error(f"Download failed: {e}")
    return None


async def get_next_batch_number(bot_name: str) -> int:
    counter = batch_counters_collection.find_one_and_update(
        {'bot_name': bot_name},
        {'$inc': {'batch_no': 1}},
        upsert=True,
        return_document=True
    )
    return counter.get('batch_no', 1)


async def get_random_unused_image() -> Optional[Dict]:
    try:
        all_images = []
        async for message in session_client.iter_messages(IMAGE_CHANNEL, limit=500):
            if message.photo:
                all_images.append(message)

        if not all_images:
            logger.error("No images found in channel")
            return None

        used_images = images_collection.find({})
        used_ids = set([img['message_id'] for img in used_images])

        unused_images = [img for img in all_images if img.id not in used_ids]

        if not unused_images:
            logger.info("All images used, resetting image pool")
            images_collection.delete_many({})
            unused_images = all_images

        selected_image = random.choice(unused_images)

        images_collection.insert_one({
            'message_id': selected_image.id,
            'timestamp': datetime.now()
        })

        return selected_image

    except Exception as e:
        logger.error(f"Error getting random image: {e}")
        return None


# ============= BATCH LINK GENERATION - FIXED =============
async def get_batch_link_from_bot(batch_bot_username: str, source_channel: int = None, 
                                  message_ids: List[int] = None, video_paths: List[str] = None,
                                  db_channel: int = None, db_message_id: int = None) -> Optional[str]:
    """
    Universal batch link generator - ONLY clicks buttons from current response message
    """
    try:
        logger.info(f"Getting batch link from {batch_bot_username}")
        
        async with session_client.conversation(batch_bot_username, timeout=120) as conv:
            # Send /custom_batch
            await conv.send_message('/custom_batch')
            logger.info("Sent /custom_batch")
            await asyncio.sleep(3)
            
            # Handle initial response (like "Send me videos" or button)
            try:
                response = await conv.get_response(timeout=10)
                logger.info(f"Initial response: {response.text[:100] if response.text else 'No text'}")
                
                # If there's a button to start, click it
                if response.buttons:
                    for row in response.buttons:
                        for button in row:
                            button_text = button.text
                            logger.info(f"Initial button: {button_text}")
                            if any(kw in button_text.lower() for kw in ['new', 'batch', 'create', 'start']):
                                logger.info(f"Clicking initial button: {button_text}")
                                await button.click()
                                await asyncio.sleep(3)
                                response = await conv.get_response(timeout=10)
                                logger.info(f"After initial click: {response.text[:100] if response.text else 'No text'}")
            except asyncio.TimeoutError:
                logger.info("No initial response")
            
            # Send video(s)
            if db_channel and db_message_id:
                logger.info(f"Forwarding from DB channel")
                await session_client.forward_messages(batch_bot_username, db_message_id, db_channel)
            elif video_paths:
                for path in video_paths:
                    await session_client.send_file(batch_bot_username, path, force_document=False)
                    await asyncio.sleep(2)
            elif source_channel and message_ids:
                for msg_id in message_ids:
                    await session_client.forward_messages(batch_bot_username, msg_id, source_channel)
                    await asyncio.sleep(2)
            else:
                logger.error("No video source")
                return None
            
            await asyncio.sleep(5)
            
            # Get the response after sending videos (THIS is the message with the generate button)
            try:
                response = await conv.get_response(timeout=30)
                logger.info(f"Response after videos: {response.text[:200] if response.text else 'No text'}")
                
                # ONLY click buttons from THIS response message
                if response.buttons:
                    logger.info(f"Found {len(response.buttons)} button rows in current message")
                    for row in response.buttons:
                        for button in row:
                            button_text = button.text
                            logger.info(f"Button in current message: '{button_text}'")
                            
                            # Look for generate button
                            button_lower = button_text.lower()
                            if ('generate' in button_lower or 
                                'link' in button_lower or
                                'ɢᴇɴᴇʀᴀᴛᴇ' in button_text or
                                'ʟɪɴᴋ' in button_text):
                                logger.info(f"Clicking generate button: '{button_text}'")
                                await button.click()
                                await asyncio.sleep(5)
                                
                                # Get the link response
                                link_response = await conv.get_response(timeout=30)
                                logger.info(f"Link response: {link_response.text[:300] if link_response.text else 'No text'}")
                                
                                # Extract link
                                if link_response.text:
                                    match = re.search(r'https://t\.me/[^\s]+start=[^\s]+', link_response.text)
                                    if match:
                                        batch_link = match.group()
                                        logger.info(f"Found batch link: {batch_link}")
                                        return batch_link
                                
                                # Check if link is in a button
                                if link_response.buttons:
                                    for btn_row in link_response.buttons:
                                        for btn in btn_row:
                                            if hasattr(btn, 'url') and btn.url and 'start=' in btn.url:
                                                logger.info(f"Link in button: {btn.url}")
                                                return btn.url
                else:
                    # No buttons - maybe link is directly in text
                    if response.text and 'start=' in response.text:
                        match = re.search(r'https://t\.me/[^\s]+start=[^\s]+', response.text)
                        if match:
                            batch_link = match.group()
                            logger.info(f"Found link directly: {batch_link}")
                            return batch_link
                            
            except asyncio.TimeoutError:
                logger.error("Timeout waiting for response after videos")
            
            # Fallback: scan recent messages (but only as last resort)
            logger.info("Scanning recent messages as fallback...")
            async for msg in session_client.iter_messages(batch_bot_username, limit=10):
                if msg.text and 'start=' in msg.text:
                    match = re.search(r'https://t\.me/[^\s]+start=[^\s]+', msg.text)
                    if match:
                        batch_link = match.group()
                        logger.info(f"Found in recent message: {batch_link}")
                        return batch_link
                if msg.buttons:
                    for row in msg.buttons:
                        for button in row:
                            if hasattr(button, 'url') and button.url and 'start=' in button.url:
                                logger.info(f"Found in recent button: {button.url}")
                                return button.url
            
            logger.error("No batch link found")
            return None
            
    except Exception as e:
        logger.error(f"Error: {e}")
        traceback.print_exc()
        return None


# ============= CACHE MANAGEMENT =============
async def cache_channel_messages(channel_id: int, collection_name: str):
    try:
        logger.info(f"Checking cache for {channel_id}")
        collection = db[collection_name]
        count = collection.count_documents({'channel_id': channel_id})
        
        if count > 500:
            logger.info(f"Already have {count} cached messages")
            return
        
        messages = []
        async for message in session_client.iter_messages(channel_id, limit=500):
            if message.video or message.document:
                messages.append({
                    'message_id': message.id,
                    'channel_id': channel_id,
                    'timestamp': datetime.now()
                })
                await rate_limit(0.5)
        
        if messages:
            collection.delete_many({'channel_id': channel_id})
            collection.insert_many(messages)
        logger.info(f"Cached {len(messages)} messages")
        
        asyncio.create_task(fetch_more_messages_background(channel_id, collection_name))
    except Exception as e:
        logger.error(f"Cache error: {e}")


async def fetch_more_messages_background(channel_id: int, collection_name: str):
    await asyncio.sleep(30)
    try:
        collection = db[collection_name]
        existing = set([doc['message_id'] for doc in collection.find({'channel_id': channel_id})])
        
        more_messages = []
        async for message in session_client.iter_messages(channel_id, limit=2000):
            if (message.video or message.document) and message.id not in existing:
                more_messages.append({
                    'message_id': message.id,
                    'channel_id': channel_id,
                    'timestamp': datetime.now()
                })
                await rate_limit(0.5)
        
        if more_messages:
            collection.insert_many(more_messages)
            logger.info(f"Added {len(more_messages)} more messages")
    except Exception as e:
        logger.error(f"Background cache error: {e}")


async def get_unused_videos_from_channel(channel_id: int, bot_name: str, collection, count: int) -> List[Dict]:
    try:
        used = set()
        used_docs = posted_history_collection.find({'source_channel': channel_id, 'bot_name': bot_name})
        for doc in used_docs:
            used.add(doc.get('message_id'))
        
        cached = list(collection.find({'channel_id': channel_id}))
        unused = [msg for msg in cached if msg['message_id'] not in used]
        
        if len(unused) < count:
            await cache_channel_messages(channel_id, f'cache_{bot_name}')
            cached = list(collection.find({'channel_id': channel_id}))
            unused = [msg for msg in cached if msg['message_id'] not in used]
        
        if len(unused) >= count:
            selected = random.sample(unused, count)
            return [{'message_id': s['message_id'], 'channel_id': channel_id} for s in selected]
        else:
            logger.warning(f"Only {len(unused)} videos available")
            return []
    except Exception as e:
        logger.error(f"Error: {e}")
        return []


# ============= SCRAPER FUNCTIONS =============
async def scrape_multiple_videos(count: int) -> List[Dict]:
    videos = []
    pages_to_scrape = max(5, count // 3)
    
    try:
        page_num = 1
        base_url = SCRAPER_CONFIG['base_url']
        pages_scraped = 0
        
        while len(videos) < count and pages_scraped < pages_to_scrape:
            page_url = f"{base_url}/" if page_num == 1 else f"{base_url}/page/{page_num}/"
            logger.info(f"Scraping page {page_num}")
            
            resp = requests.get(page_url, headers=SCRAPER_CONFIG['headers'], timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            
            for a_title in soup.find_all("a", class_="title", href=True):
                if len(videos) >= count:
                    break
                    
                href = a_title["href"].strip()
                if not href or not (href.startswith('/') or 'lol49.org' in href):
                    continue
                    
                full = urljoin(base_url, href)
                title_text = a_title.get_text(strip=True)
                
                if used_videos_collection.find_one({'url': full}):
                    continue
                
                thumb_block = a_title.find_previous("a", class_="thumb")
                img_el = thumb_block.find("img") if thumb_block else None
                dur_el = thumb_block.find("span", class_="video-duration") if thumb_block else None
                
                try:
                    r_page = requests.get(full, headers=SCRAPER_CONFIG['video_headers'], timeout=12)
                    sp = BeautifulSoup(r_page.text, "html.parser")
                    
                    direct_link = None
                    down_div = sp.find("div", class_="downLink")
                    if down_div:
                        form = down_div.find("form")
                        if form and form.get("action"):
                            direct_link = urljoin(full, form["action"])
                    
                    if not direct_link:
                        for form in sp.find_all("form"):
                            btn = form.find("button")
                            if btn and "Download Clip" in btn.get_text(strip=True):
                                action = form.get("action")
                                if action:
                                    direct_link = urljoin(full, action)
                                    break
                    
                    if direct_link:
                        videos.append({
                            "title": title_text,
                            "duration": dur_el.get_text(strip=True) if dur_el else "Unknown",
                            "thumb_url": img_el["src"] if img_el and img_el.get("src") else None,
                            "page": full,
                            "download_url": direct_link
                        })
                        
                        used_videos_collection.insert_one({
                            'url': full,
                            'title': title_text,
                            'timestamp': datetime.now()
                        })
                        
                        logger.info(f"Found video {len(videos)}/{count}")
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    continue
            
            page_num += 1
            pages_scraped += 1
            await asyncio.sleep(3)
        
        return videos
    except Exception as e:
        logger.error(f"Scraper error: {e}")
        return videos


# ============= POSTING FUNCTIONS =============
async def post_viral_heaven():
    logger.info("Starting Viral Heaven post")
    
    try:
        await cache_channel_messages(VIRAL_SOURCE, 'cache_viral')
        cache_collection = db['cache_viral']
        
        video_count = random.randint(*VIRAL_VIDEO_COUNT)
        videos_data = await get_unused_videos_from_channel(VIRAL_SOURCE, 'viral_heaven', cache_collection, video_count)
        
        if not videos_data:
            return False
        
        batch_no = await get_next_batch_number('viral_heaven')
        image_message = await get_random_unused_image()
        
        if not image_message:
            return False
        
        message_ids = [v['message_id'] for v in videos_data]
        batch_link = await get_batch_link_from_bot(
            VIRAL_HEAVEN_BOT,
            source_channel=VIRAL_SOURCE,
            message_ids=message_ids
        )
        
        if not batch_link:
            logger.error("Failed to get batch link for Viral Heaven")
            return False
        
        caption = f"""<Blockquote><b>⌯ ɴᴇᴡ ᴇɴɢʟɪsʜ ᴠɪᴅᴇᴏs ʙᴀᴛᴄʜ ⌯</b></Blockquote>

<b>🌟 • ʙᴀᴛᴄʜ ɴᴏ : {batch_no}
✨ • ᴠɪᴅᴇᴏs : {video_count}
💦 • ǫᴜᴀʟɪᴛʏ : ʜᴅ ǫᴜᴀʟɪᴛʏ
🎤 • ʟᴀɴɢᴜᴀɢᴇ : ᴇɴɢʟɪsʜ</b>

<Blockquote><b>⌯ ᴛᴀᴘ ᴏɴ ᴛʜᴇ ᴡᴀᴛᴄʜ ɴᴏᴡ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ᴠɪᴅᴇᴏs. ɪғ ʏᴏᴜ ᴅᴏɴ'ᴛ ᴋɴᴏᴡ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ ʟɪɴᴋs ᴛʜᴇɴ ғɪʀsᴛ ᴡᴀᴛᴄʜ ᴛʜᴇ ᴛᴜᴛᴏʀɪᴀʟ.</b></Blockquote>
<Blockquote><b>❗ᴅᴍ @Crystalar ᴛᴏ ʙᴜʏ ᴘʀᴇᴍɪᴜᴍ ᴍᴇᴍʙᴇʀsʜɪᴘ ᴀɴᴅ ᴅɪsᴀʙʟᴇ ᴀᴅs / ʟɪɴᴋs.</b></Blockquote>"""
        
        buttons = [
            [
                Button.url("✦ ᴡᴀᴛᴄʜ ɴᴏᴡ ✦", batch_link),
                Button.url("✦ ᴛᴜᴛᴏʀɪᴀʟ ✦", TUTORIAL_LINK)
            ],
            [
                Button.url("✦ ᴅɪsᴀʙʟᴇ ᴀᴅs ✦", DISABLE_ADS_LINK)
            ]
        ]
        
        image_path = await image_message.download_media(file=f"temp_image_{datetime.now().timestamp()}.jpg")
        await bot_client.send_file(VIRAL_TARGET, image_path, caption=caption, buttons=buttons, parse_mode='html', spoiler=True)
        os.remove(image_path)
        
        for video_data in videos_data:
            posted_history_collection.insert_one({
                'message_id': video_data['message_id'],
                'source_channel': video_data['channel_id'],
                'bot_name': 'viral_heaven',
                'batch_no': batch_no,
                'batch_link': batch_link,
                'timestamp': datetime.now()
            })
        
        logger.info(f"Viral Heaven success - Batch #{batch_no}")
        return True
    except Exception as e:
        logger.error(f"Viral Heaven error: {e}")
        return False


async def post_desi_bot():
    logger.info("Starting Desi Bot post")
    
    video_count = random.randint(*DESI_VIDEO_COUNT)
    videos_info = await scrape_multiple_videos(video_count)
    
    if not videos_info:
        return False
    
    success_count = 0
    
    for idx, video_info in enumerate(videos_info):
        temp_video_path = None
        temp_thumb_path = None
        db_message_id = None
        
        try:
            logger.info(f"Processing {idx+1}/{len(videos_info)}")
            
            filename = f"desi_video_{int(time.time())}_{idx}.mp4"
            temp_video_path = await download_video(video_info['download_url'], filename)
            if not temp_video_path:
                continue
            
            db_message = await session_client.send_file(
                DB_CHANNEL,
                temp_video_path,
                caption=f"Title: {video_info['title']}\nDuration: {video_info['duration']}"
            )
            db_message_id = db_message.id
            
            batch_link = await get_batch_link_from_bot(
                DESI_BOT,
                db_channel=DB_CHANNEL,
                db_message_id=db_message_id
            )
            
            if not batch_link:
                continue
            
            batch_no = await get_next_batch_number('desi_bot')
            
            caption = f"""<Blockquote><b>⌯ ɴᴇᴡ ᴅᴇsɪ ᴠɪᴅᴇᴏ ⌯</b></Blockquote>

<b>🌟 • ʙᴀᴛᴄʜ ɴᴏ : {batch_no}
💦 • ᴛɪᴛʟᴇ : {video_info['title']}
⏱️ • ᴅᴜʀᴀᴛɪᴏɴ : {video_info['duration']}</b>

<Blockquote><b>⌯ ᴛᴀᴘ ᴏɴ ᴛʜᴇ ᴡᴀᴛᴄʜ ɴᴏᴡ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ᴛʜᴇ ᴠɪᴅᴇᴏ.</b></Blockquote>
<Blockquote><b>❗ᴅᴍ @Crystalar ᴛᴏ ʙᴜʏ ᴘʀᴇᴍɪᴜᴍ ᴍᴇᴍʙᴇʀsʜɪᴘ ᴀɴᴅ ᴅɪsᴀʙʟᴇ ᴀᴅs / ʟɪɴᴋs.</b></Blockquote>"""
            
            if video_info.get('thumb_url'):
                try:
                    thumb_response = requests.get(video_info['thumb_url'], timeout=8)
                    if thumb_response.status_code == 200:
                        temp_thumb_path = f"/tmp/desi_thumb_{int(time.time())}_{idx}.jpg"
                        with open(temp_thumb_path, 'wb') as f:
                            f.write(thumb_response.content)
                except:
                    pass
            
            buttons = [
                [
                    Button.url("✦ ᴡᴀᴛᴄʜ ɴᴏᴡ ✦", batch_link),
                    Button.url("✦ ᴛᴜᴛᴏʀɪᴀʟ ✦", TUTORIAL_LINK)
                ],
                [
                    Button.url("✦ ᴅɪsᴀʙʟᴇ ᴀᴅs ✦", DISABLE_ADS_LINK)
                ]
            ]
            
            if temp_thumb_path and os.path.exists(temp_thumb_path):
                await bot_client.send_file(DESI_TARGET, temp_thumb_path, caption=caption, buttons=buttons, parse_mode='html', spoiler=True)
            else:
                await bot_client.send_message(DESI_TARGET, caption, buttons=buttons, parse_mode='html', link_preview=False)
            
            posted_history_collection.insert_one({
                'bot_name': 'desi_bot',
                'batch_no': batch_no,
                'batch_link': batch_link,
                'title': video_info['title'],
                'duration': video_info['duration'],
                'db_channel': DB_CHANNEL,
                'db_message_id': db_message_id,
                'timestamp': datetime.now()
            })
            
            success_count += 1
            logger.info(f"✅ Video {idx+1} posted")
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            if temp_video_path and os.path.exists(temp_video_path):
                os.remove(temp_video_path)
            if temp_thumb_path and os.path.exists(temp_thumb_path):
                os.remove(temp_thumb_path)
    
    return success_count > 0


async def post_japanese_bot():
    logger.info("Starting Japanese Bot post")
    
    try:
        await cache_channel_messages(JAPANESE_SOURCE, 'cache_japanese')
        cache_collection = db['cache_japanese']
        
        video_count = random.randint(*JAPANESE_VIDEO_COUNT)
        videos_data = await get_unused_videos_from_channel(JAPANESE_SOURCE, 'japanese_bot', cache_collection, video_count)
        
        if not videos_data:
            return False
        
        batch_no = await get_next_batch_number('japanese_bot')
        
        message_ids = [v['message_id'] for v in videos_data]
        batch_link = await get_batch_link_from_bot(
            JAPANESE_BOT,
            source_channel=JAPANESE_SOURCE,
            message_ids=message_ids
        )
        
        if not batch_link:
            logger.error("Failed to get batch link for Japanese Bot")
            return False
        
        caption = f"""<Blockquote><b>⌯ ɴᴇᴡ ᴊᴀᴘᴀɴᴇsᴇ ᴠɪᴅᴇᴏs ʙᴀᴛᴄʜ ⌯</b></Blockquote>

<b>🌟 • ʙᴀᴛᴄʜ ɴᴏ : {batch_no}
✨ • ᴠɪᴅᴇᴏs : {video_count}
💦 • ǫᴜᴀʟɪᴛʏ : ʜᴅ ǫᴜᴀʟɪᴛʏ
🎤 • ʟᴀɴɢᴜᴀɢᴇ : ᴊᴀᴘᴀɴᴇsᴇ</b>

<Blockquote><b>⌯ ᴛᴀᴘ ᴏɴ ᴛʜᴇ ᴡᴀᴛᴄʜ ɴᴏᴡ ʙᴜᴛᴛᴏɴ ʙᴇʟᴏᴡ ᴛᴏ ɢᴇᴛ ᴠɪᴅᴇᴏs. ɪғ ʏᴏᴜ ᴅᴏɴ'ᴛ ᴋɴᴏᴡ ʜᴏᴡ ᴛᴏ ᴏᴘᴇɴ ʟɪɴᴋs ᴛʜᴇɴ ғɪʀsᴛ ᴡᴀᴛᴄʜ ᴛʜᴇ ᴛᴜᴛᴏʀɪᴀʟ.</b></Blockquote>
<Blockquote><b>❗ᴅᴍ @Crystalar ᴛᴏ ʙᴜʏ ᴘʀᴇᴍɪᴜᴍ ᴍᴇᴍʙᴇʀsʜɪᴘ ᴀɴᴅ ᴅɪsᴀʙʟᴇ ᴀᴅs / ʟɪɴᴋs.</b></Blockquote>"""
        
        first_message = await session_client.get_messages(JAPANESE_SOURCE, ids=message_ids[0])
        thumb_path = None
        if first_message and first_message.video:
            try:
                if hasattr(first_message.video, 'thumb'):
                    thumb_data = await session_client.download_media(first_message.video.thumb, bytes)
                    if thumb_data:
                        thumb_path = f"/tmp/jp_thumb_{int(time.time())}.jpg"
                        with open(thumb_path, 'wb') as f:
                            f.write(thumb_data)
            except:
                pass
        
        buttons = [
            [
                Button.url("✦ ᴡᴀᴛᴄʜ ɴᴏᴡ ✦", batch_link),
                Button.url("✦ ᴛᴜᴛᴏʀɪᴀʟ ✦", TUTORIAL_LINK)
            ],
            [
                Button.url("✦ ᴅɪsᴀʙʟᴇ ᴀᴅs ✦", DISABLE_ADS_LINK)
            ]
        ]
        
        if thumb_path and os.path.exists(thumb_path):
            await bot_client.send_file(JAPANESE_TARGET, thumb_path, caption=caption, buttons=buttons, parse_mode='html', spoiler=True)
            os.remove(thumb_path)
        else:
            await bot_client.send_message(JAPANESE_TARGET, caption, buttons=buttons, parse_mode='html', link_preview=False)
        
        for video_data in videos_data:
            posted_history_collection.insert_one({
                'message_id': video_data['message_id'],
                'source_channel': video_data['channel_id'],
                'bot_name': 'japanese_bot',
                'batch_no': batch_no,
                'batch_link': batch_link,
                'timestamp': datetime.now()
            })
        
        logger.info(f"Japanese Bot success - Batch #{batch_no}")
        return True
    except Exception as e:
        logger.error(f"Japanese Bot error: {e}")
        return False


# ============= SEQUENTIAL EXECUTION =============
async def run_all_posts():
    global is_running
    
    async with execution_lock:
        if is_running:
            logger.info("Previous execution still running, skipping")
            return
        
        is_running = True
        logger.info("=" * 60)
        logger.info(f"Starting sequential posts at {datetime.now()}")
        logger.info("=" * 60)
        
        try:
            logger.info("\n[1/3] Running Viral Heaven...")
            await post_viral_heaven()
            await asyncio.sleep(15)
            
            logger.info("\n[2/3] Running Desi Bot...")
            await post_desi_bot()
            await asyncio.sleep(15)
            
            logger.info("\n[3/3] Running Japanese Bot...")
            await post_japanese_bot()
            
            logger.info("\n✅ All posts completed")
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            is_running = False


# ============= WEB SERVER =============
async def handle_all_requests(request):
    path = request.path
    
    if path == '/trigger' or path == '/trigger/':
        logger.info(f"Trigger received")
        asyncio.create_task(run_all_posts())
        return web.Response(text="OK", status=200)
    elif path == '/health' or path == '/health/':
        return web.Response(text="OK", status=200)
    else:
        return web.Response(text="Bot is running. Use /trigger to start posting.", status=200)


# ============= MAIN =============
async def main():
    logger.info("Starting Telegram Automation Bot")
    logger.info("=" * 60)
    
    await session_client.start()
    logger.info("✓ Session client connected")
    
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("✓ Bot client connected")
    
    logger.info("Initializing caches...")
    asyncio.create_task(cache_channel_messages(VIRAL_SOURCE, 'cache_viral'))
    asyncio.create_task(cache_channel_messages(JAPANESE_SOURCE, 'cache_japanese'))
    
    app = web.Application()
    app.router.add_route('*', '/', handle_all_requests)
    app.router.add_route('*', '/trigger', handle_all_requests)
    app.router.add_route('*', '/health', handle_all_requests)
    
    port = int(os.environ.get('PORT', 8080))
    
    logger.info("=" * 60)
    logger.info(f"Bot running on port {port}")
    logger.info("Trigger: GET /trigger")
    logger.info("Health: GET /health")
    logger.info("=" * 60)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    await asyncio.Event().wait()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped")
    except Exception as e:
        logger.error(f"Fatal error: {e}")