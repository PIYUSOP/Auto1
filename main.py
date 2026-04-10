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

# Scraper configuration - UPDATED
SCRAPER_CONFIG = {
    'base_url': 'https://lol49.org',
    'headers': {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'referer': 'https://lol49.org/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    },
    'video_headers': {
        'Referer': 'https://lol49.org/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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


# ============= BATCH LINK GENERATION =============
async def get_batch_link_from_bot(batch_bot_username: str, source_channel: int = None, 
                                  message_ids: List[int] = None, video_paths: List[str] = None,
                                  db_channel: int = None, db_message_id: int = None) -> Optional[str]:
    """
    Universal batch link generator
    """
    try:
        logger.info(f"Getting batch link from {batch_bot_username}")
        
        async with session_client.conversation(batch_bot_username, timeout=120) as conv:
            # Send /custom_batch
            await conv.send_message('/custom_batch')
            logger.info("Sent /custom_batch")
            await asyncio.sleep(3)
            
            # Handle initial response
            try:
                response = await conv.get_response(timeout=10)
                logger.info(f"Initial response: {response.text[:100] if response.text else 'No text'}")
                
                if response.buttons:
                    for row in response.buttons:
                        for button in row:
                            if any(kw in button.text.lower() for kw in ['new', 'batch', 'create', 'start']):
                                logger.info(f"Clicking initial: {button.text}")
                                await button.click()
                                await asyncio.sleep(3)
                                response = await conv.get_response(timeout=10)
            except asyncio.TimeoutError:
                pass
            
            # Send video(s)
            if db_channel and db_message_id:
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
            
            # Get response and click generate button
            try:
                response = await conv.get_response(timeout=30)
                logger.info(f"Response after videos: {response.text[:200] if response.text else 'No text'}")
                
                if response.buttons:
                    for row in response.buttons:
                        for button in row:
                            button_text = button.text
                            if ('generate' in button_text.lower() or 
                                'link' in button_text.lower() or
                                'ɢᴇɴᴇʀᴀᴛᴇ' in button_text or
                                'ʟɪɴᴋ' in button_text):
                                logger.info(f"Clicking generate: {button_text}")
                                await button.click()
                                await asyncio.sleep(5)
                                
                                link_response = await conv.get_response(timeout=30)
                                
                                if link_response.text:
                                    match = re.search(r'https://t\.me/[^\s]+start=[^\s]+', link_response.text)
                                    if match:
                                        return match.group()
                                
                                if link_response.buttons:
                                    for btn_row in link_response.buttons:
                                        for btn in btn_row:
                                            if hasattr(btn, 'url') and btn.url and 'start=' in btn.url:
                                                return btn.url
            except asyncio.TimeoutError:
                pass
            
            # Fallback
            async for msg in session_client.iter_messages(batch_bot_username, limit=10):
                if msg.text and 'start=' in msg.text:
                    match = re.search(r'https://t\.me/[^\s]+start=[^\s]+', msg.text)
                    if match:
                        return match.group()
                if msg.buttons:
                    for row in msg.buttons:
                        for button in row:
                            if hasattr(button, 'url') and button.url and 'start=' in button.url:
                                return button.url
            
            return None
            
    except Exception as e:
        logger.error(f"Error: {e}")
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


# ============= SCRAPER FUNCTIONS - FIXED =============
async def scrape_multiple_videos(count: int) -> List[Dict]:
    """Scrape multiple videos from lol49.org - FIXED for current structure"""
    videos = []
    
    try:
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'referer': 'https://lol49.org/',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        page_num = 1
        base_url = 'https://lol49.org'
        
        while len(videos) < count and page_num <= 20:
            # Construct URL
            if page_num == 1:
                page_url = f"{base_url}/"
            else:
                page_url = f"{base_url}/page/{page_num}/"
            
            logger.info(f"Scraping page {page_num}: {page_url}")
            
            try:
                resp = requests.get(page_url, headers=headers, timeout=20)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch page {page_num}: {e}")
                page_num += 1
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Find all video items
            video_items = soup.find_all("li", class_="video")
            
            if not video_items:
                logger.warning(f"No video items found on page {page_num}")
                page_num += 1
                continue
            
            logger.info(f"Found {len(video_items)} video items on page {page_num}")
            
            for item in video_items:
                if len(videos) >= count:
                    break
                
                # Get title and video URL
                title_elem = item.find("a", class_="title")
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                video_url = title_elem.get("href", "")
                
                if not video_url:
                    continue
                
                if not video_url.startswith("http"):
                    video_url = urljoin(base_url, video_url)
                
                # Check if already used
                existing = used_videos_collection.find_one({'url': video_url})
                if existing:
                    logger.info(f"Skipping already used: {title[:30]}")
                    continue
                
                # Get duration
                duration_elem = item.find("span", class_="video-duration")
                duration = duration_elem.get_text(strip=True) if duration_elem else "Unknown"
                
                # Get thumbnail
                thumb_elem = item.find("img")
                thumb_url = None
                if thumb_elem and thumb_elem.get("src"):
                    thumb_url = thumb_elem["src"]
                    if not thumb_url.startswith("http"):
                        thumb_url = urljoin(base_url, thumb_url)
                
                try:
                    # Get the video page to extract download link
                    logger.info(f"Fetching video page")
                    video_page_resp = requests.get(video_url, headers=headers, timeout=15)
                    video_soup = BeautifulSoup(video_page_resp.text, "html.parser")
                    
                    direct_link = None
                    
                    # Look for video source tag
                    video_tag = video_soup.find("video")
                    if video_tag:
                        source_tag = video_tag.find("source")
                        if source_tag and source_tag.get("src"):
                            direct_link = source_tag["src"]
                            logger.info(f"Found video source")
                    
                    # Look for download form action
                    if not direct_link:
                        download_div = video_soup.find("div", class_="downLink")
                        if download_div:
                            form = download_div.find("form")
                            if form and form.get("action"):
                                direct_link = form["action"]
                                if not direct_link.startswith("http"):
                                    direct_link = urljoin(video_url, direct_link)
                                logger.info(f"Found download form")
                    
                    if direct_link:
                        videos.append({
                            "title": title,
                            "duration": duration,
                            "thumb_url": thumb_url,
                            "page": video_url,
                            "download_url": direct_link
                        })
                        
                        used_videos_collection.insert_one({
                            'url': video_url,
                            'title': title,
                            'duration': duration,
                            'download_url': direct_link,
                            'timestamp': datetime.now()
                        })
                        
                        logger.info(f"✓ Found video {len(videos)}/{count}: {title[:40]}")
                        await asyncio.sleep(1)
                    else:
                        logger.warning(f"✗ No download link for: {title[:40]}")
                        
                except Exception as e:
                    logger.error(f"Error processing: {e}")
                    continue
            
            # Check for next page
            next_link = soup.find("a", string="Next")
            if not next_link:
                logger.info(f"No more pages after page {page_num}")
                break
            
            page_num += 1
            await asyncio.sleep(2)
        
        if not videos:
            logger.error("No new videos found")
        else:
            logger.info(f"Successfully scraped {len(videos)} new videos")
        
        return videos
        
    except Exception as e:
        logger.error(f"Scraper error: {e}")
        traceback.print_exc()
        return videos


# ============= POSTING FUNCTIONS =============
async def post_viral_heaven():
    logger.info("Starting Viral Heaven post")
    
    try:
        if not VIRAL_SOURCE or not VIRAL_TARGET or not VIRAL_HEAVEN_BOT:
            logger.info("Viral Heaven not configured, skipping")
            return True
        
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
        if not JAPANESE_SOURCE or not JAPANESE_TARGET or not JAPANESE_BOT:
            logger.info("Japanese Bot not configured, skipping")
            return True
        
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
keep_alive_task = None

async def keep_alive():
    while True:
        await asyncio.sleep(600)
        logger.info("Keep-alive - dyno still active")

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
    global keep_alive_task
    
    logger.info("Starting Telegram Automation Bot")
    logger.info("=" * 60)
    
    await session_client.start()
    logger.info("✓ Session client connected")
    
    await bot_client.start(bot_token=BOT_TOKEN)
    logger.info("✓ Bot client connected")
    
    logger.info("Initializing caches...")
    if VIRAL_SOURCE:
        asyncio.create_task(cache_channel_messages(VIRAL_SOURCE, 'cache_viral'))
    if JAPANESE_SOURCE:
        asyncio.create_task(cache_channel_messages(JAPANESE_SOURCE, 'cache_japanese'))
    
    keep_alive_task = asyncio.create_task(keep_alive())
    
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
