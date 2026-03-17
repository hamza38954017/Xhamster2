import os
import time
import csv
import json
import re
import random
import asyncio
import requests
import aiohttp
from aiohttp import web
from curl_cffi.requests import AsyncSession

# ---------- CONFIGURATION ----------
TELEGRAM_BOT_TOKEN = "8349995675:AAE9grCMm22vWOzmAjlDtpRd4iMR8IQiVgA"
TELEGRAM_CHAT_ID = "7369364451"

# Your Firebase Database URL
FIREBASE_DB_URL = "https://xhamster-70a9b-default-rtdb.firebaseio.com"

# Paste your channel names and max pages here
CHANNELS = ["aunt-judys-xxx", "adult-time", "trike-patrol", "swappz", "mommys-boy", "my-friends-hot-mom-channel", "fakings", "hotwife-xxx", "freeuse", "brattysis", "xtime-network", "mmv-films", "indian-real-porn", "family-xxx", "shagging-moms", "inkasex", "asian-sex-diary", "alfacontent-world", "av-jiali", "shop-lyfter", "fill-up-my-stepmom", "sunny-leone", "tuktuk-patrol", "queen-star-desi", "nuru-massage", "indian-threesome"]
MAX_PAGES = [22, 48, 17, 10, 8, 7, 52, 8, 9, 10, 265, 125, 1, 10, 92, 9, 18, 374, 28, 17, 4, 1, 11, 1, 29, 1]

BASE_DOMAIN = "https://xhamster45.desi"
BASE_CHANNEL_URL = f"{BASE_DOMAIN}/channels"

# Concurrency limits
CHANNEL_CONCURRENCY_LIMIT = 26 # Scrape 5 channels at once
VIDEO_CONCURRENCY_LIMIT = 30   # 15 concurrent video checks per channel
# -----------------------------------

PROXY_POOL = []

def send_csv_to_telegram(filepath):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(filepath, 'rb') as f:
            response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID}, files={'document': f})
            if response.status_code == 200:
                print(f"📤 Successfully sent {filepath} to Telegram!")
    except Exception:
        pass

def fetch_free_proxies():
    print("🔄 Downloading fresh list of elite free proxies...")
    api_url = "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&country=all&ssl=all&anonymity=elite"
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            proxies = response.text.strip().split('\r\n')
            valid_proxies = [f"http://{p}" for p in proxies if p]
            print(f"✅ Successfully loaded {len(valid_proxies)} free proxies.")
            return valid_proxies
    except Exception:
        pass
    return []

def recursive_preview_search(data):
    if isinstance(data, dict):
        for k, v in data.items():
            k_lower = str(k).lower()
            if isinstance(v, str) and v.startswith('http') and ('.mp4' in v or '.webm' in v):
                if 'preview' in k_lower or 'trailer' in k_lower or 'preview' in v.lower():
                    return v.replace('\\/', '/')
            res = recursive_preview_search(v)
            if res: return res
    elif isinstance(data, list):
        for item in data:
            res = recursive_preview_search(item)
            if res: return res
    return None

async def fetch_html_zero_loss(cffi_session, url):
    """Infinite proxy rotation for stealth scraping. Refetches when pool is empty."""
    global PROXY_POOL
    while True:
        if not PROXY_POOL:
            PROXY_POOL = fetch_free_proxies()
            if not PROXY_POOL:
                print("⚠️ Proxy pool empty. Waiting 5 seconds before retrying...")
                await asyncio.sleep(5)
                continue

        proxy_url = random.choice(PROXY_POOL)
        proxies = {"http": proxy_url, "https": proxy_url}

        try:
            response = await cffi_session.get(url, proxies=proxies, timeout=10, impersonate="chrome120")
            if response.status_code == 200:
                return response.text
            elif response.status_code in [403, 429]:
                PROXY_POOL.remove(proxy_url)
            else:
                PROXY_POOL.remove(proxy_url)
        except Exception:
            if proxy_url in PROXY_POOL:
                PROXY_POOL.remove(proxy_url)

async def process_single_video(cffi_session, fb_session, semaphore, url, channel_node):
    async with semaphore:
        # 1. GENERATE UNIQUE ID
        video_id = url.split('-')[-1]
        
        # USE DYNAMIC NODE BASED ON CHANNEL
        firebase_node_url = f"{FIREBASE_DB_URL}/{channel_node}/{video_id}.json"

        # 2. CHECK FIREBASE IF ALREADY EXISTS
        try:
            async with fb_session.get(firebase_node_url) as check_resp:
                if await check_resp.json() is not None:
                    print(f"   ⏭️ Skipped: {video_id} is already in Firebase under /{channel_node}.")
                    return None
        except Exception as e:
            pass

        # 3. SCRAPE THE DATA
        html = await fetch_html_zero_loss(cffi_session, url)

        title, channel_name, duration = "Unknown Title", "Unknown Channel", 0
        views, likes, dislikes = 0, 0, 0
        tags_array = []
        thumbnail_url, preview_url = "", "Preview not found"

        title_match = re.search(r'<title>(.*?)</title>', html)
        if title_match:
            title = title_match.group(1).replace(" | xHamster", "").strip()

        data = {}
        json_match = re.search(r'window\.initials\s*=\s*({.+?});\s*</script>', html, re.DOTALL)
        if json_match:
            try: data = json.loads(json_match.group(1))
            except: pass

        if not data:
            script_matches = re.findall(r'<script[^>]*>(.*?videoModel.*?)</script>', html, re.DOTALL | re.IGNORECASE)
            for script in script_matches:
                try:
                    start, end = script.find('{'), script.rfind('}') + 1
                    data = json.loads(script[start:end])
                    break
                except: continue

        if data:
            try:
                vid_entity = data.get("videoEntity", {})
                vid_model = data.get("videoModel", {})

                title = vid_entity.get("title", title)
                channel_name = vid_model.get("channelModel", {}).get("channelName", vid_model.get("author", {}).get("name", "Unknown Channel"))

                tags_array = [t.get("name") for t in data.get("videoTagsComponent", {}).get("tags", []) if t.get("name")]

                duration, views = vid_entity.get("duration", 0), vid_entity.get("views", 0)
                likes, dislikes = vid_entity.get("rating", {}).get("likes", 0), vid_entity.get("rating", {}).get("dislikes", 0)
                thumbnail_url = vid_entity.get("thumbBig", data.get("videoInfo", {}).get("thumbUrl", ""))

                found_preview = recursive_preview_search(data)
                if found_preview: preview_url = found_preview
            except: pass

        if preview_url == "Preview not found":
            preview_match = re.search(r'(https?:\/\/[^\s<>"\'\\]*(?:preview|trailer|heat-preview)[^\s<>"\'\\]*\.mp4)', html, re.IGNORECASE)
            if preview_match:
                preview_url = preview_match.group(1).replace('\\/', '/')

        # 4. INSTANT FIREBASE UPLOAD TO DYNAMIC NODE (No Streaming URL)
        firebase_payload = {
            "title": title,
            "channel_name": channel_name,
            "tags": tags_array,
            "duration": duration,
            "views": views,
            "likes": likes,
            "dislikes": dislikes,
            "thumbnail_url": thumbnail_url,
            "preview_url": preview_url,
            "page_url": url,
            "scraped_at": time.time()
        }

        try:
            async with fb_session.put(firebase_node_url, json=firebase_payload) as put_resp:
                if put_resp.status == 200:
                    print(f"   🔥 Saved to DB (/{channel_node}): {title[:20]}...")
        except Exception as e:
            print(f"   ❌ DB Error: {e}")

        csv_tags_string = ", ".join(tags_array)
        return [title, channel_name, csv_tags_string, duration, views, likes, dislikes, thumbnail_url, preview_url, url]

async def process_page_and_batch(cffi_session, fb_session, page_url, semaphore, current_page, channel_node):
    print(f"\n📄 [Channel: {channel_node}] Gathering links from: {page_url}")

    html = await fetch_html_zero_loss(cffi_session, page_url)

    links_absolute = re.findall(r'href="(https://xhamster45\.desi/videos/[^"]+)"', html)
    links_relative = re.findall(r'href="(/videos/[^"]+)"', html)

    unique_links = []
    all_links = links_absolute + [f"{BASE_DOMAIN}{l}" for l in links_relative]

    for link in all_links:
        clean_url = link.split('#')[0].split('?')[0]
        if clean_url not in unique_links and clean_url != f"{BASE_DOMAIN}/videos":
            unique_links.append(clean_url)

    if not unique_links:
        return []

    print(f"🚀 [Channel: {channel_node}] Found {len(unique_links)} videos. Extracting...")

    # EXPLICITLY CREATE TASKS TO FIX AIOHTTP ERROR
    tasks = [
        asyncio.create_task(process_single_video(cffi_session, fb_session, semaphore, url, channel_node)) 
        for url in unique_links
    ]
    results = await asyncio.gather(*tasks)

    # Return the valid results instead of saving the CSV immediately
    valid_results = [r for r in results if r is not None and r[0] != "Unknown Title"]
    return valid_results

async def process_channel(cffi_session, fb_session, channel_name, max_pages, channel_semaphore, video_semaphore):
    async with channel_semaphore:
        print(f"\n🎬 Starting to scrape channel: {channel_name} (Up to {max_pages} pages)")
        
        # --- FETCH PROGRESS FROM FIREBASE ---
        start_page = 1
        progress_url = f"{FIREBASE_DB_URL}/scraping_progress/{channel_name}.json"
        try:
            async with fb_session.get(progress_url) as resp:
                if resp.status == 200:
                    last_page = await resp.json()
                    if last_page and isinstance(last_page, int):
                        start_page = last_page + 1
                        print(f"   🔄 Resuming {channel_name} from page {start_page}...")
        except Exception as e:
            print(f"   ⚠️ Could not fetch progress for {channel_name}, starting from page 1.")
        # ----------------------------------------------

        if start_page > max_pages:
            print(f"✅ Channel {channel_name} is already fully scraped (Max pages: {max_pages}).")
            return
        
        accumulated_data = []
        start_batch_page = start_page

        for current_page in range(start_page, max_pages + 1):
            page_url = f"{BASE_CHANNEL_URL}/{channel_name}" if current_page == 1 else f"{BASE_CHANNEL_URL}/{channel_name}/{current_page}"
            
            page_results = await process_page_and_batch(cffi_session, fb_session, page_url, video_semaphore, current_page, channel_name)
            
            if page_results:
                accumulated_data.extend(page_results)
            
            # --- IF WE HIT 100+ ITEMS OR IT'S THE VERY LAST PAGE ---
            if len(accumulated_data) >= 100 or current_page == max_pages:
                if accumulated_data:
                    # Save the accumulated results
                    filename = f"{channel_name}_pages_{start_batch_page}-{current_page}.csv"
                    headers = ["Title", "Channel Name", "Tags", "Duration (sec)", "Total Views", "Likes", "Dislikes", "Thumbnail URL", "Preview URL", "Page URL"]

                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        writer.writerows(accumulated_data)

                    send_csv_to_telegram(filename)
                    
                    # Empty the list to start fresh for the next batch
                    accumulated_data = []
                    start_batch_page = current_page + 1

                # --- SAVE PROGRESS TO FIREBASE (ONLY AFTER SUCCESSFUL BATCH) ---
                try:
                    async with fb_session.put(progress_url, json=current_page) as put_resp:
                        pass # Silently update the highest completed page
                except Exception as e:
                    print(f"   ⚠️ Failed to save progress for {channel_name}: {e}")
                # -------------------------------------------
        
        print(f"\n✅ Finished processing channel: {channel_name}")

# --- Render Web Service Dummy Server ---
async def health_check(request):
    return web.Response(text="Scraper is running OK")

async def start_dummy_server():
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Dummy web server started on port {port} for Render health checks.")
# ---------------------------------------

async def main_async():
    global PROXY_POOL
    
    # Start the dummy server so Render doesn't crash the app
    await start_dummy_server()
    
    start_time = time.time()
    PROXY_POOL = fetch_free_proxies()
    
    channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY_LIMIT)
    video_semaphore = asyncio.Semaphore(VIDEO_CONCURRENCY_LIMIT)

    async with AsyncSession() as cffi_session, aiohttp.ClientSession() as fb_session:
        # EXPLICITLY CREATE TASKS TO FIX AIOHTTP ERROR
        tasks = []
        for channel_name, pages in zip(CHANNELS, MAX_PAGES):
            task = asyncio.create_task(
                process_channel(cffi_session, fb_session, channel_name, pages, channel_semaphore, video_semaphore)
            )
            tasks.append(task)
            
        await asyncio.gather(*tasks)

    total_time = time.time() - start_time
    print(f"\n🎉 ALL SCRAPING COMPLETED in {total_time:.2f} seconds!")
    
    # --- KEEP ALIVE FOR RENDER ---
    print("🛑 Scraping finished. Keeping dummy server alive to prevent Render restart loop...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main_async())
