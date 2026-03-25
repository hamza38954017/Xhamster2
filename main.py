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
BASE_DOMAIN = "https://xhamster45.desi"

# Limits & Bounds
VIDEO_CONCURRENCY_LIMIT = 100
START_ID = 1
END_ID = 22875
BATCH_SIZE = 1000
# -----------------------------------

PROXY_POOL = []
DYNAMIC_NODES = []

def send_csv_to_telegram(filepath):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        with open(filepath, 'rb') as f:
            response = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID}, files={'document': f})
            if response.status_code == 200:
                print(f"📤 Successfully sent {filepath} to Telegram!")
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")

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

async def check_firebase_node(fb_session, node_name, video_id):
    """Helper to check if video exists in a specific Firebase node"""
    node_url = f"{FIREBASE_DB_URL}/{node_name}/{video_id}.json"
    try:
        async with fb_session.get(node_url) as check_resp:
            if await check_resp.json() is not None:
                return True
    except:
        pass
    return False

async def fetch_dynamic_nodes(fb_session):
    """Fetches top-level keys from Firebase to use as dynamic channel/node lists."""
    print("🔍 Fetching dynamic database structure from Firebase...")
    try:
        # shallow=true fetches just the keys, saving massive bandwidth
        async with fb_session.get(f"{FIREBASE_DB_URL}/.json?shallow=true") as resp:
            data = await resp.json()
            if data:
                # Get all root nodes, filter out 'all' since we check that separately
                nodes = [key for key in data.keys() if key != "all"]
                print(f"✅ Found {len(nodes)} dynamic root nodes in Firebase: {', '.join(nodes[:5])}...")
                return nodes
    except Exception as e:
        print(f"⚠️ Error fetching dynamic nodes: {e}")
    return []

async def process_single_video(cffi_session, fb_session, semaphore, video_id_num):
    video_id = str(video_id_num)
    url = f"{BASE_DOMAIN}/{video_id}" 

    async with semaphore:
        # 1. CHECK FIREBASE 'all' NODE FIRST (Fastest check)
        if await check_firebase_node(fb_session, "all", video_id):
            print(f"   ⏭️ Skipped: {video_id} is already in /all node.")
            return None

        # 2. CHECK ALL DYNAMIC NODES CONCURRENTLY
        if DYNAMIC_NODES:
            node_checks = [check_firebase_node(fb_session, node, video_id) for node in DYNAMIC_NODES]
            node_results = await asyncio.gather(*node_checks)
            
            if any(node_results):
                print(f"   ⏭️ Skipped: {video_id} is already in a dynamic channel node.")
                return None

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

        if title == "Unknown Title" and not tags_array:
            # Page not found or invalid format
            return None

        # 4. INSTANT FIREBASE UPLOAD TO 'all' NODE
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
            async with fb_session.put(f"{FIREBASE_DB_URL}/all/{video_id}.json", json=firebase_payload) as put_resp:
                if put_resp.status == 200:
                    print(f"   🔥 Saved to DB (/all): {title[:20]}...")
        except Exception as e:
            print(f"   ❌ DB Error: {e}")

        csv_tags_string = ", ".join(tags_array)
        return [title, channel_name, csv_tags_string, duration, views, likes, dislikes, thumbnail_url, preview_url, url]

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
    global PROXY_POOL, DYNAMIC_NODES
    
    await start_dummy_server()
    
    start_time = time.time()
    PROXY_POOL = fetch_free_proxies()
    
    video_semaphore = asyncio.Semaphore(VIDEO_CONCURRENCY_LIMIT)

    # Increased aiohttp connection limits to handle simultaneous DB checks smoothly
    connector = aiohttp.TCPConnector(limit=5000)

    async with AsyncSession() as cffi_session, aiohttp.ClientSession(connector=connector) as fb_session:
        # Fetch the dynamic nodes right at the start
        DYNAMIC_NODES = await fetch_dynamic_nodes(fb_session)

        # Create all tasks upfront
        tasks = [
            process_single_video(cffi_session, fb_session, video_semaphore, vid_id)
            for vid_id in range(START_ID, END_ID + 1)
        ]
        
        print(f"\n🚀 Starting direct scrape of {END_ID - START_ID + 1} videos with {VIDEO_CONCURRENCY_LIMIT} concurrency...\n")
        
        accumulated_data = []
        batch_count = 1
        
        # Process results as soon as they complete
        for coroutine in asyncio.as_completed(tasks):
            result = await coroutine
            
            if result:
                accumulated_data.append(result)
                
                # Check if we hit the 1000 TG limit
                if len(accumulated_data) >= BATCH_SIZE:
                    filename = f"scraped_direct_batch_{batch_count}.csv"
                    headers = ["Title", "Channel Name", "Tags", "Duration (sec)", "Total Views", "Likes", "Dislikes", "Thumbnail URL", "Preview URL", "Page URL"]

                    with open(filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(headers)
                        writer.writerows(accumulated_data)

                    send_csv_to_telegram(filename)
                    
                    # Clear the list safely to prepare for the next batch
                    accumulated_data.clear()
                    batch_count += 1

        # Send any leftover data that didn't reach the 1000 mark at the very end
        if accumulated_data:
            filename = f"scraped_direct_batch_final.csv"
            headers = ["Title", "Channel Name", "Tags", "Duration (sec)", "Total Views", "Likes", "Dislikes", "Thumbnail URL", "Preview URL", "Page URL"]

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(accumulated_data)

            send_csv_to_telegram(filename)

    total_time = time.time() - start_time
    print(f"\n🎉 ALL SCRAPING COMPLETED in {total_time:.2f} seconds!")
    
    print("🛑 Scraping finished. Keeping dummy server alive to prevent Render restart loop...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main_async())
