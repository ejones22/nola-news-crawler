"""
NOLA News RSS Crawler - Local Version for VSCode
Scrapes New Orleans civic news and uploads to Box
"""
import os
import time
import json
import hashlib
import datetime as dt
from pathlib import Path
from urllib.parse import urlparse
from io import BytesIO
import re
import ssl
import certifi

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import trafilatura

# Box SDK imports
from boxsdk import OAuth2, Client
from boxsdk.exception import BoxAPIException

# Fix SSL certificate issues on macOS
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context

# Load environment variables from .env
load_dotenv()

# Box credentials
BOX_CLIENT_ID = os.environ.get("BOX_CLIENT_ID")
BOX_CLIENT_SECRET = os.environ.get("BOX_CLIENT_SECRET")
BOX_ACCESS_TOKEN = os.environ.get("BOX_ACCESS_TOKEN")
BOX_REFRESH_TOKEN = os.environ.get("BOX_REFRESH_TOKEN")
BOX_FOLDER_ID = os.environ.get("BOX_FOLDER_ID", "0")  # "0" is root folder

TOKEN_URL = "https://api.box.com/oauth2/token"

# RSS Feeds - New Orleans civic news sources
FEEDS = [
    "https://veritenews.org/feed/",
    "https://thelensnola.org/feed/",
    "https://neworleanscitybusiness.com/feed/"
]

# Keywords for relevance filtering
KEYWORDS = {
    "city council", "ordinance", "zoning", "budget", "millage", "public works",
    "sewerage & water board", "swbno", "dpw", "planning commission", "permit",
    "tax", "mayor", "poll", "school board", "reform", "bond", "levy",
    "land use", "infrastructure", "litigation", "city hall", "municipal",
    "nopd", "nofd", "crime", "public safety", "drainage", "flooding",
    "affordable housing", "rta", "streetcar", "neighborhood", "city attorney",
    "audit", "tourism", "economic development", "property tax", "blight",
    "sanitation", "street repair", "pothole", "traffic", "parking"
}

def sha16(s: str) -> str:
    """Generate 16-character hash for URL identification"""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

def refresh_box_token(refresh_token: str) -> tuple[str, str]:
    """Refresh the Box access token using refresh token"""
    print("🔄 Refreshing Box access token...")
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": BOX_CLIENT_ID,
        "client_secret": BOX_CLIENT_SECRET,
    }
    resp = requests.post(TOKEN_URL, data=data)
    resp.raise_for_status()
    tokens = resp.json()
    
    new_access_token = tokens["access_token"]
    new_refresh_token = tokens["refresh_token"]
    
    # Update .env file
    update_env_tokens(new_access_token, new_refresh_token)
    print("✅ Token refreshed successfully")
    
    return new_access_token, new_refresh_token

def update_env_tokens(access_token: str, refresh_token: str):
    """Update .env file with new tokens"""
    env_path = Path(".env")
    
    if env_path.exists():
        content = env_path.read_text()
        lines = content.split('\n')
        
        # Remove old token lines
        lines = [l for l in lines if not l.startswith('BOX_ACCESS_TOKEN=') 
                 and not l.startswith('BOX_REFRESH_TOKEN=')]
        
        # Add new tokens
        lines.append(f"BOX_ACCESS_TOKEN={access_token}")
        lines.append(f"BOX_REFRESH_TOKEN={refresh_token}")
        
        env_path.write_text('\n'.join(lines))
    else:
        with open(".env", "a") as f:
            f.write(f"\nBOX_ACCESS_TOKEN={access_token}")
            f.write(f"\nBOX_REFRESH_TOKEN={refresh_token}")

def init_box_client() -> Client:
    """Initialize Box client with OAuth2 and automatic token refresh"""
    global BOX_ACCESS_TOKEN, BOX_REFRESH_TOKEN
    
    # Try to refresh token if we have a refresh token
    if BOX_REFRESH_TOKEN:
        try:
            BOX_ACCESS_TOKEN, BOX_REFRESH_TOKEN = refresh_box_token(BOX_REFRESH_TOKEN)
        except Exception as e:
            print(f"⚠️ Token refresh failed: {e}. Using existing access token.")
    
    auth = OAuth2(
        client_id=BOX_CLIENT_ID,
        client_secret=BOX_CLIENT_SECRET,
        access_token=BOX_ACCESS_TOKEN,
    )
    return Client(auth)

def load_articles_from_box(client: Client, folder_id: str) -> list[dict]:
    """Load existing articles from Box articles.json file"""
    print(f"📥 Loading existing articles from Box folder {folder_id}...")
    try:
        folder = client.folder(folder_id)
        items = folder.get_items()
        
        # Find articles.json file
        articles_file = None
        for item in items:
            if item.name == "articles.json" and item.type == "file":
                articles_file = item
                break
        
        if articles_file:
            # Download and parse the file
            content = client.file(articles_file.id).content()
            data = json.loads(content.decode('utf-8'))
            article_count = len(data) if isinstance(data, list) else 0
            print(f"✅ Found {article_count} existing articles")
            return data if isinstance(data, list) else []
        else:
            print("📝 No existing articles.json found, creating new file...")
            # Create empty articles.json
            empty_data = json.dumps([], indent=2).encode('utf-8')
            stream = BytesIO(empty_data)
            folder.upload_stream(stream, "articles.json")
            print("✅ Created new articles.json in Box")
            return []
    except BoxAPIException as e:
        print(f"❌ Box API Error: {e}")
        raise

def get_seen_urls(articles: list[dict]) -> set[str]:
    """Extract seen URL IDs from articles list"""
    return {article.get('id') for article in articles if article.get('id')}

def save_articles_to_box(client: Client, folder_id: str, articles: list[dict]):
    """Save articles list to Box, overwriting existing file"""
    print(f"💾 Saving {len(articles)} articles to Box...")
    try:
        folder = client.folder(folder_id)
        items = folder.get_items()
        
        # Find existing articles.json file
        articles_file = None
        for item in items:
            if item.name == "articles.json" and item.type == "file":
                articles_file = item
                break
        
        data = json.dumps(articles, indent=2, ensure_ascii=False).encode('utf-8')
        stream = BytesIO(data)
        
        if articles_file:
            # Update existing file
            client.file(articles_file.id).update_contents_with_stream(stream)
        else:
            # Create new file
            folder.upload_stream(stream, "articles.json")
        
        print("✅ Articles saved to Box")
            
    except BoxAPIException as e:
        print(f"❌ Box API Error: {e}")
        raise

def box_upload_file(client: Client, folder_id: str, file_path: Path, box_filename: str):
    """Upload a file to Box folder"""
    try:
        folder = client.folder(folder_id)
        items = folder.get_items()
        
        # Check if file already exists
        existing_file = None
        for item in items:
            if item.name == box_filename and item.type == "file":
                existing_file = item
                break
        
        with open(file_path, 'rb') as f:
            if existing_file:
                # Update existing file
                client.file(existing_file.id).update_contents_with_stream(f)
            else:
                # Upload new file
                folder.upload_stream(f, box_filename)
                
    except BoxAPIException as e:
        print(f"❌ Box API Error uploading {box_filename}: {e}")
        raise

def fetch_rss_entries():
    """Fetch entries from all RSS feeds"""
    print(f"\n📡 Fetching RSS feeds from {len(FEEDS)} sources...")
    total_entries = 0
    
    for url in FEEDS:
        print(f"   - Fetching: {url}")
        try:
            d = feedparser.parse(url)
            source = d.feed.get("title", urlparse(url).netloc)
            
            # Debug: Check feed status
            if hasattr(d, 'status'):
                print(f"     Status: {d.status}")
            if hasattr(d, 'bozo') and d.bozo:
                print(f"     ⚠️ Feed parsing warning: {d.get('bozo_exception', 'Unknown error')}")
            
            entry_count = len(d.entries)
            total_entries += entry_count
            print(f"     ✅ Found {entry_count} entries from {source}")
            
            for e in d.entries:
                yield {
                    "title": (e.get("title") or "").strip(),
                    "url": e.get("link"),
                    "published": e.get("published", ""),
                    "source": source,
                }
        except Exception as e:
            print(f"     ❌ Error fetching feed: {e}")
            continue
    
    print(f"\n📊 Total entries fetched: {total_entries}")

def extract_text(url: str) -> tuple[str, str]:
    """Extract article text using Selenium and Trafilatura"""
    print(f"   🔍 Extracting content from: {url}")
    
    # Setup Chrome options for local use
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--window-size=1366,768")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Use webdriver-manager to automatically handle ChromeDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        driver.get(url)
        
        # Wait for article content to load
        try:
            WebDriverWait(driver, 15).until(
                EC.any_of(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "article")),
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='entry-content']"))
                )
            )
            time.sleep(2)  # Give page time to fully render
        except Exception:
            pass  # Fall back to trafilatura on whole page
        
        # Get the page HTML
        html = driver.page_source
        
        # Use trafilatura with more permissive settings
        downloaded = trafilatura.extract(
            html, 
            include_comments=False, 
            include_tables=False,
            include_links=False,
            no_fallback=False  # Enable fallback extraction
        )
        
        # Extract title and metadata
        metadata = trafilatura.extract_metadata(html)
        title = ""
        
        if metadata:
            title = metadata.title or ""
        
        # If trafilatura failed, try BeautifulSoup fallback
        if not downloaded or len(downloaded) < 100:
            print(f"   ⚠️ Trafilatura extraction weak ({len(downloaded or '')} chars), trying BeautifulSoup fallback...")
            soup = BeautifulSoup(html, "lxml")
            
            # Try to find article content
            article = soup.find("article") or soup.find(class_=re.compile("entry-content|post-content|article-content"))
            
            if article:
                # Remove unwanted elements
                for element in article.find_all(['script', 'style', 'nav', 'aside', 'header', 'footer']):
                    element.decompose()
                
                # Get text
                paragraphs = [p.get_text(strip=True) for p in article.find_all(['p', 'h2', 'h3'])]
                downloaded = "\n\n".join([p for p in paragraphs if len(p) > 20])
            
            # If still nothing, get all paragraphs from page
            if not downloaded or len(downloaded) < 100:
                paragraphs = [p.get_text(strip=True) for p in soup.find_all('p')]
                downloaded = "\n\n".join([p for p in paragraphs if len(p) > 20])
            
            if not title:
                title_tag = soup.find('title')
                title = title_tag.get_text(strip=True) if title_tag else ""
        
        text = downloaded or ""
        
        print(f"   ✅ Extracted {len(text)} characters, title: '{title[:50]}...'")
        
    finally:
        driver.quit()
    
    return title, text

def looks_relevant(title: str, text: str) -> bool:
    """Check if article is relevant based on keywords"""
    blob = f"{title}\n{text}".lower()
    matched_keywords = [k for k in KEYWORDS if k in blob]
    
    if matched_keywords:
        print(f"   ✅ Relevant (matched: {', '.join(matched_keywords[:3])}{'...' if len(matched_keywords) > 3 else ''})")
        return True
    else:
        print(f"   ⏭️  Not relevant (no keyword matches)")
        return False

def record_md(rec: dict, full_text: str) -> bytes:
    """Generate markdown file content"""
    md = f"""---
source: {rec['source']}
title: {rec['title']}
url: {rec['url']}
published: {rec.get('published', '')}
saved_at: {rec['saved_at']}
---

{full_text}
"""
    return md.encode("utf-8")

def main():
    """Main crawler function"""
    print("\n" + "="*70)
    print("🗞️  NOLA News RSS Crawler")
    print("="*70)
    
    # Validate environment variables
    assert BOX_CLIENT_ID, "❌ Missing BOX_CLIENT_ID in .env file"
    assert BOX_CLIENT_SECRET, "❌ Missing BOX_CLIENT_SECRET in .env file"
    assert BOX_ACCESS_TOKEN or BOX_REFRESH_TOKEN, "❌ Missing both BOX_ACCESS_TOKEN and BOX_REFRESH_TOKEN in .env file"
    
    print(f"📦 Box Folder ID: {BOX_FOLDER_ID}")
    
    # Initialize Box client
    client = init_box_client()
    
    # Load existing articles from Box
    articles = load_articles_from_box(client, BOX_FOLDER_ID)
    seen = get_seen_urls(articles)
    print(f"📊 Already tracking {len(seen)} articles")
    
    # Create local output directory
    output_dir = Path("out")
    output_dir.mkdir(exist_ok=True)
    print(f"📁 Local output directory: {output_dir.absolute()}")
    
    new_articles = []
    processed_count = 0
    skipped_seen = 0
    skipped_no_url = 0
    
    # Process RSS entries
    for item in fetch_rss_entries():
        url = item["url"]
        if not url:
            skipped_no_url += 1
            print(f"⏭️  Skipping entry with no URL: {item['title'][:50]}")
            continue
        
        key = sha16(url)
        if key in seen:
            skipped_seen += 1
            continue
        
        processed_count += 1
        print(f"\n{'='*70}")
        print(f"[{processed_count}] Processing: {item['title'][:60]}...")
        print(f"   Source: {item['source']}")
        print(f"   URL: {url}")
        
        try:
            # Extract article content
            title_ext, text = extract_text(url)
            title = title_ext or item["title"]
            
            # Check relevance
            if not looks_relevant(title, text):
                continue
            
            full_text = (text or "").strip()
            
            # Create article record
            rec = {
                "id": key,
                "source": item["source"],
                "url": url,
                "title": title,
                "published": item.get("published", ""),
                "saved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "content_preview": full_text[:200] + "..." if len(full_text) > 200 else full_text
            }
            
            # Generate filename
            published_date = rec["published"][:10] if rec.get("published") else dt.date.today().isoformat()
            clean_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
            clean_title = clean_title[:50]
            md_filename = f"{published_date}_{key}_{clean_title}.md"
            
            # Save markdown file locally
            md_path = output_dir / md_filename
            md_path.write_bytes(record_md(rec, full_text))
            print(f"   💾 Saved locally: {md_filename}")
            
            # Upload to Box
            box_upload_file(client, BOX_FOLDER_ID, md_path, md_filename)
            print(f"   ☁️  Uploaded to Box")
            
            # Add to tracking
            new_articles.append(rec)
            seen.add(key)
            
            # Be polite to servers
            print(f"   ⏳ Waiting 10 seconds...")
            time.sleep(10)
            
        except Exception as e:
            print(f"   ❌ Error processing article: {e}")
            continue
    
    # Save updated article list to Box
    if new_articles:
        articles.extend(new_articles)
        save_articles_to_box(client, BOX_FOLDER_ID, articles)
        
        print("\n" + "="*70)
        print(f"Success! Added {len(new_articles)} new articles")
        print("="*70)
        print(f"Total articles tracked: {len(articles)}")
        print(f"Local files saved to: {output_dir.absolute()}")
        print(f" Files uploaded to Box folder: {BOX_FOLDER_ID}")
    else:
        print("\n" + "="*70)
        print("ℹNo new relevant articles found")
        print("="*70)
        print(f"Stats:")
        print(f"   - Entries fetched: {processed_count + skipped_seen + skipped_no_url}")
        print(f"   - Already seen: {skipped_seen}")
        print(f"   - No URL: {skipped_no_url}")
        print(f"   - Processed: {processed_count}")
        print(f"   - Relevant: {len(new_articles)}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Crawler interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        raise