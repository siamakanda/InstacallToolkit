#!/usr/bin/env python3
"""
High-Speed Async Phone Scraper - Updated for CSV format
10-20x faster than requests version
"""

import asyncio
import aiohttp
import csv
import random
import time
import logging
from typing import List, Dict
from datetime import datetime
from lxml import html  # 3x faster than BeautifulSoup

# ========== CONFIGURATION ==========
CONFIG = {
    # Files
    "input_file": "numbers.csv",
    "output_file": "results.csv",
    
    # Async settings
    "concurrent_requests": 30,      # Number of parallel requests
    "timeout": 15,                  # Seconds per request
    "max_retries": 2,               # Retry failed requests
    
    # Rate limiting
    "requests_per_second": 5,       # Rate limit (requests/sec)
    "batch_size": 100,              # Write to CSV every N records
    
    # Performance
    "use_compression": True,        # Enable gzip/brotli
    "connection_limit": 100,        # Max simultaneous connections
    
    # Headers
    "headers": {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    },
}

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]

# ========== GLOBAL STATE ==========
stats = {
    "total": 0,
    "success": 0,
    "failed": 0,
    "rate_limited": 0,
    "skipped_invalid": 0,
    "start_time": time.time(),
}

results_buffer = []  # Buffer for batch writing

# ========== CORE FUNCTIONS ==========

def setup_logging():
    """Setup fast logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        handlers=[logging.StreamHandler()]
    )

def clean_number(number):
    """Fast phone number cleaning - keep only digits"""
    # Fastest way: filter digits
    digits = []
    for char in str(number):
        if char.isdigit():
            digits.append(char)
    cleaned = ''.join(digits)
    
    # Remove US country code (1) if present
    if cleaned.startswith('1') and len(cleaned) == 11:
        cleaned = cleaned[1:]  # Remove leading 1
    
    return cleaned

def read_numbers():
    """Read phone numbers from CSV file (first column has numbers)"""
    numbers = []
    try:
        with open(CONFIG["input_file"], 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            
            # Try to detect header
            first_row = next(reader, None)
            if first_row:
                # Check if first cell looks like a header (contains letters)
                if any(char.isalpha() for char in str(first_row[0])):
                    print(f"✓ Detected header: {first_row[0]}")
                    # First row is header, start from second row
                    start_from_row = 1
                else:
                    # First row is data, no header
                    start_from_row = 0
                    # Process the first row
                    cleaned = clean_number(first_row[0].strip())
                    if len(cleaned) == 10:
                        numbers.append(cleaned)
                    else:
                        stats["skipped_invalid"] += 1
            
            # Reset file pointer and skip rows as needed
            f.seek(0)
            reader = csv.reader(f)
            
            for i, row in enumerate(reader):
                if i < start_from_row:
                    continue  # Skip header row
                
                if row and row[0].strip():  # First column has phone number
                    original = row[0].strip()
                    cleaned = clean_number(original)
                    
                    if len(cleaned) == 10:  # Valid 10-digit US number
                        numbers.append(cleaned)
                    elif cleaned:  # Has digits but not 10
                        print(f"  Skipping invalid length: {original} -> {cleaned} ({len(cleaned)} digits)")
                        stats["skipped_invalid"] += 1
                    # else: empty string, skip
        
        print(f"✓ Loaded {len(numbers)} valid 10-digit phone numbers")
        if stats["skipped_invalid"] > 0:
            print(f"  Skipped {stats['skipped_invalid']} invalid numbers")
        return numbers
        
    except Exception as e:
        print(f"✗ Error reading file: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_random_headers():
    """Get headers with random user agent"""
    headers = CONFIG["headers"].copy()
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return headers

def parse_html_fast(html_content, phone_number):
    """Ultra-fast HTML parsing with lxml"""
    try:
        tree = html.fromstring(html_content)
        
        # XPath is much faster than BeautifulSoup
        def get_text(xpath):
            elements = tree.xpath(xpath)
            return elements[0].text.strip() if elements else ""
        
        # Extract data using XPath
        reputation = get_text('//div[@id="userReputation"]/h3')
        
        return {
            "phone_number": phone_number,
            "reputation": reputation if reputation else "Not Found",
            "user_reports": get_text('//div[@id="userReports"]/h3'),
            "total_calls": get_text('//div[@id="totalCall"]/h3'),
            "last_call": get_text('//div[@id="lastCall"]/h3'),
            "scraped_at": datetime.now().isoformat(),
        }
        
    except Exception:
        return {
            "phone_number": phone_number,
            "reputation": "Parse Error",
            "user_reports": "",
            "total_calls": "",
            "last_call": "",
            "scraped_at": datetime.now().isoformat(),
        }

def save_batch():
    """Save results buffer to CSV (batch writing)"""
    if not results_buffer:
        return
    
    try:
        # Append to file
        file_exists = False
        try:
            with open(CONFIG["output_file"], 'r') as f:
                file_exists = True
        except:
            pass
        
        with open(CONFIG["output_file"], 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=results_buffer[0].keys())
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(results_buffer)
        
        print(f"  ✓ Saved {len(results_buffer)} records to CSV")
        results_buffer.clear()
        
    except Exception as e:
        print(f"  ✗ Error saving batch: {e}")

class RateLimiter:
    """Simple async rate limiter"""
    def __init__(self, rate_per_second):
        self.rate = rate_per_second
        self.tokens = rate_per_second
        self.updated_at = time.time()
    
    async def acquire(self):
        """Wait if we're hitting rate limits"""
        now = time.time()
        elapsed = now - self.updated_at
        self.tokens += elapsed * self.rate
        self.tokens = min(self.tokens, self.rate)
        self.updated_at = now
        
        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / self.rate
            await asyncio.sleep(sleep_time)
            self.tokens = self.rate
        
        self.tokens -= 1

async def fetch_single(session, phone_number, semaphore, rate_limiter):
    """Fetch data for a single phone number"""
    async with semaphore:  # Limit concurrent requests
        await rate_limiter.acquire()  # Rate limiting
        
        url = f"https://lookup.robokiller.com/search?q={phone_number}"
        headers = get_random_headers()
        
        for attempt in range(CONFIG["max_retries"] + 1):
            try:
                async with session.get(
                    url, 
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"])
                ) as response:
                    
                    if response.status == 200:
                        html_content = await response.text()
                        data = parse_html_fast(html_content, phone_number)
                        
                        if data["reputation"] not in ["Parse Error", "Not Found"]:
                            stats["success"] += 1
                            print(f"✓ {phone_number}: {data['reputation']}")
                        else:
                            print(f"✓ {phone_number}: No data")
                        
                        return data
                    
                    elif response.status == 429:  # Rate limited
                        stats["rate_limited"] += 1
                        print(f"⚠ {phone_number}: Rate limited")
                        
                        # Exponential backoff
                        wait_time = 2 ** (attempt + 1)
                        await asyncio.sleep(wait_time + random.uniform(0, 1))
                        continue
                    
                    else:
                        print(f"✗ {phone_number}: HTTP {response.status}")
                
            except asyncio.TimeoutError:
                print(f"✗ {phone_number}: Timeout (attempt {attempt + 1})")
            except Exception as e:
                print(f"✗ {phone_number}: {type(e).__name__}")
            
            # Wait before retry
            if attempt < CONFIG["max_retries"]:
                await asyncio.sleep(random.uniform(1, 3))
        
        # All retries failed
        stats["failed"] += 1
        return {
            "phone_number": phone_number,
            "reputation": "Error",
            "user_reports": "",
            "total_calls": "",
            "last_call": "",
            "scraped_at": datetime.now().isoformat(),
        }

async def process_batch(session, batch, semaphore, rate_limiter):
    """Process a batch of phone numbers concurrently"""
    tasks = []
    for number in batch:
        task = fetch_single(session, number, semaphore, rate_limiter)
        tasks.append(task)
    
    # Run all tasks concurrently
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions
    valid_results = []
    for result in batch_results:
        if isinstance(result, Exception):
            stats["failed"] += 1
            continue
        valid_results.append(result)
    
    return valid_results

async def main_async():
    """Main async function"""
    setup_logging()
    
    # Read numbers
    phone_numbers = read_numbers()
    if not phone_numbers:
        print("No valid phone numbers to process. Exiting.")
        return
    
    print(f"\n⚡ Starting async scraper with {CONFIG['concurrent_requests']} concurrent requests")
    print(f"   Rate limit: {CONFIG['requests_per_second']} requests/second")
    print(f"   Batch size: {CONFIG['batch_size']} records\n")
    
    # Create session with connection pool
    connector = aiohttp.TCPConnector(
        limit=CONFIG["connection_limit"],
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,  # Cache DNS for 5 minutes
    )
    
    # Create rate limiter and semaphore
    rate_limiter = RateLimiter(CONFIG["requests_per_second"])
    semaphore = asyncio.Semaphore(CONFIG["concurrent_requests"])
    
    async with aiohttp.ClientSession(
        connector=connector,
        headers=CONFIG["headers"],
        timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"])
    ) as session:
        
        # Process in batches
        total_batches = (len(phone_numbers) - 1) // CONFIG["batch_size"] + 1
        
        for i in range(0, len(phone_numbers), CONFIG["batch_size"]):
            batch = phone_numbers[i:i + CONFIG["batch_size"]]
            batch_num = i // CONFIG["batch_size"] + 1
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches}")
            print(f"   Numbers: {i+1} to {min(i+CONFIG['batch_size'], len(phone_numbers))}")
            
            # Process this batch
            batch_results = await process_batch(session, batch, semaphore, rate_limiter)
            
            # Add to buffer
            results_buffer.extend(batch_results)
            stats["total"] += len(batch_results)
            
            # Save batch if buffer is full
            if len(results_buffer) >= CONFIG["batch_size"]:
                save_batch()
            
            # Show progress
            elapsed = time.time() - stats["start_time"]
            rate = stats["total"] / elapsed if elapsed > 0 else 0
            
            print(f"   Progress: {stats['total']}/{len(phone_numbers)} ({stats['total']/len(phone_numbers)*100:.1f}%)")
            print(f"   Speed: {rate*60:.1f} records/minute")
            
            # Small delay between batches
            if i + CONFIG["batch_size"] < len(phone_numbers):
                await asyncio.sleep(random.uniform(0.5, 1.5))
    
    # Save any remaining results
    if results_buffer:
        save_batch()

def show_final_stats():
    """Display final statistics"""
    elapsed = time.time() - stats["start_time"]
    
    print(f"\n{'='*60}")
    print("🏁 SCRAPING COMPLETE")
    print(f"{'='*60}")
    print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"Valid numbers found: {len(results_buffer)}")
    print(f"Successful scrapes: {stats['success']}")
    print(f"Failed scrapes: {stats['failed']}")
    print(f"Rate limited: {stats['rate_limited']}")
    print(f"Skipped invalid: {stats['skipped_invalid']}")
    
    if stats['total'] > 0:
        success_rate = (stats['success'] / stats['total']) * 100
        print(f"Success rate: {success_rate:.1f}%")
        
        records_per_second = stats['total'] / elapsed
        print(f"Speed: {records_per_second:.1f}/sec ({records_per_second*60:.0f}/min)")
        
        # Compare with sequential version
        sequential_time = stats['total'] * 1.5  # Assume 1.5s per request sequential
        speedup = sequential_time / elapsed if elapsed > 0 else 1
        print(f"Speedup vs sequential: {speedup:.1f}x faster")
    
    print(f"Output file: {CONFIG['output_file']}")
    print(f"{'='*60}")

def main():
    """Main entry point"""
    try:
        # Run async scraper
        asyncio.run(main_async())
        
        # Show final stats
        show_final_stats()
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
        # Save any pending results
        if results_buffer:
            save_batch()
    except Exception as e:
        print(f"\n💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()