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
    """Read phone numbers from CSV file - handles both with/without headers"""
    numbers = []
    try:
        with open(CONFIG["input_file"], 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
            if not lines:
                print("✗ File is empty")
                return []
            
            # Check if first line looks like a phone number or header
            first_line = lines[0].strip()
            
            # Simple heuristic: if first line has ONLY digits (or digits with +) it's a phone number
            # Remove any commas, quotes, or whitespace first
            first_cell = first_line.split(',')[0].strip().strip('"').strip("'")
            
            # Check if it's a valid phone number (digits only, possibly with + at start)
            has_header = False
            
            # Check multiple conditions to detect header
            if any(char.isalpha() for char in first_cell):  # Contains letters
                has_header = True
                print(f"✓ Detected header: {first_line[:50]}...")
            elif len(first_cell) < 7:  # Too short for phone number
                has_header = True
                print(f"✓ First cell too short, assuming header: {first_cell}")
            elif not any(char.isdigit() for char in first_cell):  # No digits at all
                has_header = True
                print(f"✓ No digits in first cell, assuming header: {first_cell}")
            else:
                # Could be a phone number (digits, possibly with +)
                cleaned = clean_number(first_cell)
                if len(cleaned) < 10:  # Not a valid 10-digit phone
                    has_header = True
                    print(f"✓ Invalid phone format, assuming header: {first_cell}")
                else:
                    print(f"✓ No header detected, first row is phone number: {first_cell}")
            
            # Process all lines
            start_index = 1 if has_header else 0
            
            for i in range(start_index, len(lines)):
                line = lines[i].strip()
                if not line:
                    continue
                
                # Handle CSV format - get first column
                parts = line.split(',')
                if parts:
                    raw_number = parts[0].strip().strip('"').strip("'")
                    cleaned = clean_number(raw_number)
                    
                    if len(cleaned) == 10:  # Valid 10-digit US number
                        numbers.append(cleaned)
                    elif cleaned:  # Has digits but not 10
                        print(f"  Skipping invalid length: {raw_number} -> {cleaned} ({len(cleaned)} digits)")
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


# XPaths for data extraction
XPATHS = {
    "reputation": '//div[@id="userReputation"]/h3/text()',
    "user_reports": '//div[@id="userReports"]/h3/text()',
    "total_calls": '//div[@id="totalCall"]/h3/text()',
    "last_call": '//div[@id="lastCall"]/h3/text()',
}


def parse_html_fast(html_content, phone_number):
    """Ultra-fast HTML parsing using pre-defined XPaths"""
    try:
        tree = html.fromstring(html_content)
        
        # Build the data dictionary by executing XPaths
        data = {"phone_number": phone_number}
        
        for key, path in XPATHS.items():
            result = tree.xpath(path)
            # result is a list; we take the first element and strip it
            data[key] = result[0].strip() if result else "Not Found" if key == "reputation" else ""
        
        data["scraped_at"] = datetime.now().isoformat()
        return data
        
    except Exception as e:
        # Fallback for structural failures
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
    """Fetch data for a single phone number with better error handling"""
    async with semaphore:
        await rate_limiter.acquire()
        
        url = f"https://lookup.robokiller.com/search?q={phone_number}"
        headers = get_random_headers()
        
        last_error = None
        
        for attempt in range(CONFIG["max_retries"] + 1):
            try:
                async with session.get(
                    url, 
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"])
                ) as response:
                    
                    # Immediately check for common blocking responses
                    if response.status == 403:  # Forbidden
                        print(f"🔒 {phone_number}: Blocked (403 Forbidden)")
                        stats["failed"] += 1
                        return {
                            "phone_number": phone_number,
                            "reputation": "Blocked (403)",
                            "user_reports": "",
                            "total_calls": "",
                            "last_call": "",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    
                    elif response.status == 404:  # Not Found
                        print(f"🔍 {phone_number}: Page not found (404)")
                        stats["failed"] += 1
                        return {
                            "phone_number": phone_number,
                            "reputation": "Page Not Found",
                            "user_reports": "",
                            "total_calls": "",
                            "last_call": "",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    
                    elif response.status == 429:  # Rate limited
                        stats["rate_limited"] += 1
                        wait_time = 2 ** (attempt + 1) + random.uniform(0, 1)
                        print(f"⚠ {phone_number}: Rate limited, waiting {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                        continue  # Retry
                    
                    elif response.status == 200:
                        html_content = await response.text()
                        
                        # Quick check for CAPTCHA or blocking page
                        if "captcha" in html_content.lower() or "access denied" in html_content.lower():
                            print(f"🚫 {phone_number}: CAPTCHA or blocked page detected")
                            stats["failed"] += 1
                            return {
                                "phone_number": phone_number,
                                "reputation": "Blocked (CAPTCHA)",
                                "user_reports": "",
                                "total_calls": "",
                                "last_call": "",
                                "scraped_at": datetime.now().isoformat(),
                            }
                        
                        data = parse_html_fast(html_content, phone_number)
                        
                        if data["reputation"] not in ["Parse Error", "Not Found"]:
                            stats["success"] += 1
                            print(f"✓ {phone_number}: {data['reputation']}")
                        else:
                            print(f"✓ {phone_number}: No data")
                        
                        return data
                    
                    else:
                        print(f"✗ {phone_number}: HTTP {response.status}")
                        last_error = f"HTTP {response.status}"
                
            except asyncio.TimeoutError:
                last_error = "Timeout"
                print(f"⏱️ {phone_number}: Timeout (attempt {attempt + 1})")
            except aiohttp.ClientConnectorError:
                last_error = "Connection Error"
                print(f"🔌 {phone_number}: Connection failed (attempt {attempt + 1})")
            except Exception as e:
                last_error = str(e)
                print(f"⚠ {phone_number}: {type(e).__name__} (attempt {attempt + 1})")
            
            # Wait before retry (if not last attempt)
            if attempt < CONFIG["max_retries"]:
                wait_time = random.uniform(1, 3) * (attempt + 1)
                await asyncio.sleep(wait_time)
        
        # All retries failed
        stats["failed"] += 1
        print(f"❌ {phone_number}: Failed after {CONFIG['max_retries'] + 1} attempts: {last_error}")
        
        return {
            "phone_number": phone_number,
            "reputation": f"Error: {last_error[:30]}" if last_error else "Error",
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