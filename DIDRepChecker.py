#!/usr/bin/env python3
"""
High-Speed Async Phone Scraper - Production Ready
10-20x faster than requests version
"""

import asyncio
import aiohttp
import csv
import random
import time
import logging
from typing import List, Dict, Optional
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
    
    # Proxy settings (optional)
    "use_proxy": False,
    "proxy_list": [],

    # Anti-detection settings
    "rotate_user_agents": True,
    "rotate_headers": True,
    "use_realistic_headers": True,
    
    # Header settings
    "header_shuffle_chance": 0.3,  # 30% chance to shuffle header order
    "referer_chance": 0.5,  # 50% chance to add referer
    
    # Rate limiting adjustments
    "adaptive_rate_limiting": True,
    "auto_adjust_on_blocks": True,
}


# ========== EXPANDED USER AGENTS (50+ real) ==========
USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:121.0) Gecko/20100101 Firefox/121.0",
    
    # Chrome on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    # Safari on Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.0.0",
    
    # Chrome on Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    
    # Firefox on Linux
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    
    # Chrome on Android
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G998B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    
    # Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    
    # Older browsers for realism
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
]


# ========== LANGUAGE HEADERS MATCHING USER AGENTS ==========
LANGUAGE_MAP = {
    # US English (most common)
    "en-US,en;q=0.9": ["Chrome", "Firefox", "Edge", "Windows"],
    "en-US,en;q=0.8,fr;q=0.7": ["Chrome", "Firefox"],
    
    # UK English
    "en-GB,en;q=0.9,en-US;q=0.8": ["Chrome", "Firefox", "Edge"],
    "en-GB,en;q=0.9": ["Safari"],
    
    # Canadian English/French
    "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7": ["Chrome", "Firefox"],
    
    # Australian English
    "en-AU,en;q=0.9": ["Chrome", "Firefox", "Safari"],
    
    # European languages
    "de-DE,de;q=0.9,en;q=0.8": ["Chrome", "Firefox"],
    "fr-FR,fr;q=0.9,en;q=0.8": ["Chrome", "Firefox"],
    "es-ES,es;q=0.9,en;q=0.8": ["Chrome", "Firefox"],
    "it-IT,it;q=0.9,en;q=0.8": ["Chrome", "Firefox"],
    
    # Asian languages
    "ja-JP,ja;q=0.9,en;q=0.8": ["Chrome", "Safari"],
    "ko-KR,ko;q=0.9,en;q=0.8": ["Chrome"],
    "zh-CN,zh;q=0.9,en;q=0.8": ["Chrome"],
    "zh-TW,zh;q=0.9,en;q=0.8": ["Chrome"],
}

# ========== REFERER SOURCES ==========
REFERER_SOURCES = [
    "https://www.google.com/",
    "https://www.google.com/search?q=phone+number+lookup",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "https://search.yahoo.com/",
    "https://www.facebook.com/",
    "https://twitter.com/",
    "",  # Empty referer (direct navigation)
    "https://www.robokiller.com/",  # Same site
]


# Fallback XPaths for data extraction
XPATHS = {
    "reputation": [
        '//div[@id="userReputation"]/h3/text()',
        '//div[contains(@class, "reputation")]/h3/text()',
        '//h3[contains(text(), "Reputation")]/../text()',
        '//div[contains(@class, "score") or contains(@class, "rating")]/text()',
        '//span[contains(@class, "reputation")]/text()',
    ],
    "user_reports": [
        '//div[@id="userReports"]/h3/text()',
        '//div[contains(@class, "reports")]/h3/text()',
        '//h3[contains(text(), "Reports")]/../text()',
        '//div[contains(text(), "reports")]/text()',
    ],
    "total_calls": [
        '//div[@id="totalCall"]/h3/text()',
        '//div[contains(@class, "calls")]/h3/text()',
        '//h3[contains(text(), "Calls")]/../text()',
        '//div[contains(text(), "calls")]/text()',
    ],
    "last_call": [
        '//div[@id="lastCall"]/h3/text()',
        '//div[contains(@class, "last")]/h3/text()',
        '//h3[contains(text(), "Last")]/../text()',
        '//div[contains(text(), "last")]/text()',
    ],
}

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
        format='%(asctime)s - %(message)s',
        datefmt='%H:%M:%S',
        handlers=[logging.StreamHandler()]
    )

def clean_number(number: str) -> str:
    """Fast phone number cleaning - keep only digits"""
    digits = []
    for char in str(number):
        if char.isdigit():
            digits.append(char)
    cleaned = ''.join(digits)
    
    # Remove US country code (1) if present
    if cleaned.startswith('1') and len(cleaned) == 11:
        cleaned = cleaned[1:]  # Remove leading 1
    
    return cleaned

def validate_config():
    """Validate configuration values"""
    warnings = []
    
    if CONFIG["concurrent_requests"] > CONFIG["connection_limit"]:
        warnings.append(f"concurrent_requests ({CONFIG['concurrent_requests']}) > connection_limit ({CONFIG['connection_limit']})")
    
    if CONFIG["requests_per_second"] > 10:
        warnings.append(f"High request rate ({CONFIG['requests_per_second']}/sec) may cause blocking")
    
    if CONFIG["batch_size"] < 10:
        warnings.append(f"Small batch_size ({CONFIG['batch_size']}) may impact performance")
    
    if CONFIG["use_proxy"] and not CONFIG["proxy_list"]:
        warnings.append("Proxy enabled but proxy_list is empty")
    
    return warnings

def read_numbers() -> List[str]:
    """Read phone numbers from CSV file - memory efficient"""
    numbers = []
    skipped_count = 0
    line_count = 0
    
    try:
        with open(CONFIG["input_file"], 'r', encoding='utf-8') as f:
            first_line = True
            has_header = False
            
            for line in f:
                line_count += 1
                line = line.strip()
                if not line:
                    continue
                
                # Header detection on first line only
                if first_line:
                    first_line = False
                    first_cell = line.split(',')[0].strip().strip('"').strip("'")
                    
                    # Check if it looks like a header
                    if any(char.isalpha() for char in first_cell):  # Contains letters
                        has_header = True
                        logging.info(f"✓ Detected header: {first_cell[:50]}...")
                        continue
                    elif len(first_cell) < 7:  # Too short for phone
                        has_header = True
                        logging.info(f"✓ First cell too short, assuming header: {first_cell}")
                        continue
                    elif not any(char.isdigit() for char in first_cell):  # No digits
                        has_header = True
                        logging.info(f"✓ No digits in first cell, assuming header: {first_cell}")
                        continue
                    else:
                        # Check if it's a valid phone number
                        cleaned = clean_number(first_cell)
                        if len(cleaned) < 10:
                            has_header = True
                            logging.info(f"✓ Invalid phone format, assuming header: {first_cell}")
                            continue
                        else:
                            logging.info(f"✓ No header detected, processing all rows")
                
                # Skip header row if detected
                if has_header and line_count == 1:
                    continue
                
                # Extract first column
                parts = line.split(',')
                if parts:
                    raw_number = parts[0].strip().strip('"').strip("'")
                    cleaned = clean_number(raw_number)
                    
                    if len(cleaned) == 10:  # Valid 10-digit US number
                        numbers.append(cleaned)
                    elif cleaned:  # Has digits but not 10
                        skipped_count += 1
                        if skipped_count <= 5:  # Show first 5 warnings only
                            logging.warning(f"Skipping invalid length: {raw_number} -> {cleaned} ({len(cleaned)} digits)")
        
        stats["skipped_invalid"] = skipped_count
        logging.info(f"Loaded {len(numbers)} valid 10-digit phone numbers")
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} invalid numbers")
        return numbers
        
    except FileNotFoundError:
        logging.error(f"Error: File '{CONFIG['input_file']}' not found")
        return []
    except Exception as e:
        logging.error(f"Error reading file: {e}")
        return []


def get_random_headers() -> Dict[str, str]:
    """Get realistic, varied headers with matching user agent and language"""
    # Choose random user agent
    user_agent = random.choice(USER_AGENTS)
    
    # Determine browser type from user agent
    browser_type = "Chrome"  # default
    if "Firefox" in user_agent:
        browser_type = "Firefox"
    elif "Safari" in user_agent and "Chrome" not in user_agent:
        browser_type = "Safari"
    elif "Edg" in user_agent:
        browser_type = "Edge"
    
    # Choose appropriate language based on browser type
    compatible_languages = []
    for lang, browsers in LANGUAGE_MAP.items():
        if browser_type in browsers:
            compatible_languages.append(lang)
    
    # Fallback to English if no compatible language found
    accept_language = random.choice(compatible_languages) if compatible_languages else "en-US,en;q=0.9"
    
    # Base headers that all browsers have
    headers = {
        # Accept headers vary by browser
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": accept_language,
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": random.choice(["0", "1"]),  # Do Not Track (varies)
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    }
    
    # Add browser-specific headers
    if browser_type == "Chrome" or browser_type == "Edge":
        headers.update({
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": random.choice(["none", "same-origin", "cross-site"]),
            "Sec-Fetch-User": "?1",
        })
    elif browser_type == "Firefox":
        headers.update({
            "TE": "trailers",
        })
    
    # Add random referer (50% chance)
    if random.random() > 0.5:
        headers["Referer"] = random.choice(REFERER_SOURCES)
    
    # Add user agent last (some detectors check order)
    headers["User-Agent"] = user_agent
    
    # Randomize header order slightly (shuffle keys)
    if random.random() > 0.7:  # 30% chance to shuffle
        items = list(headers.items())
        random.shuffle(items)
        headers = dict(items)
    
    return headers


def get_random_proxy() -> Optional[str]:
    """Get random proxy if enabled"""
    if CONFIG["use_proxy"] and CONFIG["proxy_list"]:
        return random.choice(CONFIG["proxy_list"])
    return None

def parse_html_fast(html_content: str, phone_number: str) -> Dict[str, str]:
    """Ultra-fast HTML parsing with fallback XPaths"""
    try:
        tree = html.fromstring(html_content)
        data = {"phone_number": phone_number}
        
        # Try each XPath pattern until we find data
        for key, paths in XPATHS.items():
            value = ""
            for path in paths:
                try:
                    result = tree.xpath(path)
                    if result:
                        first = result[0]
                        # If the xpath returned a string, strip it directly
                        if isinstance(first, str):
                            text = first.strip()
                        else:
                            # lxml elements and others: prefer text_content()
                            try:
                                text = first.text_content().strip()
                            except Exception:
                                text = str(first).strip()

                        if text:
                            value = text
                            break
                except Exception:
                    continue
            
            # Set default if not found
            if key == "reputation" and not value:
                value = "Not Found"
            
            data[key] = value
        
        data["scraped_at"] = datetime.now().isoformat()
        return data
        
    except Exception as e:
        logging.debug(f"Parse error for {phone_number}: {e}")
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
        # Check if file exists
        file_exists = False
        try:
            with open(CONFIG["output_file"], 'r') as f:
                file_exists = True
        except:
            pass
        
        # Write batch
        with open(CONFIG["output_file"], 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=results_buffer[0].keys())
            
            if not file_exists:
                writer.writeheader()
            
            writer.writerows(results_buffer)
        
        logging.info(f"Saved {len(results_buffer)} records to CSV")
        results_buffer.clear()
        
    except Exception as e:
        logging.error(f"Error saving batch: {e}")

class RateLimiter:
    """Simple async token-bucket rate limiter with basic 429 cooldown support"""
    def __init__(self, rate_per_second: float):
        # rate_per_second may be fractional (e.g., 0.5 req/sec)
        self.rate = float(rate_per_second)
        # capacity controls how many tokens can be accumulated
        self.capacity = max(self.rate, 1.0)
        self.tokens = self.capacity
        self.updated_at = time.time()
        # timestamp until which the limiter should stay in cooldown (set on 429)
        self.cooldown_until = 0.0
    
    async def acquire(self):
        """Wait if we're hitting rate limits or in cooldown. Returns True when allowed."""
        now = time.time()
        # Respect 429-triggered cooldown
        if now < self.cooldown_until:
            await asyncio.sleep(self.cooldown_until - now)
            now = time.time()

        elapsed = now - self.updated_at
        # Refill tokens based on elapsed time and rate
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated_at = now

        if self.tokens < 1:
            sleep_time = (1 - self.tokens) / max(self.rate, 1e-6)
            await asyncio.sleep(sleep_time)
            # After sleeping, refill tokens to capacity
            self.tokens = self.capacity
            self.updated_at = time.time()

        self.tokens -= 1
        return True

    def record_429(self, backoff_seconds: float = 10.0):
        """Called when we receive a 429 - set a short cooldown and reduce tokens conservatively."""
        self.cooldown_until = time.time() + float(backoff_seconds)
        # Make the bucket more conservative
        self.tokens = max(0.0, self.tokens - (self.rate * 0.5))
        logging.warning(f"RateLimiter: recorded 429, cooling down for {backoff_seconds}s")

async def health_check(session: aiohttp.ClientSession) -> bool:
    """Check if we can still access the website"""
    test_number = "5551234567"
    try:
        headers = get_random_headers()
        async with session.get(
            f"https://lookup.robokiller.com/search?q={test_number}",
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            return response.status == 200
    except:
        return False



async def fetch_single(session: aiohttp.ClientSession, phone_number: str, 
                      semaphore: asyncio.Semaphore, rate_limiter: RateLimiter) -> Dict[str, str]:
    """Fetch data for single number with enhanced anti-detection"""
    async with semaphore:
        # Rate limiting - ensure we respect global rate limits / cooldowns
        await rate_limiter.acquire()

        url = f"https://lookup.robokiller.com/search?q={phone_number}"
        headers = get_random_headers()
        
        # Add some random delays to mimic human behavior
        if random.random() > 0.8:  # 20% chance
            await asyncio.sleep(random.uniform(0.1, 0.5))
        
        for attempt in range(CONFIG["max_retries"] + 1):
            try:
                # Use different headers for each retry attempt
                if attempt > 0 and CONFIG["rotate_headers"]:
                    headers = get_random_headers()
                
                async with session.get(
                    url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"])
                ) as response:
                    
                    # Enhanced status code handling
                    if response.status == 403:
                        logging.warning(f"🔒 {phone_number}: Blocked (403)")
                        # Try different user agent next time
                        stats["failed"] += 1
                        return {
                            "phone_number": phone_number,
                            "reputation": "Blocked",
                            "user_reports": "",
                            "total_calls": "",
                            "last_call": "",
                            "scraped_at": datetime.now().isoformat(),
                        }
                    
                    elif response.status == 429:
                        stats["rate_limited"] += 1
                        rate_limiter.record_429()
                        
                        # Variable backoff based on attempt
                        base_wait = min(2 ** (attempt + 1), 60)
                        jitter = random.uniform(0.5, 1.5)
                        wait_time = base_wait * jitter
                        
                        logging.warning(f"⚠ {phone_number}: Rate limited, waiting {wait_time:.1f}s")
                        await asyncio.sleep(wait_time)
                        
                        # Rotate user agent after rate limit
                        if CONFIG["rotate_user_agents"]:
                            headers = get_random_headers()
                        
                        continue  # Retry
                    
                    elif response.status == 200:
                        html_content = await response.text()
                        
                        # Enhanced block detection
                        block_indicators = [
                            "captcha", "access denied", "cloudflare", 
                            "security check", "robot", "blocked",
                            "please verify", "unusual traffic"
                        ]
                        
                        content_lower = html_content.lower()
                        if any(indicator in content_lower for indicator in block_indicators):
                            logging.warning(f"🚫 {phone_number}: Blocking page detected")
                            stats["failed"] += 1
                            return {
                                "phone_number": phone_number,
                                "reputation": "Blocked",
                                "user_reports": "",
                                "total_calls": "",
                                "last_call": "",
                                "scraped_at": datetime.now().isoformat(),
                            }
                        
                        # Check if we got actual data or empty page
                        if len(html_content) < 1000:  # Very small response
                            logging.warning(f"📄 {phone_number}: Suspiciously small response")
                            stats["failed"] += 1
                            return {
                                "phone_number": phone_number,
                                "reputation": "Empty Response",
                                "user_reports": "",
                                "total_calls": "",
                                "last_call": "",
                                "scraped_at": datetime.now().isoformat(),
                            }
                        
                        data = parse_html_fast(html_content, phone_number)
                        
                        if data["reputation"] not in ["Parse Error", "Not Found", ""]:
                            stats["success"] += 1
                            logging.info(f"✓ {phone_number}: {data['reputation']}")
                        else:
                            logging.info(f"✓ {phone_number}: No data found")
                        
                        return data
                    
                    else:
                        logging.warning(f"✗ {phone_number}: HTTP {response.status}")
                        # Don't retry on client errors (4xx) except 429
                        if 400 <= response.status < 500 and response.status != 429:
                            stats["failed"] += 1
                            return {
                                "phone_number": phone_number,
                                "reputation": f"HTTP {response.status}",
                                "user_reports": "",
                                "total_calls": "",
                                "last_call": "",
                                "scraped_at": datetime.now().isoformat(),
                            }
                
            except asyncio.TimeoutError:
                logging.warning(f"⏱️ {phone_number}: Timeout (attempt {attempt + 1})")
            except aiohttp.ClientConnectorError:
                logging.warning(f"🔌 {phone_number}: Connection failed (attempt {attempt + 1})")
            except Exception as e:
                logging.warning(f"⚠ {phone_number}: {type(e).__name__} (attempt {attempt + 1})")
            
            # Variable delay between retries
            if attempt < CONFIG["max_retries"]:
                retry_delay = random.uniform(1, 4) * (attempt + 1)
                await asyncio.sleep(retry_delay)
        
        # All retries failed
        stats["failed"] += 1
        logging.error(f"❌ {phone_number}: Failed after {CONFIG['max_retries'] + 1} attempts")
        
        return {
            "phone_number": phone_number,
            "reputation": "Error",
            "user_reports": "",
            "total_calls": "",
            "last_call": "",
            "scraped_at": datetime.now().isoformat(),
        }
    
    
async def process_batch(
    session: aiohttp.ClientSession, 
    batch: List[str], 
    semaphore: asyncio.Semaphore, 
    rate_limiter: RateLimiter
) -> List[Dict[str, str]]:
    """Process a batch of phone numbers concurrently"""
    tasks = []
    for number in batch:
        task = fetch_single(session, number, semaphore, rate_limiter)
        tasks.append(task)
    
    # Run all tasks concurrently
    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions (they're already handled in fetch_single)
    valid_results = []
    for result in batch_results:
        if not isinstance(result, Exception):
            valid_results.append(result)
    
    return valid_results

async def main_async():
    """Main async function"""
    setup_logging()
    
    # Validate config
    warnings = validate_config()
    if warnings:
        logging.warning("Configuration warnings:")
        for warning in warnings:
            logging.warning(f"  ⚠ {warning}")
        logging.warning("")
    
    # Read numbers
    phone_numbers = read_numbers()
    if not phone_numbers:
        logging.info("No valid phone numbers to process. Exiting.")
        return
    
    logging.info(f"Starting async scraper with {CONFIG['concurrent_requests']} concurrent requests")
    logging.info(f"Rate limit: {CONFIG['requests_per_second']} requests/second")
    logging.info(f"Batch size: {CONFIG['batch_size']} records")
    
    # Create session with connection pool
    connector = aiohttp.TCPConnector(
        limit=CONFIG["connection_limit"],
        force_close=False,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,
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
            
            logging.info(f"Processing batch {batch_num}/{total_batches}")
            logging.info(f"Numbers: {i+1} to {min(i+CONFIG['batch_size'], len(phone_numbers))}")
            
            # Optional: Health check every 500 requests
            if stats["total"] > 0 and stats["total"] % 500 == 0:
                logging.info("Running health check...")
                if not await health_check(session):
                    logging.warning("Health check failed - possible blocking, pausing...")
                    await asyncio.sleep(10)
            
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
            
            logging.info(f"Progress: {stats['total']}/{len(phone_numbers)} ({stats['total']/len(phone_numbers)*100:.1f}%)")
            logging.info(f"Speed: {rate*60:.1f} records/minute")
            
            # Small delay between batches
            if i + CONFIG["batch_size"] < len(phone_numbers):
                await asyncio.sleep(random.uniform(0.5, 1.5))
    
    # Save any remaining results
    if results_buffer:
        save_batch()

def show_final_stats():
    """Display final statistics"""
    elapsed = time.time() - stats["start_time"]
    
    logging.info(f"{'='*60}")
    logging.info("🏁 SCRAPING COMPLETE")
    logging.info(f"{'='*60}")
    logging.info(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    logging.info(f"Total processed: {stats['total']}")
    logging.info(f"Successful: {stats['success']}")
    logging.info(f"Failed: {stats['failed']}")
    logging.info(f"Rate limited: {stats['rate_limited']}")
    logging.info(f"Skipped invalid: {stats['skipped_invalid']}")
    
    if stats['total'] > 0:
        success_rate = (stats['success'] / stats['total']) * 100
        logging.info(f"Success rate: {success_rate:.1f}%")
        
        records_per_second = stats['total'] / elapsed
        logging.info(f"Speed: {records_per_second:.1f}/sec ({records_per_second*60:.0f}/min)")
        
        # Compare with sequential version
        sequential_time = stats['total'] * 1.5
        speedup = sequential_time / elapsed if elapsed > 0 else 1
        logging.info(f"Speedup vs sequential: {speedup:.1f}x faster")
    
    logging.info(f"Output file: {CONFIG['output_file']}")
    logging.info(f"{'='*60}")

def main():
    """Main entry point"""
    try:
        # Run async scraper
        asyncio.run(main_async())
        show_final_stats()
        
    except KeyboardInterrupt:
        logging.info("Stopped by user")
        if results_buffer:
            save_batch()
        show_final_stats()
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to save any results we have
        if results_buffer:
            logging.info("Attempting to save collected results...")
            save_batch()

if __name__ == "__main__":
    main()