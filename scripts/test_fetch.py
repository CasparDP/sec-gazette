"""Quick test to fetch SEC archive page with proper headers."""

import httpx

# SEC requires User-Agent header
headers = {
    "User-Agent": "SEC Digest Research Project caspar@example.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Test robots.txt first
print("Fetching robots.txt...")
try:
    response = httpx.get("https://www.sec.gov/robots.txt", headers=headers, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Content (first 500 chars):\n{response.text[:500]}\n")
except Exception as e:
    print(f"Error: {e}\n")

# Test archive index page
print("\nFetching archive index...")
try:
    response = httpx.get("https://www.sec.gov/news/digest/digarchives/", headers=headers, timeout=30)
    print(f"Status: {response.status_code}")
    print(f"Content length: {len(response.text)} chars")
    print(f"First 1000 chars:\n{response.text[:1000]}")
except Exception as e:
    print(f"Error: {e}")
