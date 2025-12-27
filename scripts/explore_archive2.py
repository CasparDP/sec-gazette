"""Explore SEC archive structure - show all links."""

import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin

headers = {
    "User-Agent": "SEC Digest Research Project caspar@example.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

print("Fetching archive index page...\n")
response = httpx.get("https://www.sec.gov/news/digest/digarchives/", headers=headers, timeout=30)

soup = BeautifulSoup(response.text, 'html.parser')

# Get all text content first to understand structure
print("=" * 80)
print("Page text content (first 2000 chars):")
print("=" * 80)
print(soup.get_text()[:2000])

print("\n" + "=" * 80)
print("All links on the page:")
print("=" * 80)

all_links = soup.find_all('a')
print(f"Total links: {len(all_links)}\n")

for i, link in enumerate(all_links[:30], 1):  # Show first 30 links
    href = link.get('href', '')
    text = link.get_text(strip=True)
    full_url = urljoin("https://www.sec.gov", href) if href else ''

    print(f"{i}. Text: '{text}'")
    print(f"   Href: {href}")
    if full_url != href:
        print(f"   Full: {full_url}")
    print()
