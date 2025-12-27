"""Explore SEC archive structure to understand URL patterns."""

import httpx
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

headers = {
    "User-Agent": "SEC Digest Research Project caspar@example.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

print("Fetching archive index page...\n")
response = httpx.get("https://www.sec.gov/news/digest/digarchives/", headers=headers, timeout=30)
print(f"Status: {response.status_code}")

soup = BeautifulSoup(response.text, 'html.parser')

# Find all links to PDFs
pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))

print(f"\nTotal PDF links found: {len(pdf_links)}\n")

# Analyze structure for 1985
print("=" * 80)
print("PDF links for 1985:")
print("=" * 80)

links_1985 = []
for link in pdf_links:
    href = link.get('href')
    text = link.get_text(strip=True)

    if '1985' in href or '1985' in text:
        full_url = urljoin("https://www.sec.gov", href)
        links_1985.append({
            'url': full_url,
            'href': href,
            'text': text
        })

# Show first 10 links from 1985
for i, link_info in enumerate(links_1985[:10], 1):
    print(f"{i}. Text: {link_info['text']}")
    print(f"   URL: {link_info['url']}")
    print()

print(f"\nTotal 1985 links: {len(links_1985)}")

# Analyze URL pattern
if links_1985:
    print("\n" + "=" * 80)
    print("URL Pattern Analysis:")
    print("=" * 80)
    sample_url = links_1985[0]['url']
    print(f"Sample URL: {sample_url}")

    # Extract pattern
    match = re.search(r'/news/digest/(\d{4})/dig(\d{6})\.pdf', sample_url)
    if match:
        year = match.group(1)
        date_code = match.group(2)
        print(f"Pattern: /news/digest/YYYY/digMMDDYY.pdf")
        print(f"  Year folder: {year}")
        print(f"  Date code: {date_code} (likely MMDDYY format)")
        print(f"  Example: {date_code[0:2]}/{date_code[2:4]}/{date_code[4:6]}")

# Check if there are year-specific index pages
print("\n" + "=" * 80)
print("Looking for year-specific index pages:")
print("=" * 80)

year_links = soup.find_all('a', href=re.compile(r'/\d{4}/$|/\d{4}$'))
print(f"Found {len(year_links)} potential year index pages")

for link in year_links[:5]:
    print(f"  - {link.get_text(strip=True)}: {link.get('href')}")
