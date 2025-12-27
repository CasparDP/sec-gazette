"""Test direct access to SEC digest PDFs using known URL pattern."""

import httpx
from datetime import datetime, timedelta

headers = {
    "User-Agent": "SEC Digest Research Project caspar@example.com",
    "Accept": "application/pdf,*/*",
}

# Test pattern: /news/digest/YYYY/digMMDDYY.pdf
# Example from instructions: https://www.sec.gov/news/digest/1984/dig092884.pdf

# Try a few dates from 1985
test_dates = [
    "1985-01-02",  # First business day of year
    "1985-01-15",
    "1985-06-15",
    "1985-12-31",
]

print("Testing direct PDF access with known URL pattern...")
print("Pattern: /news/digest/YYYY/digMMDDYY.pdf\n")

for date_str in test_dates:
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    # Format: digMMDDYY.pdf (e.g., dig092884.pdf for Sept 28, 1984)
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    year_2digit = dt.strftime("%y")
    year_4digit = dt.strftime("%Y")

    filename = f"dig{month}{day}{year_2digit}.pdf"
    url = f"https://www.sec.gov/news/digest/{year_4digit}/{filename}"

    try:
        response = httpx.head(url, headers=headers, timeout=10, follow_redirects=True)
        status = response.status_code
        content_type = response.headers.get('content-type', 'unknown')

        print(f"Date: {date_str}")
        print(f"URL: {url}")
        print(f"Status: {status}")
        print(f"Content-Type: {content_type}")

        if status == 200:
            content_length = response.headers.get('content-length', 'unknown')
            print(f"Size: {content_length} bytes")
            print("✓ PDF EXISTS!")
        elif status == 404:
            print("✗ Not found (might be weekend/holiday)")
        else:
            print(f"? Unexpected status")

        print()

    except Exception as e:
        print(f"Date: {date_str}")
        print(f"URL: {url}")
        print(f"Error: {e}\n")

# Try alternative archive URL
print("\n" + "=" * 80)
print("Trying year-specific archive pages...")
print("=" * 80)

for year in [1984, 1985, 1986]:
    url = f"https://www.sec.gov/news/digest/{year}/"
    try:
        response = httpx.get(url, headers=headers, timeout=10)
        print(f"\nYear {year}: Status {response.status_code}")
        if response.status_code == 200:
            print(f"Content length: {len(response.text)} chars")
            print(f"First 500 chars:\n{response.text[:500]}")
    except Exception as e:
        print(f"Year {year}: Error - {e}")
