import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from pandas import ExcelWriter

from Michal.Jira import API_TOKEN

# === CONFIGURATION ===
BASE_URL = "https://biocatch.atlassian.net/wiki"
SPACE_KEYS = ["INT", "ONE"]  # add as many as needed
EMAIL = "michal.reuveni@biocatch.com"
API_TOKEN =
# === API SETUP ===
AUTH = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {
    "Accept": "application/json"
}

# === GET PAGES IN SPACE ===
def get_all_pages(space_key):
    url = f"{BASE_URL}/rest/api/content"
    pages = []
    start = 0

    while True:
        params = {
            "spaceKey": space_key,
            "limit": 100,
            "start": start,
            "type": "page",
            "expand": "version,history"
        }
        res = requests.get(url, headers=HEADERS, params=params, auth=AUTH)
        res.raise_for_status()
        data = res.json()
        pages.extend(data["results"])

        if "_links" in data and "next" in data["_links"]:
            start += 100
        else:
            break
    return pages

# === FORMAT ANALYTICS ===
def extract_page_data(pages, space_key):
    rows = []
    for page in pages:
        row = {
            "Title": page.get("title"),
            "Page ID": page.get("id"),
            "Last Updated": page["version"].get("when"),
            "Created By": page["history"]["createdBy"]["displayName"],
            "Created At": page["history"].get("createdDate"),
            "URL": f"{BASE_URL}/spaces/{space_key}/pages/{page['id']}"

        }
        rows.append(row)
    return pd.DataFrame(rows)

# === MAIN ===
if __name__ == "__main__":
    with ExcelWriter("confluence_pages_analytics.xlsx", engine="openpyxl") as writer:
        for key in SPACE_KEYS:
            print(f"Processing space: {key}")
            pages = get_all_pages(key)
            df = extract_page_data(pages, key)
            df.to_excel(writer, sheet_name=key[:31], index=False)  # sheet name max 31 chars
            print(f"Added {len(df)} pages from {key} to Excel sheet")
    print("✅ Export complete: confluence_pages_analytics.xlsx")
