import requests
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta, timezone

# === CONFIGURATION ===
BASE_URL = "https://biocatch.atlassian.net/wiki"
SPACE_KEYS = ["INT", "ONE"]  # add as many as needed
EMAIL = "michal.reuveni@biocatch.com"
#API_TOKEN

# === API SETUP ===
AUTH = HTTPBasicAuth(EMAIL, API_TOKEN)
HEADERS = {
    "Accept": "application/json"
}

# === GET PAGES IN SPACE ===
def get_all_pages(space_key: str):
    """Return a list of page objects for the given space."""
    url = f"{BASE_URL}/rest/api/content"
    pages = []
    start = 0

    while True:
        params = {
            "spaceKey": space_key,
            "limit": 100,
            "start": start,
            "type": "page",
            "expand": "version"
        }
        res = requests.get(url, headers=HEADERS, params=params, auth=AUTH)
        res.raise_for_status()
        data = res.json()
        pages.extend(data["results"])

        # pagination
        if data.get("_links", {}).get("next"):
            start += 100
        else:
            break
    return pages

# === GET SPACE CREATOR INFO ===
def get_space_creator_status(space_key: str):
    url = f"{BASE_URL}/rest/api/space/{space_key}?expand=metadata.properties.creator"
    res = requests.get(url, headers=HEADERS, auth=AUTH)
    res.raise_for_status()
    data = res.json()
    creator_info = data.get("metadata", {}).get("properties", {}).get("creator", {}).get("value")

    if creator_info:
        display_name = creator_info.get("displayName", "Unknown")
        is_active = creator_info.get("accountStatus", "unknown") != "deactivated"
        return display_name, "Active" if is_active else "Deactivated"
    return "Unknown", "Unknown"

# === MAIN ===
if __name__ == "__main__":
    report_rows = []
    now = datetime.now(timezone.utc)

    # Define time cut‑offs
    cutoff_3m = now - timedelta(days=90)
    cutoff_6m = now - timedelta(days=180)
    cutoff_12m = now - timedelta(days=365)

    for space_key in SPACE_KEYS:
        print(f"Processing space: {space_key}")
        pages = get_all_pages(space_key)
        total_pages = len(pages)

        updated_3m = 0
        updated_6m = 0
        updated_12m = 0

        for page in pages:
            last_updated_str = page["version"].get("when")
            if not last_updated_str:
                continue  # skip if no update timestamp

            last_updated = datetime.fromisoformat(last_updated_str.replace("Z", "+00:00"))

            if last_updated >= cutoff_3m:
                updated_3m += 1
                updated_6m += 1
                updated_12m += 1
            elif last_updated >= cutoff_6m:
                updated_6m += 1
                updated_12m += 1
            elif last_updated >= cutoff_12m:
                updated_12m += 1

        # Get creator info
        creator_name, creator_status = get_space_creator_status(space_key)

        # Calculate percentages safely
        def pct(count: int) -> float:
            return round((count / total_pages) * 100, 2) if total_pages else 0.0

        report_rows.append({
            "Space Key": space_key,
            "Creator": creator_name,
            "Creator Status": creator_status,
            "Total Pages": total_pages,
            "% Pages Updated < 3M": pct(updated_3m),
            "% Pages Updated < 6M": pct(updated_6m),
            "% Pages Updated < 12M": pct(updated_12m)
        })

    df = pd.DataFrame(report_rows)
    df.to_excel("space_update_percentages_report_creator.xlsx", index=False)
    print("✅ Export complete: space_update_percentages_report_creator.xlsx")
