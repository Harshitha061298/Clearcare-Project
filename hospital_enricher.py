# hospital_enricher_v3.py
import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
import yaml
from difflib import get_close_matches

with open("utils/config.yaml", "r") as f:
    config = yaml.safe_load(f)

CITY_STATES = [(c['name'], c['state']) for c in config['cities']]
OUTPUT_FILE = config['output_file']
SLEEP_SECONDS = config.get('sleep_between_requests', 1)
CMS_API_URL = config['cms']['hospital_info_api']

from loguru import logger
os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)
logger.add("logs/enrichment.log", rotation="1 MB")

# Load credentials from .env
load_dotenv(dotenv_path="utils/.env")
SERP_API_KEY = os.getenv("SERP_API_KEY")
LEAPFROG_API_KEY = os.getenv("LEAPFROG_API_KEY")

# Leapfrog API Base
LEAPFROG_CONFIG = config['leapfrog']
BASE_API_URL = LEAPFROG_CONFIG['base_api_url']
BASE_HOSPITAL_URL = LEAPFROG_CONFIG['base_hospital_url']

# Words to remove from campus_id
GENERIC_WORDS = ["hospital", "medical", "center", "campus", "health", "system", "of", "corporation", "general", "university", "s", "regional","INC"]

def clean_text(text):
    return ' '.join(text.strip().split())

def generate_campus_id(name):
    tokens = re.sub(r"[\.\,\'\-&]", "", name.lower()).split()
    filtered = [word for word in tokens if word not in GENERIC_WORDS]
    return '_'.join(filtered)

def extract_zip_code(address):
    match = re.search(r"(\d{5})(?:-\d{4})?$", address)
    return match.group(1) if match else ""

def fetch_cms_data(limit=50000):
    url = CMS_API_URL
    payload = {"query": {"limit": limit}}
    response = requests.post(url, json=payload)
    response.raise_for_status()

    json_data = response.json()
    if "results" not in json_data or not json_data["results"]:
        logger.warning(f"CMS API failed: Unexpected structure. Keys: {list(json_data.keys())}")
        raise ValueError("CMS API response missing 'results'")

    records = json_data["results"]
    columns = list(records[0].keys())
    df = pd.DataFrame(records, columns=columns)
    df.to_csv("data/cached_cms_data.csv", index=False)
    return df

def normalize(text):
    return re.sub(r"[^\w]", "", str(text).lower().strip())

def scrape_hospitals_for_city(city, state):
    logger.info(f"Scraping hospitals for {city}, {state}")
    params = {
        "apiKey": LEAPFROG_API_KEY,
        "f.cityState": f"{city},{state}",
        "f.radius": 40
    }
    headers = {"Accept": "application/json"}
    response = requests.get(BASE_API_URL, headers=headers, params=params)
    hospitals = []

    if response.status_code == 200:
        data = response.json()
        html_content = data['response']['html']
        soup = BeautifulSoup(html_content, 'lxml')
        for item in soup.select(".itemWrapper"):
            name = clean_text(item.select_one(".name a").get_text())
            slug = item.select_one(".name a")['href']
            leapfrog_url = urljoin(BASE_HOSPITAL_URL, slug)
            address = clean_text(item.select_one(".address").get_text(" ", strip=True))
            grade_img = item.select_one(".grade img")
            leapfrog_grade = grade_img['alt'].replace("Grade ", "") if grade_img else "N/A"
            leapfrog_grade_term = clean_text(item.select_one(".date").get_text())

            zip_code = extract_zip_code(address)
            campus_id = generate_campus_id(name)

            hospitals.append({
                "hospital_name": name,
                "campus_id": campus_id,
                "healthcare_system": name.split()[0],
                "city": "",
                "metro_area": city,
                "state": state,
                "hospital_address": address,
                "zip_code": zip_code,
                "latitude": "",
                "longitude": "",
                "leapfrog_grade": leapfrog_grade,
                "leapfrog_grade_term": leapfrog_grade_term,
                "leapfrog_grade_url": leapfrog_url
            })
            time.sleep(SLEEP_SECONDS)

    return hospitals

def main():
    all_hospitals = []

    for city, state in CITY_STATES:
        hospitals = scrape_hospitals_for_city(city, state)
        all_hospitals.extend(hospitals)

    df = pd.DataFrame(all_hospitals)

    # Fetch CMS dataset from API (with local cache fallback)
    try:
        cms_df = fetch_cms_data()
    except Exception as e:
        logger.warning(f"CMS API failed: {e}")
        cache_path = "data/cached_cms_data.csv"
        if os.path.exists(cache_path):
            logger.info("Loading CMS data from cached CSV...")
            cms_df = pd.read_csv(cache_path)
        else:
            logger.critical("No CMS data available (API failed and no cache found). Exiting.")
            raise

    unmatched = []

    try:
        cms_df["campus_id"] = cms_df["facility_name"].apply(generate_campus_id)
        cms_df["zip"] = cms_df["zip_code"].str.extract(r"(\d{5})")
        cms_lookup = cms_df.set_index("campus_id")
        cms_keys = list(cms_lookup.index)

        for idx, row in df.iterrows():
            campus_id = row["campus_id"]
            if campus_id in cms_lookup.index:
                match = cms_lookup.loc[campus_id]
            else:
                close_matches = get_close_matches(campus_id, cms_keys, n=1, cutoff=0.9)
                if close_matches:
                    match = cms_lookup.loc[close_matches[0]]
                    logger.info(f"Fuzzy matched '{row['hospital_name']}' to CMS: '{match['facility_name']}'")
                else:
                    unmatched.append(row["hospital_name"])
                    continue

            df.at[idx, "hospital_type"] = match.get("hospital_type", "")
            df.at[idx, "city"] = match.get("citytown", "")
            df.at[idx, "county"] = match.get("countyparish", "")
            df.at[idx, "telephone_num"] = match.get("telephone_number", "")
            df.at[idx, "cms_rating"] = match.get("hospital_overall_rating", "")
    except Exception as e:
        logger.error(f"CMS enrichment failed: {e}")

    if unmatched:
        logger.warning(f"{len(unmatched)} hospitals unmatched by CMS data:")
        for name in unmatched:
            logger.warning(f" - {name}")

    # Define full structure
    full_columns = [
        "hospital_name", "campus_id", "healthcare_system", "hospital_type", "city", "county", "metro_area", "state",
        "hospital_address", "zip_code", "telephone_num", "latitude", "longitude", "cms_rating", "leapfrog_grade", "leapfrog_grade_term", "leapfrog_grade_url",
        "last_updated_on", "version", "etl_status", "processed_by", "last_processed_on", "issues_encountered", "transparency_score",
        "raw_filename", "file_format", "structure", "download_url", "contact_num", "email_id"
    ]

    for col in full_columns:
        if col not in df.columns:
            df[col] = ""

    df = df[full_columns]
    df.to_excel(OUTPUT_FILE, index=False)
    logger.success(f"Saved {len(df)} hospitals to '{OUTPUT_FILE}'")

if __name__ == "__main__":
    main()
else:
    logger.error(f"Failed to fetch hospitals for {city}, {state}. Status code: {response.status_code}")
