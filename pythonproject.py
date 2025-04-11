import requests
import pandas as pd
import time
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = '83b8c42dcdd155fcd42ba4511b664c34'  # Replace with your actual API key
BASE_URL = 'https://api.themoviedb.org/3'
START_YEAR = 2024
END_YEAR = 2025
MAX_PAGES = 50  # You can increase this if needed

session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retries)
session.mount('http://', adapter)
session.mount('https://', adapter)

def discover_movies(year, page):
    url = f"{BASE_URL}/discover/movie"
    params = {
        'api_key': API_KEY,
        'primary_release_year': year,
        'sort_by': 'popularity.desc',
        'page': page
    }
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get('results', [])
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error getting page {page} for {year}: {e}")
        return []

def fetch_movie_details(movie_id):
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {'api_key': API_KEY, 'append_to_response': 'keywords'}
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Detail fetch failed for ID {movie_id}: {e}")
        return {}

def fetch_movie_credits(movie_id):
    url = f"{BASE_URL}/movie/{movie_id}/credits"
    params = {'api_key': API_KEY}
    try:
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Credits fetch failed for ID {movie_id}: {e}")
        return {}

# Loop over each year
for year in range(START_YEAR, END_YEAR + 1):
    movie_ids = []
    print(f"\n📆 Fetching movie IDs for {year}...")
    for page in range(1, MAX_PAGES + 1):
        movies = discover_movies(year, page)
        if not movies:
            break
        for movie in movies:
            movie_ids.append((movie['id'], movie['title']))
        time.sleep(0.25)

    print(f"📦 Found {len(movie_ids)} movies for {year}")

    # Fetch details and credits
    metadata = []
    credits_list = []

    print(f"⬇️ Fetching metadata and credits for {year} movies...")
    for movie_id, title in tqdm(movie_ids, desc=f"Processing {year}"):
        detail = fetch_movie_details(movie_id)
        credit = fetch_movie_credits(movie_id)

        if detail:
            metadata.append({
                "budget": detail.get("budget"),
                "genres": detail.get("genres"),
                "homepage": detail.get("homepage"),
                "id": detail.get("id"),
                "keywords": detail.get("keywords", {}).get("keywords", []),
                "original_language": detail.get("original_language"),
                "original_title": detail.get("original_title"),
                "overview": detail.get("overview"),
                "popularity": detail.get("popularity"),
                "production_companies": detail.get("production_companies"),
                "production_countries": detail.get("production_countries"),
                "release_date": detail.get("release_date"),
                "revenue": detail.get("revenue"),
                "runtime": detail.get("runtime"),
                "spoken_languages": detail.get("spoken_languages"),
                "status": detail.get("status"),
                "tagline": detail.get("tagline"),
                "title": detail.get("title"),
                "vote_average": detail.get("vote_average"),
                "vote_count": detail.get("vote_count")
            })

        if credit:
            credits_list.append({
                "movie_id": movie_id,
                "title": title,
                "cast": credit.get("cast", []),
                "crew": credit.get("crew", [])
            })

        time.sleep(0.25)

    # Save to CSV
    pd.DataFrame(metadata).to_csv(f"movies_metadata_{year}.csv", index=False)
    pd.DataFrame(credits_list).to_csv(f"movies_credits_{year}.csv", index=False)
    print(f"✅ Year {year} saved successfully!")

print("\n🎉 All years processed and saved!")