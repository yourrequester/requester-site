"""
REQUESTER - Phase 4 — Multi-Source Community Intelligence
==========================================================
Sources:
  ✅ Reddit          — public .json endpoints, no key needed
  ✅ Apple App Store — iTunes RSS feed, no key needed
  ✅ Google Play     — google-play-scraper, no key needed
  ✅ Trustpilot      — public HTML scraping, no key needed
  ✅ Steam           — public JSON API, no key needed
  ✅ YouTube         — YouTube Data API v3 (free 10k units/day, needs key)
  ✅ Google Trends   — pytrends (unofficial, free, no key)

Usage:
    python requester_phase1.py

Requirements:
    pip install requests anthropic python-dotenv google-play-scraper beautifulsoup4 pytrends

YouTube (optional, free):
    Add YOUTUBE_API_KEY to .env — get one free at console.cloud.google.com
    Enable "YouTube Data API v3" on the project (no billing required for free tier)
"""

import requests
import json
import time
import re
import os
from datetime import datetime, timezone
from collections import defaultdict
from dotenv import load_dotenv
import anthropic
import requester_db

load_dotenv()

# ── Config loader ─────────────────────────────────────────────────────────────
# Edit requester_config.json to change targets, or use the dashboard Settings panel.
# If no config file found, falls back to the defaults below.

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH  = os.path.join(_SCRIPT_DIR, "requester_config.json")

_DEFAULT_CONFIG = {
    # ── Reddit ────────────────────────────────────────────────
    "subreddits":            ["netflix", "cordcutters"],
    "sort_modes":            ["hot", "top", "new", "controversial"],
    "post_limit":            40,
    "max_comments":          15,
    "max_posts_for_ai":      40,
    # ── App Store ─────────────────────────────────────────────
    "appstore_apps":         [{"name": "Netflix", "app_id": "363590051"}],
    "appstore_pages":        5,
    "appstore_min_rating":   3,
    # ── Google Play ───────────────────────────────────────────
    "googleplay_apps":       [{"name": "Netflix", "app_id": "com.netflix.mediaclient"}],
    "googleplay_count":      200,
    "googleplay_max_rating": 3,
    # ── Trustpilot ────────────────────────────────────────────
    "trustpilot_companies":  [{"name": "Netflix", "slug": "netflix.com"}],
    "trustpilot_pages":      3,
    "trustpilot_max_rating": 3,
    # ── Steam ─────────────────────────────────────────────────
    "steam_apps":            [],  # e.g. [{"name": "Cyberpunk 2077", "app_id": "1091500"}]
    "steam_count":           150,
    # ── YouTube (free key from console.cloud.google.com) ──────
    "youtube_searches":      ["Netflix feature request", "Netflix bug"],
    "youtube_max_results":   100,
    # ── Google Trends (no key needed) ─────────────────────────
    "google_trends_boost":   True,
    "target_name":           "Netflix",
}

def _load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                cfg = json.load(f)
            # Fill in any new keys from defaults without overwriting user's values
            for k, v in _DEFAULT_CONFIG.items():
                cfg.setdefault(k, v)
            print(f"  ✓ Loaded config from requester_config.json")
            return cfg
        except Exception as e:
            print(f"  ⚠ Config file error ({e}), using defaults")
    return dict(_DEFAULT_CONFIG)

_cfg = _load_config()

SUBREDDITS            = [s.lower().strip() for s in _cfg.get("subreddits", [])]
SORT_MODES            = _cfg.get("sort_modes", ["hot","top","new","controversial"])
POST_LIMIT            = int(_cfg.get("post_limit", 40))
MAX_COMMENTS          = int(_cfg.get("max_comments", 15))
MAX_POSTS_FOR_AI      = int(_cfg.get("max_posts_for_ai", 40))
APP_STORE_APPS        = _cfg.get("appstore_apps", [])
APP_STORE_PAGES       = int(_cfg.get("appstore_pages", 5))
APP_STORE_MIN_RATING  = int(_cfg.get("appstore_min_rating", 3))
GOOGLE_PLAY_APPS      = _cfg.get("googleplay_apps", [])
GOOGLE_PLAY_COUNT     = int(_cfg.get("googleplay_count", 200))
GOOGLE_PLAY_MAX_RATING= int(_cfg.get("googleplay_max_rating", 3))
TRUSTPILOT_COMPANIES  = _cfg.get("trustpilot_companies", [])
TRUSTPILOT_PAGES      = int(_cfg.get("trustpilot_pages", 3))
TRUSTPILOT_MAX_RATING = int(_cfg.get("trustpilot_max_rating", 3))
STEAM_APPS            = _cfg.get("steam_apps", [])
STEAM_COUNT           = int(_cfg.get("steam_count", 150))
YOUTUBE_SEARCHES      = _cfg.get("youtube_searches", [])
YOUTUBE_MAX_RESULTS   = int(_cfg.get("youtube_max_results", 100))
GOOGLE_TRENDS_BOOST   = bool(_cfg.get("google_trends_boost", True))
TARGET_NAME           = _cfg.get("target_name", "")

COMMENT_DELAY = 1.2
APP_STORE_MIN_RATING  = 3
APP_STORE_DELAY       = 1.0
GOOGLE_PLAY_MAX_RATING = 3


# ── Reddit Fetching ───────────────────────────────────────────────────────────

INTENT_KEYWORDS = [
    "should", "wish", "want", "needs to", "need to", "please add",
    "bring back", "fix", "why won't", "why doesn't", "why can't",
    "would be nice", "feature request", "suggestion", "hope they",
    "they should", "devs should", "add ", "remove ", "revert",
    "annoying", "frustrated", "broken", "missing", "where is",
    "used to", "we need", "give us", "let us", "tired of",
    "hate that", "love if", "please fix", "still no", "why is there no",
    "needs a", "deserves", "petition", "upvote if", "complaint",
    "issue", "problem", "bug", "glitch", "not working", "keeps",
    "always", "never", "impossible", "why do", "why does", "how come",
    "can't believe", "seriously", "disappointed", "bring back",
    "used to have", "used to be able", "just let", "allow us"
]

HEADERS = {
    "User-Agent": "Requester-PoC/1.0 (personal research project; non-commercial)"
}


def fetch_posts(subreddit, sort, limit):
    url = f"https://www.reddit.com/r/{subreddit}/{sort}.json?limit={limit}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        posts = resp.json()["data"]["children"]
        print(f"  ✓ {len(posts)} posts from r/{subreddit}/{sort}")
        return [p["data"] for p in posts]
    except Exception as e:
        print(f"  ✗ Failed {sort}: {e}")
        return []


def fetch_post_with_comments(subreddit, post_id):
    url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit={MAX_COMMENTS}&depth=1&sort=top"
    try:
        time.sleep(COMMENT_DELAY)
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        post_data = data[0]["data"]["children"][0]["data"]
        comments = []
        if len(data) > 1:
            for child in data[1]["data"]["children"]:
                if child["kind"] == "t1":
                    body = child["data"].get("body", "").strip()
                    score = child["data"].get("score", 0)
                    if body and body not in ("[deleted]", "[removed]") and len(body) > 20:
                        comments.append({"text": body[:400], "score": score})
        comments.sort(key=lambda x: x["score"], reverse=True)
        post_data["top_comments"] = comments[:MAX_COMMENTS]
        return post_data
    except Exception as e:
        return None


def contains_intent(text):
    text_lower = text.lower()
    return any(kw in text_lower for kw in INTENT_KEYWORDS)


def recency_factor(created_utc):
    now = datetime.now(timezone.utc).timestamp()
    age_days = (now - created_utc) / 86400
    return max(0.05, 1.0 - (age_days / 30))



# ── App Store Fetcher ─────────────────────────────────────────────────────────

def fetch_appstore_reviews(app_id, app_name, pages=5, max_rating=3):
    """
    Fetch App Store reviews using Apple's public iTunes RSS feed.
    No API key needed. Returns list of review dicts shaped like Reddit posts
    so they flow through the same AI extraction pipeline.
    """
    reviews = []
    print(f"  📱 Fetching App Store reviews for {app_name} (id={app_id})...")

    for page in range(1, pages + 1):
        url = f"https://itunes.apple.com/us/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json"
        try:
            time.sleep(APP_STORE_DELAY)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            entries = data.get("feed", {}).get("entry", [])

            # First entry is app metadata, not a review — skip it
            for entry in entries:
                if "im:rating" not in entry:
                    continue
                rating = int(entry["im:rating"]["label"])
                if rating > max_rating:
                    continue

                review_id  = entry.get("id", {}).get("label", f"as_{page}_{len(reviews)}")
                title      = entry.get("title", {}).get("label", "")
                body       = entry.get("content", {}).get("label", "")
                version    = entry.get("im:version", {}).get("label", "")
                updated    = entry.get("updated", {}).get("label", "")

                # Parse date for recency scoring
                try:
                    from datetime import datetime, timezone
                    dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                    created_utc = dt.timestamp()
                except:
                    created_utc = time.time()

                if not body or len(body) < 20:
                    continue

                # Shape to match Reddit post format for unified pipeline
                reviews.append({
                    "id":           f"as_{review_id}",
                    "title":        title,
                    "selftext":     body,
                    "score":        (4 - rating) * 10,  # invert: 1-star = 30pts, 3-star = 10pts
                    "created_utc":  created_utc,
                    "permalink":    f"/app-store/{app_id}",
                    "_source":      "appstore",
                    "_subreddit":   "appstore",
                    "_app_name":    app_name,
                    "_rating":      rating,
                    "top_comments": [],  # no comments on App Store reviews
                })

            if not entries or len(entries) < 2:
                break  # no more pages

        except Exception as e:
            print(f"    ✗ Page {page} failed: {e}")
            break

    print(f"    ✓ {len(reviews)} low-rated reviews fetched")
    return reviews

# ── Google Play Fetcher ───────────────────────────────────────────────────────

def fetch_google_play_reviews(app_id, app_name, count=200, max_rating=3):
    """
    Fetch Google Play reviews using the google-play-scraper package.
    No API key needed. pip install google-play-scraper
    Returns list of review dicts shaped like Reddit posts for the unified pipeline.
    """
    try:
        from google_play_scraper import reviews, Sort
    except ImportError:
        print(f"  ⚠  google-play-scraper not installed. Run: pip install google-play-scraper")
        return []

    print(f"  🤖 Fetching Google Play reviews for {app_name} ({app_id})...")
    result = []

    try:
        # Fetch most-relevant low-rated reviews in one call
        raw, _ = reviews(
            app_id,
            lang="en",
            country="us",
            sort=Sort.MOST_RELEVANT,
            count=count,
            filter_score_with=None,  # get all stars, filter below
        )

        for r in raw:
            rating = r.get("score", 5)
            if rating > max_rating:
                continue

            body = (r.get("content") or "").strip()
            if not body or len(body) < 20:
                continue

            review_id = r.get("reviewId", f"gp_{len(result)}")
            at = r.get("at")
            created_utc = at.timestamp() if at else time.time()

            result.append({
                "id":          f"gp_{review_id}",
                "title":       body[:60] + "…" if len(body) > 60 else body,
                "selftext":    body,
                "score":       (4 - rating) * 10,   # invert: 1★ = 30pts, 3★ = 10pts
                "created_utc": created_utc,
                "permalink":   f"https://play.google.com/store/apps/details?id={app_id}",
                "_source":     "googleplay",
                "_subreddit":  "googleplay",
                "_app_name":   app_name,
                "_rating":     rating,
                "top_comments": [],
            })

        print(f"    ✓ {len(result)} low-rated reviews fetched (from {len(raw)} total)")

    except Exception as e:
        print(f"    ✗ Google Play fetch failed: {e}")

    return result


# ── Trustpilot Fetcher ────────────────────────────────────────────────────────

def fetch_trustpilot_reviews(company_slug, company_name, pages=3, max_rating=3):
    """
    Scrapes Trustpilot public review pages. No API key needed.
    company_slug: the slug in trustpilot.com/review/[slug] e.g. 'netflix.com'
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  ⚠  beautifulsoup4 not installed. Run: pip install beautifulsoup4")
        return []

    print(f"  ⭐ Fetching Trustpilot reviews for {company_name}...")
    results = []
    scrape_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    for page in range(1, pages + 1):
        url = (f"https://www.trustpilot.com/review/{company_slug}"
               f"?page={page}&stars=1&stars=2&stars=3")
        try:
            time.sleep(1.5)
            resp = requests.get(url, headers=scrape_headers, timeout=15)
            if resp.status_code == 404:
                print(f"    ✗ No Trustpilot page found for '{company_slug}'")
                break
            if resp.status_code != 200:
                print(f"    ✗ HTTP {resp.status_code} on page {page}, stopping")
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Trustpilot puts all data in a __NEXT_DATA__ JSON blob
            next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
            if next_data_tag:
                try:
                    page_data = json.loads(next_data_tag.string)
                    # Path varies by Trustpilot version — try both
                    reviews_raw = (
                        page_data.get("props",{}).get("pageProps",{}).get("reviews", []) or
                        page_data.get("props",{}).get("pageProps",{}).get("initialState",{})
                                  .get("reviewsList",{}).get("reviews", [])
                    )
                    for r in reviews_raw:
                        if isinstance(r.get("rating"), dict):
                            rating = r["rating"].get("stars", 3)
                        else:
                            rating = r.get("rating", 3)
                        if rating > max_rating:
                            continue
                        title    = r.get("title", "") or ""
                        body     = r.get("text",  "") or r.get("body", "") or ""
                        if not body or len(body) < 20:
                            continue
                        rid      = r.get("id", f"tp_{page}_{len(results)}")
                        date_str = (r.get("dates", {}) or {}).get("publishedDate", "")
                        results.append(_make_review(
                            f"tp_{rid}", title, body, rating, company_name, "trustpilot",
                            f"https://www.trustpilot.com/review/{company_slug}",
                            created_utc=_parse_iso_date(date_str)
                        ))
                    continue
                except Exception:
                    pass  # fall through to HTML parse

            # HTML fallback
            cards = soup.select('[data-service-review-card-paper]')
            if not cards:
                break
            for card in cards:
                rating_el = card.select_one('[data-service-review-rating]')
                rating = int(rating_el["data-service-review-rating"]) if rating_el else 3
                if rating > max_rating:
                    continue
                title_el = card.select_one('[data-service-review-title-typography]')
                body_el  = card.select_one('[data-service-review-text-typography]')
                body = body_el.get_text(strip=True) if body_el else ""
                if not body or len(body) < 20:
                    continue
                title = title_el.get_text(strip=True) if title_el else ""
                rid = f"tp_{company_slug}_{page}_{len(results)}"
                results.append(_make_review(
                    rid, title, body, rating, company_name, "trustpilot",
                    f"https://www.trustpilot.com/review/{company_slug}"
                ))

        except Exception as e:
            print(f"    ✗ Page {page} failed: {e}")
            break

    print(f"    ✓ {len(results)} Trustpilot reviews fetched")
    return results


# ── Steam Reviews Fetcher ─────────────────────────────────────────────────────

def fetch_steam_reviews(app_id, app_name, count=150):
    """
    Fetch Steam reviews via Steam's public JSON API. No key needed.
    app_id: Steam numeric app ID (e.g. 1091500 for Cyberpunk 2077)
    Only fetches negative reviews containing intent signals.
    """
    print(f"  🎮 Fetching Steam reviews for {app_name} (appid={app_id})...")
    results = []
    cursor  = "*"
    fetched = 0

    while fetched < count:
        batch = min(100, count - fetched)
        url = (f"https://store.steampowered.com/appreviews/{app_id}"
               f"?json=1&filter=recent&language=english&review_type=negative"
               f"&num_per_page={batch}&cursor={requests.utils.quote(str(cursor))}")
        try:
            time.sleep(1.0)
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if not data.get("success"):
                break

            reviews_raw = data.get("reviews", [])
            if not reviews_raw:
                break

            for r in reviews_raw:
                body = (r.get("review") or "").strip()
                if not body or len(body) < 20 or not contains_intent(body):
                    continue
                if r.get("voted_up", False):  # skip positive reviews
                    continue

                rid = r.get("recommendationid", f"st_{app_id}_{len(results)}")
                votes_up = r.get("votes_up", 0)
                results.append({
                    "id":          f"st_{rid}",
                    "title":       body[:60] + "…" if len(body) > 60 else body,
                    "selftext":    body,
                    "score":       max(5, votes_up),
                    "created_utc": r.get("timestamp_created", time.time()),
                    "permalink":   f"https://store.steampowered.com/app/{app_id}/#app_reviews_hash",
                    "_source":     "steam",
                    "_subreddit":  "steam",
                    "_app_name":   app_name,
                    "_rating":     1,
                    "top_comments": [],
                })

            cursor  = data.get("cursor", "")
            fetched += len(reviews_raw)
            if not cursor or len(reviews_raw) < batch:
                break

        except Exception as e:
            print(f"    ✗ Steam fetch error: {e}")
            break

    print(f"    ✓ {len(results)} Steam reviews with intent signals")
    return results


# ── YouTube Comments Fetcher ──────────────────────────────────────────────────

def fetch_youtube_comments(search_query, max_results=100, api_key=None):
    """
    Fetch YouTube comments using the free YouTube Data API v3.
    Free quota: 10,000 units/day — this function uses ~100-300 units per call.

    Get a free key at console.cloud.google.com:
      1. Create project → Enable "YouTube Data API v3"
      2. Credentials → Create API Key (no billing required)
      3. Add YOUTUBE_API_KEY=your_key to .env
    """
    if not api_key:
        return []

    print(f"  📺 Fetching YouTube comments: '{search_query}'...")
    results = []

    try:
        # Search for relevant videos
        resp = requests.get(
            "https://www.googleapis.com/youtube/v3/search",
            params={"part":"snippet","q":search_query,"type":"video",
                    "maxResults":10,"relevanceLanguage":"en","key":api_key},
            timeout=15
        )
        resp.raise_for_status()
        video_items = resp.json().get("items", [])
        video_ids   = [v["id"]["videoId"] for v in video_items if v.get("id",{}).get("videoId")]

        if not video_ids:
            print("    ✗ No videos found")
            return []

        print(f"    Found {len(video_ids)} videos, scanning comments...")
        per_video = max(15, max_results // max(len(video_ids), 1))

        for vid_id in video_ids[:8]:
            if len(results) >= max_results:
                break
            try:
                time.sleep(0.5)
                r = requests.get(
                    "https://www.googleapis.com/youtube/v3/commentThreads",
                    params={"part":"snippet","videoId":vid_id,"maxResults":min(per_video,100),
                            "order":"relevance","textFormat":"plainText","key":api_key},
                    timeout=15
                )
                if r.status_code == 403:
                    continue  # comments disabled
                r.raise_for_status()
                for item in r.json().get("items", []):
                    snip  = item.get("snippet",{}).get("topLevelComment",{}).get("snippet",{})
                    text  = snip.get("textDisplay","").strip()
                    likes = snip.get("likeCount", 0)
                    if not text or len(text) < 20 or not contains_intent(text):
                        continue
                    cid = item.get("id", f"yt_{vid_id}_{len(results)}")
                    vid_title = next((v["snippet"]["title"] for v in video_items
                                      if v["id"]["videoId"] == vid_id), search_query)
                    results.append({
                        "id":           f"yt_{cid}",
                        "title":        text[:80] + "…" if len(text) > 80 else text,
                        "selftext":     text,
                        "score":        max(1, likes),
                        "created_utc":  _parse_iso_date(snip.get("publishedAt","")),
                        "permalink":    f"https://www.youtube.com/watch?v={vid_id}",
                        "_source":      "youtube",
                        "_subreddit":   "youtube",
                        "_app_name":    search_query.split()[0],
                        "_rating":      None,
                        "_video_title": vid_title[:100],
                        "top_comments": [],
                    })
            except Exception:
                continue

    except Exception as e:
        print(f"    ✗ YouTube fetch failed: {e}")

    print(f"    ✓ {len(results)} YouTube comments with intent signals")
    return results


# ── Google Trends Gravity Booster ─────────────────────────────────────────────

def fetch_google_trends_boost(demands, target_name):
    """
    Boosts gravity scores for demands trending on Google Search.
    Uses pytrends (unofficial, free, no key needed).
    Returns dict: {action_slug -> multiplier}  (1.0 = no boost, up to 2.5x)
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        print("  ⚠  pytrends not installed — skipping Google Trends boost.")
        print("     Run: pip install pytrends")
        return {}

    print(f"\n📈 Checking Google Trends for signal boost...")
    boosts = {}

    try:
        pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25))
        for i in range(0, min(len(demands), 50), 5):
            batch = demands[i:i+5]
            terms = []
            for d in batch:
                words = (d.get("action","")).split()[:5]
                terms.append(f"{target_name} {' '.join(words)}"[:100])
            try:
                time.sleep(2)
                pytrends.build_payload(terms, timeframe="now 7-d", geo="")
                interest = pytrends.interest_over_time()
                if interest.empty:
                    continue
                for term, demand in zip(terms, batch):
                    if term not in interest.columns:
                        continue
                    avg = interest[term].mean()
                    multiplier = round(1.0 + (avg / 100.0) * 1.5, 2)
                    slug = demand.get("slug") or _make_demand_slug(demand.get("subject",""), demand.get("action",""))
                    boosts[slug] = multiplier
                    if avg > 10:
                        print(f"    📈 {multiplier:.1f}x boost → {demand.get('action','')[:55]}")
            except Exception as e:
                print(f"    ⚠ Trends batch failed: {e}")
    except Exception as e:
        print(f"    ⚠ Google Trends unavailable: {e}")

    return boosts


# ── Shared helpers ────────────────────────────────────────────────────────────

def _make_review(rid, title, body, rating, app_name, source, permalink, created_utc=None):
    return {
        "id":          rid,
        "title":       title or body[:60],
        "selftext":    body,
        "score":       max(1, (4 - min(rating, 3)) * 10),
        "created_utc": created_utc or time.time(),
        "permalink":   permalink,
        "_source":     source,
        "_subreddit":  source,
        "_app_name":   app_name,
        "_rating":     rating,
        "top_comments": [],
    }

def _parse_iso_date(date_str):
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except:
        return time.time()


# ── AI Extraction ─────────────────────────────────────────────────────────────

def build_post_summary(post):
    comments_text = []
    for c in post.get("top_comments", []):
        comments_text.append(f"[{c['score']}↑] {c['text']}")
    source = post.get("_source", "reddit")
    is_store = source in ("appstore", "googleplay", "trustpilot", "steam")
    is_social = source in ("youtube",)
    summary = {
        "id":      post["id"],
        "title":   post.get("title", ""),
        "score":   post.get("score", 0),
        "_source": source,
    }
    if is_store:
        summary["review_text"] = post.get("selftext", "")[:400]
        summary["star_rating"] = post.get("_rating", 1)
        summary["app"]         = post.get("_app_name", "")
    elif is_social:
        summary["comment_text"] = post.get("selftext", "")[:400]
        summary["platform"]     = source
        summary["app"]          = post.get("_app_name", "")
        if post.get("_video_title"):
            summary["video_title"] = post["_video_title"]
    else:
        summary["selftext"] = post.get("selftext", "")[:300]
        summary["comments"] = comments_text[:8]
    return summary


def _build_canonical_names():
    """Build set of canonical subject names from config (app names, company names)."""
    names = set()
    for app in APP_STORE_APPS:
        names.add(app["name"])
    for app in GOOGLE_PLAY_APPS:
        names.add(app["name"])
    for co in TRUSTPILOT_COMPANIES:
        names.add(co["name"])
    for app in STEAM_APPS:
        names.add(app["name"])
    if TARGET_NAME:
        names.add(TARGET_NAME)
    return names


def _normalize_subject(subject, canonical_names):
    """Map a subject string to the nearest canonical name from config.
    Priority: exact match → canonical contained in subject → word overlap → unchanged.
    """
    if not subject:
        return subject
    subj_lower = subject.lower().strip()
    # 1. Exact match (case-insensitive)
    for name in canonical_names:
        if name.lower() == subj_lower:
            return name
    # 2. Canonical name is wholly contained in subject ("Delta" in "Delta Airlines App")
    for name in canonical_names:
        if name.lower() in subj_lower:
            return name
    # 3. Subject is wholly contained in canonical (rare, e.g. "Delta" in "Delta Air Lines")
    for name in canonical_names:
        if subj_lower in name.lower():
            return name
    # 4. Any word from the canonical name appears in the subject
    for name in canonical_names:
        name_words = set(name.lower().split())
        subj_words = set(subj_lower.split())
        if name_words & subj_words:
            return name
    return subject  # no match, keep original


def _make_demand_slug(subject, action):
    """Generate a stable, URL-safe slug from subject + action.
    Using both fields ensures demands for different companies with the same
    action text (e.g. 'Fix login bug' for Delta vs United) stay distinct.
    """
    raw = f"{subject}-{action}".lower()
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-")
    return slug[:80]


def extract_demands_with_ai(posts):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("\n⚠  No ANTHROPIC_API_KEY in .env — add it to your .env file.\n")
        return []

    client = anthropic.Anthropic(api_key=api_key)

    # ── Batch processing ───────────────────────────────────────────────────────
    # Split into batches of 60 so Claude can return complete JSON every time.
    # Results are merged and re-deduplicated by action slug afterward.
    BATCH_SIZE = 60
    batches = [posts[i:i+BATCH_SIZE] for i in range(0, len(posts), BATCH_SIZE)]
    print(f"  Splitting {len(posts)} items into {len(batches)} batches of up to {BATCH_SIZE}...")

    all_demands = []
    for batch_num, batch in enumerate(batches, 1):
        batch_demands = _extract_batch(client, batch, batch_num, len(batches))
        all_demands.extend(batch_demands)

    if not all_demands:
        print("  ⚠ No demands extracted.")
        return []

    # ── Merge duplicates across batches ───────────────────────────────────────
    # Same action slug = same demand; merge their post_ids
    merged = {}
    for d in all_demands:
        slug = _make_demand_slug(d.get("subject", ""), d.get("action") or "")
        if slug in merged:
            existing = merged[slug]
            existing["post_ids"] = list(set(existing.get("post_ids", []) + d.get("post_ids", [])))
        else:
            merged[slug] = d
    demands = list(merged.values())

    # ── Normalize subject names to canonical config names ─────────────────────
    canonical = _build_canonical_names()
    if canonical:
        for d in demands:
            d["subject"] = _normalize_subject(d.get("subject", ""), canonical)

    # ── Assign stable slugs (after normalization so subject is canonical) ─────
    for d in demands:
        d["slug"] = _make_demand_slug(d.get("subject", ""), d.get("action", ""))

    # ── Fix source_subreddit based on actual post_id prefixes ────────────────
    PREFIX_SOURCE = {"as_":"appstore","gp_":"googleplay","tp_":"trustpilot","st_":"steam","yt_":"youtube"}
    for d in demands:
        pids = d.get("post_ids", [])
        found_sources = set()
        for pid in pids:
            for prefix, src in PREFIX_SOURCE.items():
                if pid.startswith(prefix):
                    found_sources.add(src)
                    break
        if len(found_sources) == 1:
            d["source_subreddit"] = found_sources.pop()

    as_d = [d for d in demands if any(p.startswith("as_") for p in d.get("post_ids", []))]
    gp_d = [d for d in demands if any(p.startswith("gp_") for p in d.get("post_ids", []))]
    tp_d = [d for d in demands if any(p.startswith("tp_") for p in d.get("post_ids", []))]
    st_d = [d for d in demands if any(p.startswith("st_") for p in d.get("post_ids", []))]
    yt_d = [d for d in demands if any(p.startswith("yt_") for p in d.get("post_ids", []))]
    parts = [f"{len(as_d)} App Store", f"{len(gp_d)} Google Play"]
    if tp_d: parts.append(f"{len(tp_d)} Trustpilot")
    if st_d: parts.append(f"{len(st_d)} Steam")
    if yt_d: parts.append(f"{len(yt_d)} YouTube")
    print(f"  ✓ {len(demands)} total requests extracted ({', '.join(parts)})")
    return demands


def _extract_batch(client, posts, batch_num, total_batches):
    """Send one batch of posts to Claude and return a list of demand dicts."""
    summaries  = [build_post_summary(p) for p in posts]
    valid_ids  = {s["id"] for s in summaries}

    SOURCE_LABELS = {
        "appstore":   ("APP STORE", "as_"),
        "googleplay": ("GOOGLE PLAY", "gp_"),
        "trustpilot": ("TRUSTPILOT", "tp_"),
        "steam":      ("STEAM", "st_"),
        "youtube":    ("YOUTUBE COMMENT", "yt_"),
    }

    id_lines = []
    sources_present = set()
    for s in summaries:
        src = s.get("_source", "reddit")
        sources_present.add(src)
        if src in SOURCE_LABELS:
            label, _ = SOURCE_LABELS[src]
            rating = s.get("star_rating")
            rating_str = f", {rating}★" if rating else ""
            id_lines.append(f'  id="{s["id"]}" [{label} review{rating_str}, app={s.get("app","")}]')
        else:
            id_lines.append(f'  id="{s["id"]}" [Reddit post, r/{s.get("_subreddit","?")}]')

    # Build sources string
    subs_str = " and ".join(f"r/{s}" for s in SUBREDDITS) if SUBREDDITS else "multiple subreddits"
    extra_sources = []
    if "appstore"   in sources_present: extra_sources.append("Apple App Store")
    if "googleplay" in sources_present: extra_sources.append("Google Play")
    if "trustpilot" in sources_present: extra_sources.append("Trustpilot")
    if "steam"      in sources_present: extra_sources.append("Steam")
    if "youtube"    in sources_present: extra_sources.append("YouTube")
    sources_str = subs_str
    if extra_sources:
        sources_str += " and " + ", ".join(extra_sources)

    non_reddit_section = ""
    if sources_present - {"reddit"}:
        non_reddit_section = """
NON-REDDIT SOURCES:
- "review_text" or "comment_text" field contains the actual content (use this, not the title)
- IDs starting with as_=App Store, gp_=Google Play, tp_=Trustpilot, st_=Steam, yt_=YouTube
- Set source_subreddit to the platform name (appstore/googleplay/trustpilot/steam/youtube)
- All are valid demand signals — extract complaints and requests from them just like Reddit posts
"""

    # Build canonical name list for Claude to use exactly
    all_app_names = list(dict.fromkeys(
        [a["name"] for a in APP_STORE_APPS] +
        [a["name"] for a in GOOGLE_PLAY_APPS] +
        [a["name"] for a in TRUSTPILOT_COMPANIES] +
        [a["name"] for a in STEAM_APPS]
    ))
    if not all_app_names and TARGET_NAME:
        all_app_names = [TARGET_NAME]
    app_names_str = ", ".join(f'"{n}"' for n in all_app_names) if all_app_names else '"the company"'

    prompt = f"""You are analyzing {len(summaries)} posts and reviews (batch {batch_num}/{total_batches}) from {sources_str}.
{non_reddit_section}
CRITICAL: post_ids MUST be exact id values from the list below. Do not invent IDs.

Available IDs:
{chr(10).join(id_lines)}

Group similar requests together. Categories: Bug Fix | Feature Request | Content Request | UI/UX | Policy Change | Other
Skip pure venting, general discussion, jokes, or questions seeking recommendations.

Posts to analyze:
{json.dumps(summaries, indent=2)}

Return ONLY a JSON array. No markdown, no explanation.
[
  {{
    "subject": "Use EXACTLY one of these names: {app_names_str}. No variations, no appending App/Airlines/Airways.",
    "action": "One clear sentence describing what people want",
    "category": "category name",
    "summary": "2-3 sentences explaining the request and why it matters",
    "post_ids": ["exact_id_1", "exact_id_2"],
    "source_subreddit": "subreddit name, or appstore/googleplay/trustpilot/steam/youtube"
  }}
]"""

    try:
        print(f"\n🤖 Batch {batch_num}/{total_batches} → Claude ({len(summaries)} items)...")
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=8000,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = message.content[0].text.strip()
        raw = re.sub(r"^```json\s*|^```\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
        demands = json.loads(raw)

        # Validate IDs
        for d in demands:
            bad = [pid for pid in d.get("post_ids", []) if pid not in valid_ids]
            if bad:
                print(f"  ⚠ Removing invented IDs: {bad}")
                d["post_ids"] = [pid for pid in d.get("post_ids", []) if pid in valid_ids]

        as_d = sum(1 for d in demands if any(p.startswith("as_") for p in d.get("post_ids",[])))
        gp_d = sum(1 for d in demands if any(p.startswith("gp_") for p in d.get("post_ids",[])))
        print(f"  ✓ Batch {batch_num}: {len(demands)} requests ({as_d} App Store, {gp_d} Google Play)")
        return demands

    except Exception as e:
        err = str(e)
        if "credit balance" in err or "too low" in err:
            print(f"  ✗ Credits not active. Check console.anthropic.com → Plans & Billing.")
        else:
            print(f"  ✗ Batch {batch_num} failed: {e}")
        return []



def calculate_gravity(demand, post_lookup):
    ids = demand.get("post_ids", [])
    if not ids:
        return 0.0
    total_upvotes, recency_sum, comment_score, valid = 0, 0.0, 0, 0
    for pid in ids:
        if pid in post_lookup:
            p = post_lookup[pid]
            total_upvotes += p.get("score", 0)
            recency_sum   += recency_factor(p.get("created_utc", 0))
            # Count comments as engagement signal
            comment_score += len(p.get("top_comments", []))
            valid         += 1
    if valid == 0:
        return 0.0
    avg_recency    = recency_sum / valid
    unique_threads = len(set(ids))
    # Base score: every demand gets minimum points from thread count + comments + recency
    base = (unique_threads * 5) + (comment_score * 2) + (avg_recency * 10)
    # Main score: upvotes are still king but base prevents 0s
    main = total_upvotes * unique_threads * avg_recency
    return round(main + base, 1)


# ── Main Pipeline ─────────────────────────────────────────────────────────────

def run():
    subs_label = " + ".join(f"r/{s}" for s in SUBREDDITS)
    print("=" * 60)
    print("  REQUESTER v2 — Community Request Intelligence")
    print(f"  Targets: {subs_label}")
    print("=" * 60)

    all_raw_posts = []
    for sub in SUBREDDITS:
        print(f"\n📡 Ingesting posts from r/{sub}...")
        for sort in SORT_MODES:
            posts = fetch_posts(sub, sort, POST_LIMIT)
            # Tag each post with its subreddit
            for p in posts:
                p["_subreddit"] = sub
            all_raw_posts.extend(posts)

    seen_ids, unique_posts = set(), []
    for p in all_raw_posts:
        if p["id"] not in seen_ids:
            seen_ids.add(p["id"])
            unique_posts.append(p)
    print(f"\n  Total unique posts across all sources: {len(unique_posts)}")

    print(f"\n🔍 Filtering for intent signals...")
    intent_posts = [
        p for p in unique_posts
        if contains_intent(p.get("title","") + " " + p.get("selftext",""))
    ]
    print(f"  Intent-bearing posts: {len(intent_posts)} / {len(unique_posts)}")

    # Sort by score descending so the highest-upvoted posts are analyzed first
    intent_posts.sort(key=lambda p: p.get("score", 0), reverse=True)

    print(f"\n💬 Fetching comments (takes ~{min(len(intent_posts), MAX_POSTS_FOR_AI)*1.2:.0f}s)...")
    enriched = []
    for i, post in enumerate(intent_posts[:MAX_POSTS_FOR_AI]):
        sub = post.get("_subreddit", SUBREDDITS[0])
        print(f"  [{i+1}/{min(len(intent_posts), MAX_POSTS_FOR_AI)}] r/{sub}: {post['title'][:55]}...")
        full = fetch_post_with_comments(sub, post["id"])
        if full:
            full["_subreddit"] = sub
        enriched.append(full if full else post)

    post_lookup = {p["id"]: p for p in unique_posts}

    # ── App Store ingestion ───────────────────────────────────────────────────
    appstore_reviews = []
    if APP_STORE_APPS:
        print(f"\n📱 Fetching App Store reviews...")
        for app_cfg in APP_STORE_APPS:
            r_list = fetch_appstore_reviews(
                app_cfg["app_id"], app_cfg["name"],
                pages=APP_STORE_PAGES, max_rating=APP_STORE_MIN_RATING
            )
            appstore_reviews.extend(r_list)
            for r in r_list: post_lookup[r["id"]] = r
        print(f"  Total App Store reviews added: {len(appstore_reviews)}")

    # ── Google Play ingestion ─────────────────────────────────────────────────
    googleplay_reviews = []
    if GOOGLE_PLAY_APPS:
        print(f"\n▶ Fetching Google Play reviews...")
        for app_cfg in GOOGLE_PLAY_APPS:
            gp_list = fetch_google_play_reviews(
                app_cfg["app_id"], app_cfg["name"],
                count=GOOGLE_PLAY_COUNT, max_rating=GOOGLE_PLAY_MAX_RATING
            )
            googleplay_reviews.extend(gp_list)
            for r in gp_list: post_lookup[r["id"]] = r
        print(f"  Total Google Play reviews added: {len(googleplay_reviews)}")

    # ── Trustpilot ingestion ──────────────────────────────────────────────────
    trustpilot_reviews = []
    if TRUSTPILOT_COMPANIES:
        print(f"\n⭐ Fetching Trustpilot reviews...")
        for co in TRUSTPILOT_COMPANIES:
            tp_list = fetch_trustpilot_reviews(
                co["slug"], co["name"],
                pages=TRUSTPILOT_PAGES, max_rating=TRUSTPILOT_MAX_RATING
            )
            trustpilot_reviews.extend(tp_list)
            for r in tp_list: post_lookup[r["id"]] = r
        print(f"  Total Trustpilot reviews added: {len(trustpilot_reviews)}")

    # ── Steam ingestion ───────────────────────────────────────────────────────
    steam_reviews = []
    if STEAM_APPS:
        print(f"\n🎮 Fetching Steam reviews...")
        for app_cfg in STEAM_APPS:
            st_list = fetch_steam_reviews(app_cfg["app_id"], app_cfg["name"], count=STEAM_COUNT)
            steam_reviews.extend(st_list)
            for r in st_list: post_lookup[r["id"]] = r
        print(f"  Total Steam reviews added: {len(steam_reviews)}")

    # ── YouTube ingestion ─────────────────────────────────────────────────────
    youtube_comments = []
    yt_api_key = os.getenv("YOUTUBE_API_KEY")
    if YOUTUBE_SEARCHES:
        if not yt_api_key:
            print(f"\n📺 YouTube skipped — add YOUTUBE_API_KEY to .env (free at console.cloud.google.com)")
        else:
            print(f"\n📺 Fetching YouTube comments...")
            for query in YOUTUBE_SEARCHES:
                yt_list = fetch_youtube_comments(query, max_results=YOUTUBE_MAX_RESULTS, api_key=yt_api_key)
                youtube_comments.extend(yt_list)
                for r in yt_list: post_lookup[r["id"]] = r
            print(f"  Total YouTube comments added: {len(youtube_comments)}")

    # ── Merge all non-Reddit sources, filter for intent ───────────────────────
    all_external = (appstore_reviews + googleplay_reviews + trustpilot_reviews
                    + steam_reviews + youtube_comments)
    external_intent = [
        r for r in all_external
        if contains_intent(r.get("title","") + " " + r.get("selftext",""))
        and r.get("score", 0) > 0
    ]

    # Breakdown for logging
    def _count_src(lst, src): return sum(1 for r in lst if r.get("_source") == src)
    ext_counts = {s: _count_src(external_intent, s)
                  for s in ("appstore","googleplay","trustpilot","steam","youtube")}
    ext_summary = ", ".join(f"{v} {k}" for k, v in ext_counts.items() if v > 0)
    print(f"\n  External intent-bearing: {len(external_intent)} / {len(all_external)} ({ext_summary})")

    combined = external_intent + enriched[:MAX_POSTS_FOR_AI]
    reddit_count = min(len(enriched), MAX_POSTS_FOR_AI)
    print(f"  Sending to Claude: {ext_summary} + {reddit_count} Reddit = {len(combined)} total")

    print(f"\n⚙️  Extracting requests from {len(combined)} sources...")
    demands = extract_demands_with_ai(combined)

    print(f"\n📊 Calculating Gravity Scores...")
    for d in demands:
        d["gravity_score"] = calculate_gravity(d, post_lookup)

    # ── Google Trends boost ───────────────────────────────────────────────────
    if GOOGLE_TRENDS_BOOST and demands:
        target = TARGET_NAME or (SUBREDDITS[0].capitalize() if SUBREDDITS else "")
        boosts = fetch_google_trends_boost(demands, target)
        if boosts:
            for d in demands:
                slug = d.get("slug") or _make_demand_slug(d.get("subject",""), d.get("action",""))
                multiplier = boosts.get(slug, 1.0)
                if multiplier > 1.0:
                    d["gravity_score"] = round(d["gravity_score"] * multiplier, 1)
                    d["trends_boost"]  = multiplier

    demands.sort(key=lambda x: x["gravity_score"], reverse=True)

    # ── Print leaderboard ─────────────────────────────────────────────────────
    all_sources_label = subs_label
    if appstore_reviews:  all_sources_label += " + App Store"
    if googleplay_reviews: all_sources_label += " + Google Play"
    if trustpilot_reviews: all_sources_label += " + Trustpilot"
    if steam_reviews:     all_sources_label += " + Steam"
    if youtube_comments:  all_sources_label += " + YouTube"

    print("\n")
    print("=" * 60)
    print(f"  🏆 TOP REQUESTS — {all_sources_label}")
    print(f"  {len(unique_posts)} posts · {len(all_external)} reviews · {len(demands)} requests found")
    print("=" * 60)

    for i, d in enumerate(demands[:10], 1):
        threads = len(d.get("post_ids", []))
        boost   = f" 📈{d['trends_boost']:.1f}x" if d.get("trends_boost") else ""
        print(f"\n  #{i}  {d['subject']}  [{d['category']}]{boost}")
        print(f"  Request : {d['action']}")
        if d.get("summary"):
            print(f"  Summary : {d['summary'][:120]}...")
        print(f"  Score   : {d['gravity_score']:,.0f}  ({threads} thread{'s' if threads!=1 else ''})")
        for pid in d.get("post_ids", [])[:2]:
            if pid in post_lookup:
                t = post_lookup[pid].get("title","")[:65]
                s = post_lookup[pid].get("score", 0)
                src = post_lookup[pid].get("_source","reddit")
                src_label = {"appstore":"📱","googleplay":"▶","trustpilot":"⭐","steam":"🎮","youtube":"📺"}.get(src,"📡")
                print(f"  └ {src_label} [{s:,}] {t}")

    # ── Assemble output ───────────────────────────────────────────────────────
    NON_REDDIT_SOURCES = {"appstore","googleplay","trustpilot","steam","youtube"}
    demanded_pids = {pid for d in demands for pid in d.get("post_ids",[])}

    output = {
        "subreddits":          SUBREDDITS,
        "appstore_apps":       [a["name"] for a in APP_STORE_APPS],
        "googleplay_apps":     [a["name"] for a in GOOGLE_PLAY_APPS],
        "trustpilot_companies":[c["name"] for c in TRUSTPILOT_COMPANIES],
        "steam_apps":          [a["name"] for a in STEAM_APPS],
        "youtube_searches":    YOUTUBE_SEARCHES,
        "scraped_at":          datetime.now(timezone.utc).isoformat(),
        "posts_scanned":       len(unique_posts),
        "appstore_reviews":    len(appstore_reviews),
        "googleplay_reviews":  len(googleplay_reviews),
        "trustpilot_reviews":  len(trustpilot_reviews),
        "steam_reviews":       len(steam_reviews),
        "youtube_comments":    len(youtube_comments),
        "requests_found":      len(demands),
        "leaderboard":         demands[:100],
        "post_lookup": {
            pid: {
                "title":        p.get("title",""),
                "score":        p.get("score", 0),
                "created_utc":  p.get("created_utc", 0),
                "permalink":    p.get("permalink",""),
                "top_comments": p.get("top_comments", []),
                "_source":      p.get("_source","reddit"),
                "_subreddit":   p.get("_subreddit",""),
                "_rating":      p.get("_rating", None),
                "selftext":     p.get("selftext","")[:300] if p.get("_source") in NON_REDDIT_SOURCES else "",
            }
            for pid, p in post_lookup.items()
            if pid in demanded_pids or p.get("_source") in NON_REDDIT_SOURCES
        }
    }

    with open("requester_results.json", "w") as f:
        json.dump(output, f, indent=2)

    try:
        run_id = requester_db.save_run(output)
        print(f"\n\n  ✅ Saved to requester_results.json + requester.db (run #{run_id})")
    except Exception as e:
        print(f"\n  ⚠  DB save failed (JSON still saved): {e}")
        print(f"\n\n  ✅ Saved to requester_results.json ({len(demands)} requests)")
    print("=" * 60)


if __name__ == "__main__":
    run()
