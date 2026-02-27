"""
Lead Dataset Builder - Compliant business lead collection via Google Places API.
Supports checkpoint/resume and optional email extraction from business websites.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote, urlparse


class GooglePlacesAPIError(Exception):
    """Raised when Google Places API returns an error (e.g. REQUEST_DENIED, OVER_QUERY_LIMIT)."""


# Constants
CITIES = [
    # Original 8 (Baden-Württemberg)
    "Mannheim",
    "Heidelberg",
    "Heilbronn",
    "Pforzheim",
    "Ulm",
    "Reutlingen",
    "Tübingen",
    "Esslingen am Neckar",
    # +20 more cities (Germany)
    "Stuttgart",
    "Karlsruhe",
    "Freiburg im Breisgau",
    "Munich",
    "Nuremberg",
    "Augsburg",
    "Regensburg",
    "Frankfurt am Main",
    "Mainz",
    "Wiesbaden",
    "Darmstadt",
    "Cologne",
    "Düsseldorf",
    "Dortmund",
    "Essen",
    "Hamburg",
    "Bremen",
    "Hanover",
    "Berlin",
    "Leipzig",
    "Dresden",
    "Münster",
    "Bonn",
]

NICHES = [
    # Original 11
    "Physical therapists",
    "Dentists",
    "Auto repair shops",
    "Moving companies",
    "Cleaning companies",
    "Beauty & wellness (premium)",
    "Real estate agents",
    "Lawyers / tax advisors",
    "Pet services",
    "Plumbing & heating",
    "Gardening & landscaping",
    # +20 more niches
    "Hair salons",
    "Restaurants",
    "Cafes",
    "Hotels",
    "Gyms / fitness",
    "Electricians",
    "Roofing",
    "Locksmiths",
    "HVAC",
    "Car wash",
    "Florists",
    "Bakeries",
    "Pharmacies",
    "Optometrists",
    "Insurance agents",
    "Accountants",
    "Photographers",
    "Dry cleaning",
    "IT support",
    "Marketing agencies",
    "Catering",
]

# Extra search terms per niche (e.g. German) to get more results for one city+niche (e.g. 100+ leads)
NICHE_SEARCH_VARIATIONS: dict[str, list[str]] = {
    "Physical therapists": ["Physical therapists", "Physiotherapeut", "Physiotherapie", "Physio"],
    "Dentists": ["Dentists", "Zahnarzt", "Zahnarztpraxis", "Zahnärzte"],
    "Auto repair shops": ["Auto repair shops", "Autowerkstatt", "Kfz-Werkstatt", "Autoreparatur"],
    "Moving companies": ["Moving companies", "Umzugsunternehmen", "Möbelspedition", "Umzug"],
    "Cleaning companies": ["Cleaning companies", "Reinigungsservice", "Gebäudereinigung", "Putzdienst"],
    "Beauty & wellness (premium)": ["Beauty & wellness", "Kosmetikstudio", "Wellness", "Schönheit"],
    "Real estate agents": ["Real estate agents", "Immobilienmakler", "Makler", "Immobilien"],
    "Lawyers / tax advisors": ["Lawyers", "Rechtsanwalt", "Steuerberater", "Anwalt"],
    "Pet services": ["Pet services", "Tierarzt", "Hundesalon", "Tierpflege"],
    "Plumbing & heating": ["Plumbing", "Klempner", "Heizung", "Sanitär"],
    "Gardening & landscaping": ["Gardening", "Gartenbau", "Landschaftsgestaltung", "Gärtner"],
    # New niches with German variations
    "Hair salons": ["Hair salon", "Friseur", "Friseursalon", "Haarsalon"],
    "Restaurants": ["Restaurant", "Restaurants", "Gaststätte", "Essen"],
    "Cafes": ["Cafe", "Café", "Kaffee", "Kaffeebar"],
    "Hotels": ["Hotel", "Hotels", "Unterkunft", "Pension"],
    "Gyms / fitness": ["Gym", "Fitnessstudio", "Fitness", "Sportstudio"],
    "Electricians": ["Electrician", "Elektriker", "Elektro", "Elektroinstallation"],
    "Roofing": ["Roofing", "Dachdecker", "Dach", "Dacharbeiten"],
    "Locksmiths": ["Locksmith", "Schlüsseldienst", "Schlosser", "Schlüssel"],
    "HVAC": ["HVAC", "Heizung Sanitär", "Klima", "Lüftung"],
    "Car wash": ["Car wash", "Autowaschanlage", "Autowaschen", "Waschstraße"],
    "Florists": ["Florist", "Blumenladen", "Floristik", "Blumen"],
    "Bakeries": ["Bakery", "Bäckerei", "Bäckereien", "Brot"],
    "Pharmacies": ["Pharmacy", "Apotheke", "Apotheken"],
    "Optometrists": ["Optometrist", "Optiker", "Augenoptiker", "Brillen"],
    "Insurance agents": ["Insurance", "Versicherung", "Versicherungsmakler", "Versicherer"],
    "Accountants": ["Accountant", "Buchhalter", "Buchhaltung", "Steuerberatung"],
    "Photographers": ["Photographer", "Fotograf", "Fotografie", "Fotostudio"],
    "Dry cleaning": ["Dry cleaning", "Chemische Reinigung", "Reinigung", "Wäscherei"],
    "IT support": ["IT support", "IT Dienstleistung", "EDV", "Computer Service"],
    "Marketing agencies": ["Marketing", "Marketing Agentur", "Werbung", "Agentur"],
    "Catering": ["Catering", "Catering Service", "Partyservice", "Event Catering"],
}

COLUMNS = [
    "niche",
    "city",
    "business_name",
    "phone",
    "google_maps_url",
    "website_url",
    "emails_found",
]

# On Vercel, filesystem is read-only except /tmp
if os.environ.get("VERCEL"):
    OUTPUT_DIR = Path("/tmp")
else:
    OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
DEFAULT_OUTPUT_CSV = OUTPUT_DIR / "leads_de_bw.csv"
CHECKPOINT_INTERVAL = 100
MAX_RESULTS_PER_SEARCH = 20
REQUEST_TIMEOUT = 15
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
# Parallel workers for speed (1000+ leads)
PLACE_DETAIL_WORKERS = 12
EMAIL_EXTRACT_WORKERS = 20
TEXT_SEARCH_MAX_PAGES = 5  # 20 * 5 = 100 results per query (more leads per city/niche)
PAGE_TOKEN_DELAY = 2.0  # seconds before using next_page_token (Google requirement)

# Default country code for phone numbers (cities are in Germany)
DEFAULT_PHONE_COUNTRY_CODE = "+49"


def format_phone_with_country_code(phone: str | None) -> str:
    """
    Normalize phone to include country code. German numbers: remove leading 0, add +49.
    E.g. '0621 39182930' -> '+49 621 39182930', '0176 87830185' -> '+49 176 87830185'.
    """
    if not phone or not str(phone).strip():
        return "Nill"
    s = re.sub(r"\s+", " ", str(phone).strip())
    if not s:
        return "Nill"
    if s.startswith("+"):
        return s
    # Assume Germany: strip leading 0 and add +49
    if s.startswith("0"):
        s = s[1:]
    return f"{DEFAULT_PHONE_COUNTRY_CODE} {s}" if s else "Nill"


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: Path) -> dict:
    """Load configuration from JSON file or env. Exits if no API key found."""
    config = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)
    api_key = (
        config.get("google_api_key")
        or config.get("api_key")
        or os.environ.get("GOOGLE_PLACES_API_KEY")
        or os.environ.get("GOOGLE_API_KEY")
    )
    if not api_key or api_key == "YOUR_GOOGLE_API_KEY_HERE":
        logging.error(
            "Please set your Google API key in %s or set GOOGLE_PLACES_API_KEY / GOOGLE_API_KEY env var. "
            "Copy config.example.json to config.json and add your key.",
            config_path,
        )
        sys.exit(1)
    config["google_api_key"] = api_key
    return config


def load_checkpoint(checkpoint_file: Path | None = None) -> tuple[list[dict], set[str], set[str]]:
    """Load checkpoint if exists. Returns (leads, seen_place_ids, seen_domains)."""
    cf = checkpoint_file or CHECKPOINT_FILE
    if not cf.exists():
        return [], set(), set()

    try:
        with open(cf, encoding="utf-8") as f:
            data = json.load(f)
        leads = data.get("leads", [])
        seen_place_ids = set(data.get("seen_place_ids", []))
        seen_domains = set(data.get("seen_domains", []))
        logging.info(
            "Loaded checkpoint: %d leads, %d place_ids, %d domains",
            len(leads), len(seen_place_ids), len(seen_domains),
        )
        return leads, seen_place_ids, seen_domains
    except Exception as e:
        logging.warning("Failed to load checkpoint: %s", e)
        return [], set(), set()


def save_checkpoint(
    leads: list[dict],
    seen_place_ids: set[str],
    seen_domains: set[str],
    checkpoint_file: Path | None = None,
) -> None:
    """Save checkpoint to file."""
    cf = checkpoint_file or CHECKPOINT_FILE
    cf.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "leads": leads,
        "seen_place_ids": list(seen_place_ids),
        "seen_domains": list(seen_domains),
    }
    with open(cf, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info("Checkpoint saved: %d leads", len(leads))


def get_domain_for_dedup(website: str | None) -> str:
    """Extract domain from URL for deduplication."""
    if not website or not website.strip().startswith("http"):
        return ""
    try:
        netloc = urlparse(website.strip()).netloc
        return netloc.lower() if netloc else ""
    except Exception:
        return ""


def text_search(
    query: str, api_key: str, page_token: str | None = None
) -> tuple[list[dict], str | None]:
    """Google Places Text Search. Returns (results, next_page_token or None)."""
    if page_token:
        url = (
            "https://maps.googleapis.com/maps/api/place/textsearch/json"
            f"?pagetoken={quote(page_token)}"
            f"&key={quote(api_key)}"
        )
    else:
        url = (
            "https://maps.googleapis.com/maps/api/place/textsearch/json"
            f"?query={quote(query)}"
            f"&key={quote(api_key)}"
        )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read().decode()
    except Exception as e:
        logging.debug("Text search error: %s", e)
        return [], None
    try:
        out = json.loads(data)
    except json.JSONDecodeError:
        return [], None
    if out.get("status") not in ("OK", "ZERO_RESULTS"):
        status = out.get("status", "UNKNOWN")
        err_msg = out.get("error_message", "")
        logging.warning("API %s: %s", status, err_msg)
        if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT", "INVALID_REQUEST"):
            raise GooglePlacesAPIError(
                f"Google Places API {status}: {err_msg or 'Check your API key and that Places API is enabled.'}"
            )
        return [], None
    results = out.get("results", [])
    next_token = out.get("next_page_token") if out.get("status") == "OK" else None
    return results, next_token


def text_search_all_pages(
    query: str, api_key: str, max_pages: int = TEXT_SEARCH_MAX_PAGES
) -> list[dict]:
    """Fetch up to max_pages of text search results (20 per page)."""
    all_results: list[dict] = []
    page_token: str | None = None
    for _ in range(max_pages):
        if page_token:
            time.sleep(PAGE_TOKEN_DELAY)
        results, page_token = text_search(query, api_key, page_token=page_token)
        all_results.extend(results)
        if not page_token or not results:
            break
    return all_results


def place_details(place_id: str, api_key: str) -> dict:
    """Fetch place details: name, formatted_phone_number, website, url (Google Maps link)."""
    fields = "name,formatted_phone_number,website,url"
    url = (
        "https://maps.googleapis.com/maps/api/place/details/json"
        f"?place_id={quote(place_id)}"
        f"&fields={quote(fields)}"
        f"&key={quote(api_key)}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read().decode()
    except Exception as e:
        logging.debug("Place details error: %s", e)
        return {}
    try:
        out = json.loads(data)
    except json.JSONDecodeError:
        return {}
    if out.get("status") != "OK":
        status = out.get("status", "UNKNOWN")
        err_msg = out.get("error_message", "")
        if status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT", "INVALID_REQUEST"):
            raise GooglePlacesAPIError(
                f"Google Places API {status}: {err_msg or 'Check your API key and that Places API is enabled.'}"
            )
        return {}
    return out.get("result", {})


# File/asset extensions that are not real email domains (false positives from HTML)
EMAIL_FALSE_POSITIVE_EXTENSIONS = (
    ".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".eot", ".otf",
    ".css", ".js", ".map",
)
# Domains to never treat as business email (widgets, trackers, etc.)
EMAIL_BLOCKED_DOMAINS = (
    "example.com", "example.org", "sentry.io", "wixpress.com",
    "personio.de", "personio.com", "kzv-sh.de", "google.com",
    "facebook.com", "youtube.com", "twitter.com", "linkedin.com",
    "gravatar.com", "w3.org", "schema.org", "cloudflare.com",
)


def _is_valid_business_email(email: str, website_url: str) -> bool:
    """Return False if email is clearly a false positive (image path, blocked domain, etc.)."""
    if not email or len(email) < 6 or "@" not in email:
        return False
    e = email.strip().lower()
    # URL-encoded or junk
    if "%" in e or " " in e or "/" in e or "\\" in e:
        return False
    # Image/font/file paths mistaken as email
    for ext in EMAIL_FALSE_POSITIVE_EXTENSIONS:
        if e.endswith(ext) or ext in e.split("@")[-1]:
            return False
    # Domain part looks like a filename (e.g. 2x.png, 261w.jpeg)
    domain = e.split("@")[-1]
    if re.search(r"\d+w\.(jpeg|jpg|png|webp|gif)", domain):
        return False
    if "." not in domain or len(domain) < 4:
        return False
    for blocked in EMAIL_BLOCKED_DOMAINS:
        if domain == blocked or domain.endswith("." + blocked):
            return False
    return True


def _pick_one_email(emails: list[str], website_url: str) -> str:
    """
    From a list of candidate emails, return exactly one: prefer same domain as website.
    """
    website_domain = ""
    try:
        website_domain = urlparse(website_url.strip()).netloc.lower()
        # strip www
        if website_domain.startswith("www."):
            website_domain = website_domain[4:]
    except Exception:
        pass
    valid = [e for e in emails if _is_valid_business_email(e, website_url)]
    if not valid:
        return ""
    # Prefer email whose domain matches the business website
    same_domain = [e for e in valid if website_domain and e.split("@")[-1].lower() == website_domain]
    if same_domain:
        # Prefer info@, kontakt@, buero@, then first
        for prefix in ("info@", "kontakt@", "buero@", "office@", "mail@", "contact@"):
            for e in same_domain:
                if e.lower().startswith(prefix):
                    return e.strip()
        return same_domain[0].strip()
    return valid[0].strip()


def extract_emails_from_website(
    url: str, sleep_seconds: float = 0.05
) -> tuple[list[str], str]:
    """
    Fetch page, extract email-like strings, filter false positives, pick one valid email.
    Returns (list with 0 or 1 email, source_url). The one email is the best match (same domain as website).
    """
    if not url or not url.strip().startswith("http"):
        return [], ""
    raw = set()
    email_re = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    try:
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        req = urllib.request.Request(url.strip(), headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        for m in email_re.finditer(html):
            e = m.group(0).strip()
            raw.add(e)
        one = _pick_one_email(sorted(raw), url)
        return ([one] if one else [], url)
    except Exception as e:
        logging.debug("Email extraction failed for %s: %s", url, e)
        return [], ""


def export_csv(
    leads: list[dict],
    output_path: Path,
    require_email_and_website: bool = False,
) -> None:
    """
    Export leads to CSV.
    If require_email_and_website, only export leads with both email and website;
    if none match, export all so CSV is never empty.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if require_email_and_website:
        rows = [
            lead for lead in leads
            if lead.get("emails_found") and str(lead.get("emails_found", "")).strip() not in ("", "Nill")
            and lead.get("website_url") and str(lead.get("website_url", "")).strip() not in ("", "Nill")
        ]
        if not rows:
            rows = leads
    else:
        rows = leads
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logging.info("Exported %d leads to %s", len(rows), output_path)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Lead Dataset Builder - Google Places API",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.json"),
        help="Path to config JSON (default: config.json)",
    )
    parser.add_argument(
        "--max-leads",
        type=int,
        default=1000,
        help="Maximum number of leads to collect (default: 1000)",
    )
    parser.add_argument(
        "--extract-emails",
        type=str,
        choices=["true", "false"],
        default="true",
        help="Extract emails from websites; only leads with website+email are kept (default: true)",
    )
    parser.add_argument(
        "--sleep-api",
        type=float,
        default=0.2,
        help="Sleep seconds between text search API calls (default: 0.2)",
    )
    parser.add_argument(
        "--sleep-web",
        type=float,
        default=0.1,
        help="Sleep per website fetch when extracting emails in parallel (default: 0.1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_CSV})",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint and start fresh",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


def _fetch_details_task(
    item: tuple[str, str, str, str],
    api_key: str,
    seen_place_ids: set[str],
    seen_domains: set[str],
    lock: threading.Lock,
) -> tuple[dict, str] | None:
    """Fetch place details; return (lead_dict, website) if has website and new domain, else None."""
    place_id, niche, city, name_from_search = item
    details = place_details(place_id, api_key)
    website = (details.get("website") or "").strip()
    if not website or not website.startswith("http"):
        with lock:
            seen_place_ids.add(place_id)
        return None
    domain = get_domain_for_dedup(website)
    with lock:
        if domain and domain in seen_domains:
            seen_place_ids.add(place_id)
            return None
        seen_place_ids.add(place_id)
        if domain:
            seen_domains.add(domain)
    name = details.get("name") or name_from_search or ""
    raw_phone = details.get("formatted_phone_number") or ""
    phone = format_phone_with_country_code(raw_phone)
    maps_url = details.get("url") or ""
    lead = {
        "niche": niche,
        "city": city,
        "business_name": name,
        "phone": phone,
        "google_maps_url": maps_url,
        "website_url": website,
        "emails_found": "Nill",
    }
    return (lead, website)


def _extract_email_task(
    lead_website: tuple[dict, str],
    sleep_web: float,
    leads: list[dict],
    max_leads: int,
    lock: threading.Lock,
    checkpoint_file: Path | None,
    seen_place_ids: set[str],
    seen_domains: set[str],
) -> bool:
    """Extract emails and append to leads if found. Returns True if lead was added."""
    lead, website = lead_website
    try:
        emails, _ = extract_emails_from_website(website, sleep_seconds=sleep_web)
        one_email = emails[0] if emails else ""
        if not one_email:
            return False
        lead["emails_found"] = one_email
        with lock:
            if len(leads) >= max_leads:
                return False
            leads.append(lead)
            if len(leads) % CHECKPOINT_INTERVAL == 0 and checkpoint_file:
                save_checkpoint(leads, seen_place_ids, seen_domains, checkpoint_file)
        return True
    except Exception as e:
        logging.debug("Email extraction failed for %s: %s", website, e)
        return False


def run_google(
    api_key: str,
    leads: list[dict],
    seen_place_ids: set[str],
    seen_domains: set[str],
    max_leads: int,
    extract_emails: bool,
    sleep_api: float,
    sleep_web: float,
    checkpoint_file: Path | None = None,
    cities: list[str] | None = None,
    niches: list[str] | None = None,
    max_time_seconds: float | None = None,
) -> None:
    """Collect leads using Google Places API with parallel place details and email extraction.
    If max_time_seconds is set, stop when that many seconds have elapsed (for Vercel/serverless).
    """
    cities = cities if cities is not None else CITIES
    niches = niches if niches is not None else NICHES
    lock = threading.Lock()
    sleep_api = min(sleep_api, 0.25)  # cap for speed
    sleep_web = min(sleep_web, 0.15)
    deadline = (time.time() + max_time_seconds) if max_time_seconds else None

    def time_left() -> bool:
        return deadline is None or time.time() < deadline

    for niche in niches:
        if not time_left() or len(leads) >= max_leads:
            break
        for city in cities:
            if not time_left() or len(leads) >= max_leads:
                break
            # Use multiple search variations (e.g. German terms) to get more than 60 results per city+niche
            search_terms = NICHE_SEARCH_VARIATIONS.get(niche, [niche])
            all_results: list[dict] = []
            seen_in_batch: set[str] = set()
            for search_term in search_terms:
                if not time_left() or len(leads) >= max_leads:
                    break
                query = f"{search_term} in {city}"
                logging.info("Collecting: %s in %s (query: %s)", niche, city, query)
                time.sleep(sleep_api)
                results = text_search_all_pages(query, api_key)
                for r in results:
                    place_id = r.get("place_id")
                    if place_id and place_id not in seen_in_batch:
                        seen_in_batch.add(place_id)
                        all_results.append(r)
            # Build list of (place_id, niche, city, name) not yet seen globally
            batch: list[tuple[str, str, str, str]] = []
            for r in all_results:
                place_id = r.get("place_id")
                if not place_id:
                    continue
                with lock:
                    if place_id in seen_place_ids:
                        continue
                batch.append((place_id, niche, city, r.get("name") or ""))

            if not batch:
                continue

            # Parallel place details
            candidates: list[tuple[dict, str]] = []
            with ThreadPoolExecutor(max_workers=PLACE_DETAIL_WORKERS) as executor:
                futures = {
                    executor.submit(
                        _fetch_details_task,
                        item,
                        api_key,
                        seen_place_ids,
                        seen_domains,
                        lock,
                    ): item
                    for item in batch
                }
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            candidates.append(result)
                    except Exception as e:
                        logging.debug("Place details task error: %s", e)

            if not candidates or len(leads) >= max_leads:
                continue

            # Parallel email extraction
            with ThreadPoolExecutor(max_workers=EMAIL_EXTRACT_WORKERS) as executor:
                futures = {
                    executor.submit(
                        _extract_email_task,
                        lead_website,
                        sleep_web,
                        leads,
                        max_leads,
                        lock,
                        checkpoint_file,
                        seen_place_ids,
                        seen_domains,
                    ): lead_website
                    for lead_website in candidates
                }
                for future in as_completed(futures):
                    if not time_left() or len(leads) >= max_leads:
                        break
                    try:
                        future.result()
                    except Exception as e:
                        logging.debug("Email task error: %s", e)
            if len(leads) % CHECKPOINT_INTERVAL == 0 and checkpoint_file:
                save_checkpoint(leads, seen_place_ids, seen_domains, checkpoint_file)


def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    extract_emails = args.extract_emails.lower() == "true"
    max_leads = args.max_leads

    if args.clear_checkpoint and CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        logging.info("Checkpoint cleared.")

    leads, seen_place_ids, seen_domains = load_checkpoint()

    if len(leads) >= max_leads:
        logging.info("Already have %d leads (>= max %d). Exporting and exiting.", len(leads), max_leads)
        export_csv(leads, args.output, require_email_and_website=extract_emails)
        return

    logging.info(
        "max_leads=%d, extract_emails=%s, sleep_api=%.2f, sleep_web=%.2f",
        max_leads, extract_emails, args.sleep_api, args.sleep_web,
    )

    config = load_config(args.config)
    api_key = config["google_api_key"]

    run_google(
        api_key=api_key,
        leads=leads,
        seen_place_ids=seen_place_ids,
        seen_domains=seen_domains,
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=args.sleep_api,
        sleep_web=args.sleep_web,
        checkpoint_file=CHECKPOINT_FILE,
    )

    save_checkpoint(leads, seen_place_ids, seen_domains)
    export_csv(leads, args.output, require_email_and_website=extract_emails)
    logging.info("Done. Total leads: %d", len(leads))


def run_collection_for_city_niche(
    api_key: str,
    city: str,
    niche: str,
    max_leads: int,
    extract_emails: bool = True,
    sleep_api: float = 0.2,
    sleep_web: float = 0.1,
) -> list[dict]:
    """
    Run lead collection for a single city and niche. Returns list of lead dicts.
    Used by the web frontend; does not use checkpoint.
    """
    return run_collection_for_cities_niches(
        api_key=api_key,
        cities=[city.strip()],
        niches=[niche.strip()],
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=sleep_api,
        sleep_web=sleep_web,
    )


def run_collection_for_cities_niches(
    api_key: str,
    cities: list[str],
    niches: list[str],
    max_leads: int,
    extract_emails: bool = True,
    sleep_api: float = 0.2,
    sleep_web: float = 0.1,
    max_time_seconds: float | None = None,
) -> list[dict]:
    """
    Run lead collection for multiple cities and niches. Returns list of lead dicts.
    If max_time_seconds is set (e.g. on Vercel), stops when time is up and returns whatever was collected.
    """
    cities = [c.strip() for c in cities if c and str(c).strip()]
    niches = [n.strip() for n in niches if n and str(n).strip()]
    if not cities or not niches:
        return []
    leads: list[dict] = []
    seen_place_ids: set[str] = set()
    seen_domains: set[str] = set()
    run_google(
        api_key=api_key,
        leads=leads,
        seen_place_ids=seen_place_ids,
        seen_domains=seen_domains,
        max_leads=max_leads,
        extract_emails=extract_emails,
        sleep_api=sleep_api,
        sleep_web=sleep_web,
        checkpoint_file=None,
        cities=cities,
        niches=niches,
        max_time_seconds=max_time_seconds,
    )
    return leads


if __name__ == "__main__":
    main()
