"""
Daily Job Digest for Jahnvi Pathak
- Scrapes LinkedIn, Naukri, Indeed via Apify
- Ranks jobs using Claude API
- Sends digest via WhatsApp (Twilio)
- Schedule: runs daily at 8:00 AM IST
"""

import os
import json
import time
import requests
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
APIFY_TOKEN        = os.environ["APIFY_TOKEN"]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
TWILIO_SID         = os.environ["TWILIO_ACCOUNT_SID"]
TWILIO_AUTH        = os.environ["TWILIO_AUTH_TOKEN"]
TWILIO_FROM        = os.environ["TWILIO_WHATSAPP_FROM"]   # e.g. "whatsapp:+14155238886"
WHATSAPP_TO        = os.environ["WHATSAPP_TO"]            # e.g. "whatsapp:+918237730919"

APIFY_BASE = "https://api.apify.com/v2"

# ── JAHNVI'S PROFILE ─────────────────────────────────────────────────────────
PROFILE = """
Name: Jahnvi Pathak
Role: Senior Business Analyst / BI Analyst
Experience: 3+ years
Current Company: WeWork India (Real Estate / Flexible Workspaces)

Core Skills:
- Tableau (Advanced), SQL (Automation, Performance tuning), PowerBI
- Advanced Excel, Airtable, GitHub, BigQuery
- Dashboard design: operational → strategic dashboards
- Cohort analysis, churn/renewals analysis, revenue forecasting
- MIS reporting, executive dashboards, KPI definition
- Stakeholder management, cross-functional collaboration
- Process automation, data adoption training

Domain Expertise:
- Real Estate, SaaS, Flexible Workspaces
- Footfall analytics, lease performance, non-aero revenue
- Product analytics (digital products, NPS, MAU)

Education:
- MBA (Urban Management) - CEPT University, Ahmedabad
- B.Des (Interior Space & Furniture) - MIT Institute of Design

Location: Ahmedabad, India
Job Preference: Full-time | Ahmedabad-based OR Remote
"""

# ── JOB SEARCH QUERIES ───────────────────────────────────────────────────────
LINKEDIN_QUERIES = [
    {"keywords": "Business Analyst Tableau SQL", "location": "Ahmedabad"},
    {"keywords": "BI Analyst Business Intelligence", "location": "Ahmedabad"},
    {"keywords": "Business Analyst Tableau", "location": "Remote India"},
    {"keywords": "Data Analyst Real Estate", "location": "India"},
]

INDEED_QUERIES = [
    {"position": "Business Analyst", "country": "IN", "location": "Ahmedabad"},
    {"position": "BI Analyst Tableau", "country": "IN", "location": "Ahmedabad"},
    {"position": "Business Analyst remote", "country": "IN"},
]

NAUKRI_QUERIES = [
    "Business Analyst Tableau Ahmedabad",
    "BI Analyst SQL Ahmedabad",
    "Business Analyst remote India",
    "Senior Business Analyst real estate",
]

# ── APIFY HELPERS ─────────────────────────────────────────────────────────────

def run_actor(actor_id: str, run_input: dict, timeout: int = 120) -> list:
    """Run an Apify actor and return dataset items."""
    url = f"{APIFY_BASE}/acts/{actor_id}/runs?token={APIFY_TOKEN}"
    resp = requests.post(url, json=run_input, timeout=30)
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]

    # Poll until finished
    for _ in range(timeout // 5):
        time.sleep(5)
        status_resp = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}?token={APIFY_TOKEN}", timeout=15
        )
        status = status_resp.json()["data"]["status"]
        if status == "SUCCEEDED":
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"Actor {actor_id} run {status}")
            return []

    dataset_id = status_resp.json()["data"]["defaultDatasetId"]
    items_resp = requests.get(
        f"{APIFY_BASE}/datasets/{dataset_id}/items?token={APIFY_TOKEN}&format=json&clean=true",
        timeout=30,
    )
    return items_resp.json() if items_resp.ok else []


def scrape_linkedin() -> list:
    jobs = []
    for q in LINKEDIN_QUERIES:
        print(f"  LinkedIn: {q}")
        items = run_actor(
            "bebity/linkedin-jobs-scraper",
            {
                "title": q["keywords"],
                "location": q.get("location", "India"),
                "publishedAt": "r86400",   # last 24 hours
                "rows": 10,
            },
        )
        for item in items:
            jobs.append({
                "source": "LinkedIn",
                "title": item.get("title", ""),
                "company": item.get("companyName", ""),
                "location": item.get("location", ""),
                "url": item.get("jobUrl", ""),
                "description": item.get("description", "")[:800],
                "posted": item.get("postedAt", ""),
            })
    return jobs


def scrape_indeed() -> list:
    jobs = []
    for q in INDEED_QUERIES:
        print(f"  Indeed: {q}")
        items = run_actor(
            "misceres/indeed-scraper",
            {
                "position": q["position"],
                "country": q.get("country", "IN"),
                "location": q.get("location", ""),
                "maxItems": 10,
                "parseCompanyDetails": False,
            },
        )
        for item in items:
            jobs.append({
                "source": "Indeed",
                "title": item.get("positionName", ""),
                "company": item.get("company", ""),
                "location": item.get("location", ""),
                "url": item.get("url", ""),
                "description": item.get("description", "")[:800],
                "posted": item.get("datePosted", ""),
            })
    return jobs


def scrape_naukri() -> list:
    jobs = []
    for query in NAUKRI_QUERIES:
        print(f"  Naukri: {query}")
        items = run_actor(
            "curious_coder/naukri-scraper",
            {
                "keyword": query,
                "location": "Ahmedabad",
                "experience": "2-5",
                "rows": 10,
            },
        )
        for item in items:
            jobs.append({
                "source": "Naukri",
                "title": item.get("title", ""),
                "company": item.get("company", ""),
                "location": item.get("location", ""),
                "url": item.get("jdURL", item.get("url", "")),
                "description": item.get("jobDescription", "")[:800],
                "posted": item.get("createdDate", ""),
            })
    return jobs


def deduplicate(jobs: list) -> list:
    seen = set()
    unique = []
    for job in jobs:
        key = (job["title"].lower().strip(), job["company"].lower().strip())
        if key not in seen and job["title"]:
            seen.add(key)
            unique.append(job)
    return unique


# ── CLAUDE RANKING ────────────────────────────────────────────────────────────

def rank_jobs_with_claude(jobs: list) -> list:
    """Send jobs to Claude API and get top 10 ranked with fit scores."""
    jobs_text = json.dumps(
        [{"id": i, **j} for i, j in enumerate(jobs)], indent=2
    )

    prompt = f"""You are a career advisor. Below is a candidate's professional profile and a list of job postings scraped today.

CANDIDATE PROFILE:
{PROFILE}

JOB POSTINGS (JSON):
{jobs_text}

Task:
1. Score each job from 1–10 based on how well it fits the candidate's skills, experience, domain, and location preference (Ahmedabad or Remote).
2. Return ONLY the top 10 jobs as a JSON array, sorted by score descending.
3. Each item must have: id, title, company, location, source, url, score (int), reason (1 sentence why it's a good fit).
4. Return raw JSON only — no markdown, no explanation outside the JSON array."""

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    raw = resp.json()["content"][0]["text"].strip()
    raw = raw.replace("```json", "").replace("```", "").strip()
    ranked = json.loads(raw)

    # Merge full job data back in
    job_map = {i: j for i, j in enumerate(jobs)}
    for r in ranked:
        r.update(job_map.get(r["id"], {}))

    return ranked


# ── WHATSAPP DIGEST ───────────────────────────────────────────────────────────

def send_whatsapp(message: str):
    from twilio.rest import Client
    client = Client(TWILIO_SID, TWILIO_AUTH)
    # Split into chunks if > 1600 chars (WhatsApp limit)
    chunks = [message[i:i+1500] for i in range(0, len(message), 1500)]
    for chunk in chunks:
        client.messages.create(
            body=chunk,
            from_=TWILIO_FROM,
            to=WHATSAPP_TO,
        )
        time.sleep(1)


def build_message(ranked_jobs: list, total_scraped: int) -> str:
    today = datetime.now().strftime("%d %b %Y")
    lines = [
        f"🔍 *Daily Job Digest — {today}*",
        f"_Scraped {total_scraped} jobs · Showing top {len(ranked_jobs)} matches for Jahnvi_\n",
    ]
    for i, job in enumerate(ranked_jobs, 1):
        score = job.get("score", "?")
        emoji = "🟢" if score >= 8 else "🟡" if score >= 6 else "🔴"
        lines.append(
            f"{i}. {emoji} *{job['title']}* — {job['company']}\n"
            f"   📍 {job['location']} | 🌐 {job['source']} | ⭐ {score}/10\n"
            f"   💡 {job.get('reason', '')}\n"
            f"   🔗 {job.get('url', 'N/A')}\n"
        )
    lines.append("_Powered by Apify + Claude · Runs daily at 8 AM IST_")
    return "\n".join(lines)


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*50}")
    print(f"Job Digest Run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    all_jobs = []

    print("Scraping LinkedIn...")
    all_jobs += scrape_linkedin()

    print("Scraping Indeed...")
    all_jobs += scrape_indeed()

    print("Scraping Naukri...")
    all_jobs += scrape_naukri()

    print(f"\nTotal raw jobs: {len(all_jobs)}")
    all_jobs = deduplicate(all_jobs)
    print(f"After deduplication: {len(all_jobs)}")

    if not all_jobs:
        print("No jobs found today. Sending notification.")
        send_whatsapp("🔍 Daily Job Digest: No new jobs found today. Will retry tomorrow!")
        return

    print("\nRanking with Claude...")
    ranked = rank_jobs_with_claude(all_jobs)
    print(f"Top {len(ranked)} jobs selected.")

    message = build_message(ranked, len(all_jobs))
    print("\nSending WhatsApp digest...")
    send_whatsapp(message)
    print("Done! ✅")


if __name__ == "__main__":
    main()
