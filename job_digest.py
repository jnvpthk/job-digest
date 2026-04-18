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
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

# ── CONFIG ──────────────────────────────────────────────────────────────────
APIFY_TOKEN        = os.environ["APIFY_TOKEN"]
ANTHROPIC_API_KEY  = os.environ["ANTHROPIC_API_KEY"]
GMAIL_USER         = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_TO           = os.environ["EMAIL_TO"]

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
            "bebity~linkedin-jobs-scraper",
            {
                "title": q["keywords"],
                "location": q.get("location", "India"),
                "publishedAt": "r86400",
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


def scrape_google_jobs() -> list:
    jobs = []
    queries = [
        "Business Analyst Tableau Ahmedabad",
        "BI Analyst SQL remote India",
        "Senior Business Analyst real estate India",
    ]
    for query in queries:
        print(f"  Google Jobs: {query}")
        items = run_actor(
            "orgupdate~google-jobs-scraper",
            {
                "query": query,
                "maxItems": 10,
            },
        )
        for item in items:
            jobs.append({
                "source": "Google Jobs",
                "title": item.get("title", ""),
                "company": item.get("companyName", ""),
                "location": item.get("location", ""),
                "url": item.get("applyLink", item.get("url", "")),
                "description": item.get("description", "")[:800],
                "posted": item.get("postedAt", ""),
            })
    return jobs


def scrape_indeed() -> list:
    jobs = []
    for q in INDEED_QUERIES:
        print(f"  Indeed: {q}")
        items = run_actor(
            "borderline~indeed-scraper",
            {
                "position": q["position"],
                "country": q.get("country", "IN"),
                "location": q.get("location", ""),
                "maxItems": 10,
            },
        )
        for item in items:
            jobs.append({
                "source": "Indeed",
                "title": item.get("positionName", item.get("title", "")),
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
            "stealth_mode~naukri-jobs-search-scraper",
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

def send_email(subject: str, html_body: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_USER
    msg["To"]      = EMAIL_TO
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, EMAIL_TO, msg.as_string())


def build_email(ranked_jobs: list, total_scraped: int) -> tuple:
    today = datetime.now().strftime("%d %b %Y")
    subject = f"🔍 Your Daily Job Digest — {today}"

    rows = ""
    for i, job in enumerate(ranked_jobs, 1):
        score = job.get("score", "?")
        color = "#22c55e" if score >= 8 else "#f59e0b" if score >= 6 else "#ef4444"
        rows += f"""
        <tr style="border-bottom:1px solid #e5e7eb;">
          <td style="padding:12px;font-weight:bold;color:#1e3a5f;">{i}. {job['title']}</td>
          <td style="padding:12px;">{job['company']}</td>
          <td style="padding:12px;">{job['location']}</td>
          <td style="padding:12px;">{job['source']}</td>
          <td style="padding:12px;text-align:center;">
            <span style="background:{color};color:white;padding:2px 8px;border-radius:12px;font-weight:bold;">{score}/10</span>
          </td>
          <td style="padding:12px;color:#6b7280;font-size:13px;">{job.get('reason','')}</td>
          <td style="padding:12px;"><a href="{job.get('url','#')}" style="color:#3b82f6;">Apply →</a></td>
        </tr>"""

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:900px;margin:auto;padding:20px;">
      <h2 style="color:#1e3a5f;">🔍 Daily Job Digest — {today}</h2>
      <p style="color:#6b7280;">Scraped <b>{total_scraped}</b> jobs across LinkedIn, Naukri & Indeed. Here are your top <b>{len(ranked_jobs)}</b> matches:</p>
      <table style="width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,0.1);">
        <thead style="background:#1e3a5f;color:white;">
          <tr>
            <th style="padding:12px;text-align:left;">Role</th>
            <th style="padding:12px;text-align:left;">Company</th>
            <th style="padding:12px;text-align:left;">Location</th>
            <th style="padding:12px;text-align:left;">Source</th>
            <th style="padding:12px;text-align:left;">Fit Score</th>
            <th style="padding:12px;text-align:left;">Why it fits</th>
            <th style="padding:12px;text-align:left;">Link</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="color:#9ca3af;font-size:12px;margin-top:20px;">Powered by Apify + Claude · Runs daily at 8 AM IST</p>
    </div>"""

    return subject, html


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*50}")
    print(f"Job Digest Run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    all_jobs = []

    print("Scraping LinkedIn...")
    all_jobs += scrape_linkedin()

    print("Scraping Google Jobs...")
    all_jobs += scrape_google_jobs()

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

    subject, html = build_email(ranked, len(all_jobs))
    print("\nSending email digest...")
    send_email(subject, html)
    print("Done! ✅")


if __name__ == "__main__":
    main()
