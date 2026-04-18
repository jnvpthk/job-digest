"""
Scheduler — runs job_digest.py every day at 8:00 AM IST (UTC+5:30)
Deploy this on Railway / Render / any always-on server.
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import subprocess
import sys

scheduler = BlockingScheduler(timezone=pytz.utc)

@scheduler.scheduled_job(CronTrigger(hour=2, minute=30, timezone="UTC"))  # 8:00 AM IST = 2:30 AM UTC
def run_digest():
    print("⏰ Scheduler triggered — running job digest...")
    subprocess.run([sys.executable, "job_digest.py"], check=True)

print("🚀 Scheduler started. Job digest will run daily at 8:00 AM IST.")
scheduler.start()
