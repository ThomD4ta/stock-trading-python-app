import schedule
import time
from script import run_stock_job
from datetime import datetime

# --- Logging function to update in tas_log.txt ---
def log_run(msg="Task ran"):
    """Append log messages with timestamp to task_log.txt"""
    with open("task_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {msg}\n")

# --- Job wrappers ---
def basic_job():
    print("Job started at:", datetime.now())
    log_run(f"🚀 Job started at {datetime.now()}")


 # Schedule jobs every minute
schedule.every().minute.do(basic_job)
schedule.every().minute.do(run_stock_job)

print("Scheduler started. Press Ctrl+C to stop.")

# Run loop
while True:
    schedule.run_pending()
    time.sleep(1)
