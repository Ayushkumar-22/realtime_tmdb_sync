from apscheduler.schedulers.blocking import BlockingScheduler
from tmdb_realtime_pg import main 
import datetime

scheduler = BlockingScheduler()

# Run every day at 2 AM
scheduler.add_job(
    main,
    trigger="cron",
    hour=2,
    minute=0,
    id="tmdb_batch_job",
    max_instances=1,
    replace_existing=True
)

print("TMDB scheduler started...")
print("Next run:", datetime.datetime.now())

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    print("Scheduler stopped")
