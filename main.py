import sentry_sdk
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.schedulers.blocking import BlockingScheduler

from jobs.fetch_card_listing_prices import fetch_all_near_mint_card_listing_data
from jobs.update_ygo_database import update_card_database


def job_listener(event):
    job = event.job
    if event.exception:
        # Job failed, capture the exception
        with sentry_sdk.push_scope() as scope:
            scope.set_extra("job_id", job.id)
            scope.set_extra("job_name", str(job))
            sentry_sdk.capture_exception(event.exception)
    else:
        # Job succeeded, capture a message
        sentry_sdk.capture_message(f"Job {job.id} completed successfully")


sentry_sdk.init(
    dsn="https://b4c75638f0b0c1048390ff7a3b0fb352@o4506209812873216.ingest.sentry.io/4506209838366720",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

scheduler = BlockingScheduler()

scheduler.add_job(update_card_database, trigger='interval', days=1)
scheduler.add_job(fetch_all_near_mint_card_listing_data, trigger='interval', hours=1)

scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

# Trigger database update on initialization

if __name__ == "__main__":
    update_card_database()
    scheduler.start()
