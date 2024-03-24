from services.find_profitable_skus import find_profitable_skus
from tasks import scheduler
from tasks.fetch_card_listings import fetch_all_near_mint_card_listing_data
from tasks.update_card_database import update_card_database  # Import your task
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

scheduler.add_job(update_card_database, trigger='interval', days=1)  # Schedule to run every day
scheduler.add_job(fetch_all_near_mint_card_listing_data, id="fetch_all_near_mint_listing", trigger='cron',
                  hour='0,6,12,18')


# For now we're not fetching card sales because it can be replaced by listing count deltas, and the listings endpoint
# is not rate limited

def job_listener(event):
    if event.job_id == 'fetch_all_near_mint_listing':
        scheduler.add_job(find_profitable_skus)


scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

if __name__ == "__main__":
    scheduler.add_job(update_card_database)
    scheduler.start()
