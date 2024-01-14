import time

from models.card_sync_data import SyncFrequency
from tasks import scheduler
from tasks.fetch_card_listings import fetch_all_near_mint_card_listing_data
from tasks.update_card_database import update_card_database  # Import your task
from tasks.fetch_card_sales import fetch_card_sales_data_for_frequency

scheduler.add_job(update_card_database, trigger='interval', days=1)  # Schedule to run every minute
scheduler.add_job(fetch_all_near_mint_card_listing_data, trigger='interval', hours=6)
# For now we're not fetching card sales because it can be replaced by listing count deltas, and the listings endpoint
# is not rate limited


if __name__ == "__main__":
    update_card_database()
    fetch_all_near_mint_card_listing_data()
    scheduler.start()
