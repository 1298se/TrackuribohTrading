from decimal import Decimal

SYNC_FREQUENCY_INTERVAL_HOURS = 4
NUM_WORKERS = 14
"""Max 14 workers because of DB config of 5 concurrent + 10 overflow connections"""
TAX = Decimal(0.10)
SELLER_COST = Decimal(0.85)
BATCH_SIZE = 1000
MAX_PROFIT_CUTOFF_DOLLARS = 1
COPIES_TIME_DELTA_DAYS = 7
SALES_TIME_DELTA_DAYS = 7
