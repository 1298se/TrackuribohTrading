import logging

import sentry_sdk
from apscheduler.schedulers.blocking import BlockingScheduler
from sentry_sdk.integrations.logging import LoggingIntegration

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

sentry_sdk.init(
    dsn="https://b4c75638f0b0c1048390ff7a3b0fb352@o4506209812873216.ingest.sentry.io/4506209838366720",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
    integrations=[
        LoggingIntegration(
            level=logging.INFO,
            event_level=logging.INFO,
        )
    ]
)

scheduler = BlockingScheduler(
    job_defaults={'misfire_grace_time': 60}
)

logger = logging.getLogger(__name__)
