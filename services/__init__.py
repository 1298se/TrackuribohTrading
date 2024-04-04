import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

sentry_sdk.init(
    dsn="https://10a229c7bc4d2953f655cca7add05f6f@o4506209812873216.ingest.us.sentry.io/4507025756061696",
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
