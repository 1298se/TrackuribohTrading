import os

from datetime import datetime
from typing import Optional

import requests
from threading import Lock
from requests import RequestException
import logging

TCGPLAYER_CATEGORY_ID = 2
TCGPLAYER_BASE_URL = "https://api.tcgplayer.com"
TCGPLAYER_ACCESS_TOKEN_URL = f'{TCGPLAYER_BASE_URL}/token'
TCGPLAYER_PRICING_URL = f'{TCGPLAYER_BASE_URL}/pricing/sku'
TCGPLAYER_CATALOG_URL = f'{TCGPLAYER_BASE_URL}/catalog'
TCGPLAYER_CATALOG_METADATA_URL = f'{TCGPLAYER_CATALOG_URL}/categories/{TCGPLAYER_CATEGORY_ID}'


def access_token_expired(expiry) -> bool:
    if expiry is None:
        return True
    # Sat, 20 Aug 2022 18:39:21 GMT
    expiry_date = datetime.strptime(expiry, "%a, %d %b %Y %H:%M:%S %Z")

    return datetime.now() > expiry_date


def _fetch_tcgplayer_resource(url, **kwargs):
    try:
        response = requests.get(
            url=url,
            **kwargs
        )

        response.raise_for_status()

        data = response.json()
        errors = data.get('errors')

        if errors is None or len(errors) == 0 or errors[0] == "No products were found.":
            del data['errors']
        else:
            print(f'ERRORS: {data["errors"]} on request {url}, {kwargs}')

        return data
    except RequestException as e:
        print(e)

        return {}


class TCGPlayerCatalogService:
    def __init__(self):
        self.lock = Lock()
        self.access_token = None
        self.access_token_expiry = None

    def get_authorization_headers(self) -> dict:
        with self.lock:
            headers = {}

            if self._check_and_refresh_access_token():
                headers['Authorization'] = f'bearer {self.access_token}'

            return headers

    def get_card_printings(self) -> dict:
        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_METADATA_URL}/printings',
            headers=self.get_authorization_headers()
        ).get('results', [])

    def get_card_conditions(self) -> dict:
        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_METADATA_URL}/conditions',
            headers=self.get_authorization_headers()
        ).get('results', [])

    def get_card_rarities(self):
        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_METADATA_URL}/rarities',
            headers=self.get_authorization_headers()
        ).get('results', [])

    def get_sets(self, offset, limit):
        query_params = {
            'offset': offset,
            'limit': limit,
        }

        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_METADATA_URL}/groups',
            headers=self.get_authorization_headers(),
            params=query_params,
        ).get('results', [])

    def get_cards(self, offset, limit, set_id=None):
        query_params = {
            'getExtendedFields': "true",
            'includeSkus': "true",
            'productTypes': ["Cards"],
            'offset': offset,
            'limit': limit,
            'categoryId': TCGPLAYER_CATEGORY_ID,
        }

        if set_id is not None:
            query_params['groupId'] = set_id

        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_URL}/products',
            headers=self.get_authorization_headers(),
            params=query_params
        ).get('results', [])

    def get_sku_prices(self, sku_ids: list):
        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_PRICING_URL}/{",".join([str(sku_id) for sku_id in sku_ids])}',
            headers=self.get_authorization_headers(),
        )

    def get_total_card_set_count(self) -> Optional[int]:
        query_params = {
            'offset': 0,
            'limit': 1,
        }

        return _fetch_tcgplayer_resource(
            f'{TCGPLAYER_CATALOG_METADATA_URL}/groups',
            headers=self.get_authorization_headers(),
            params=query_params,
        ).get('totalItems', 0)

    def fetch_total_card_count(self, set_id=None) -> Optional[int]:
        query_params = {
            'getExtendedFields': "true",
            'includeSkus': "true",
            'productTypes': ["Cards"],
            'offset': 0,
            'limit': 1,
            'categoryId': TCGPLAYER_CATEGORY_ID,
        }

        if set_id is not None:
            query_params['groupId'] = set_id

        try:
            return _fetch_tcgplayer_resource(
                f'{TCGPLAYER_CATALOG_URL}/products',
                headers=self.get_authorization_headers(),
                params=query_params
            ).get('totalItems', 0)
        except Exception as e:
            print(f"SET ID IS: {set_id}")

    def _check_and_refresh_access_token(self) -> bool:
        if access_token_expired(self.access_token_expiry):
            logging.debug("ACCESS TOKEN EXPIRED: Fetching new one")
            client_id = os.environ.get("TCGPLAYER_CLIENT_ID")
            client_secret = os.environ.get("TCGPLAYER_CLIENT_SECRET")

            try:
                response = requests.post(
                    TCGPLAYER_ACCESS_TOKEN_URL,
                    data={
                        'grant_type': "client_credentials",
                        'client_id': client_id,
                        'client_secret': client_secret,
                    }
                )
                data = response.json()
                self.access_token = data['access_token']
                self.access_token_expiry = data['.expires']

                logging.debug("UPDATING ACCESS TOKEN")

                return True
            except RequestException as e:
                logging.debug(e)
                return False
        else:
            return True
