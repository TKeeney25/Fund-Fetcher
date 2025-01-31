
import requests

from fundfetcher.scraper import auth

session = requests.Session()

def get_trailing_returns():
    payload = {
        'duration': 'daily',
        'currency': None,
        'limitAge': None,
        'languageId': 'en',
        'locale': 'en',
        'clientId': 'MDC',
        'benchmarkId': 'mstarorcat',
        'component': 'sal-mip-trailing-return',
        'version': '4.14.0',
    }

    response = session.get(url, headers=HEADERS, params=payload, timeout=TIMEOUT, auth=auth.get_bearer())
    if response.status_code == 401:
        update_bearer()
    return validate_response(response)