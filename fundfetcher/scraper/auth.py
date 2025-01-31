import logging
import re
from threading import Lock
import time
import requests

from .constants import BEARER_TOKEN_URL, BUSY_WAIT, TIMEOUT, TOKEN_TIMER, USER_AGENT
logger = logging.getLogger(__name__)

_session_bearer:str = ""
_last_update:float = 0.0
_update_bearer_lock:Lock = Lock()
_session = requests.Session()
_session.headers.update({'user-agent': USER_AGENT})

def _update_bearer():
    with _update_bearer_lock:
        global _session_bearer, _last_update
        headers = {
            'user-agent': USER_AGENT}
        payload = {
            'Site': 'fr',
            'FC': 'F000010S65',
            'IT': 'FO',
            'LANG': 'fr-FR'}

        response:requests.Response = _session.get(BEARER_TOKEN_URL, headers=headers, params=payload, timeout=TIMEOUT)

        search = re.search('maasToken = \".*\"', response.text, re.IGNORECASE)
        _session_bearer = 'Bearer ' + search.group(0).split('\"')[1]

        _last_update = time.time()

def get_bearer():
    while _update_bearer_lock.locked():
        time.sleep(BUSY_WAIT)
    if time.time() - _last_update > TOKEN_TIMER:
        _update_bearer()
    return _session_bearer

_update_bearer()
