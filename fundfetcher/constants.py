from pathlib import Path
from fundfetcher.helpers import get_root_dir
import json

with open(Path(get_root_dir()) / 'config.json', encoding='utf-8') as f:
    config:dict = json.load(f)

# Config
SELENIUM_TIMEOUT = 30
SELENIUM_POLLING_RATE = 0.01

BASE_URL = "https://www.morningstar.com/"
SEARCH_URL = f"{BASE_URL}search?query="

LOGIN_URL = f"{BASE_URL}login"
LOGIN_EMAIL = config.get("ADMIN_EMAIL")
LOGIN_PASSWORD = config.get("ADMIN_PASSWORD")

SCREENSHOTS_FOLDER = 'screenshots'

# region Logging
LOG_FILE_NAME = 'logs/log.log'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_MAX_BYTES = 100000
LOG_BACKUP_COUNT = 10
# endregion

# REUSED XPATHS
LOGIN_BUTTON = "//button[@type='submit']"
SPAN_CONTAINS_TEXT = "//span[contains(text(), '{text}')]"

CSV_FILE_PATH = '/fundfetcher/funds/'
MAX_PROCESSING_ATTEMPTS = 3
EMAIL_SOURCE = config.get('AWS_EMAIL')

CLIENT_EMAILS = config.get('CLIENT_EMAILS')
OUTPUT_CSV_FILE = 'DailyFundReturns.csv'
OUTPUT_CSV_FILE_PATH = Path(get_root_dir()) / 'output' / OUTPUT_CSV_FILE
