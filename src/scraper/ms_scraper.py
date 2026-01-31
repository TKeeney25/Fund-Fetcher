from datetime import datetime
import logging
import os
from time import sleep
from typing import List

import selenium
import selenium.webdriver
import undetected_chromedriver as uc
from urllib3.exceptions import MaxRetryError
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import WebDriverException
from undetected_chromedriver import Chrome, WebElement

from enums.screener import ScreenerDownPresses
from enums.ticker_types import TickerType
from models import trailing_returns
from models.trailing_returns import TrailingReturns
from constants import *

logger = logging.getLogger(__name__)

class Scraper:
    driver:Chrome
    wait:WebDriverWait
    retries = 0
    retry_backoff = [0, 10, 60, 5*60, 10*60, 60*60]
    headless:bool

    def __init__(self, keep_screenshots:bool = False, headless:bool = True):
        if not keep_screenshots:
            self.clear_screenshots_folder()
        self.headless = headless

    def __enter__(self):
        self.login()
        return self
    
    def __exit__(self, *_):
        self.driver.quit()

    @staticmethod
    def scraper_exception_handler(func): # TODO add retries
        def inner_function(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (MaxRetryError, WebDriverException) as e:
                scraper = args[0]
                logger.exception("Max retries exceeded doing func %s with args: %s", func.__name__, *args)
                try:
                    scraper.driver.quit()
                except Exception:
                    pass
                scraper.login()
                raise e
            except Exception as e:
                scraper = args[0]
                # scraper.screenshot(f"EXCEPTION_{func.__name__}")
                logger.exception("Exception occurred at url %s: %s", scraper.driver.current_url, repr(e))
                raise e
        return inner_function

    def check_chrome_is_up_to_date(self):
        with selenium.webdriver.Chrome() as driver:
            # TODO: Make better
            driver.get("chrome://settings/help")
            sleep(30)
            logger.info("Hopefully Chrome is up to date")

    @scraper_exception_handler
    def login(self):
        self.check_chrome_is_up_to_date()
        logger.info("Logging in to Morningstar")
        self.driver = uc.Chrome(headless=self.headless, use_subprocess=False, version_main=144)
        self.driver.command_executor.set_timeout(SELENIUM_TIMEOUT)
        self.driver.get(LOGIN_URL)

        self.wait = WebDriverWait(self.driver, SELENIUM_TIMEOUT, 0.01)
        username_field = self.wait.until(EC.presence_of_element_located((By.ID, 'username')))
        username_field.send_keys(ADMIN_EMAIL)

        submit_button = self.wait.until(EC.presence_of_element_located((By.XPATH, LOGIN_BUTTON)))
        submit_button.click()

        password_field = self.wait.until(EC.presence_of_element_located((By.ID, 'password')))
        password_field.send_keys(LOGIN_PASSWORD)

        old_url = self.driver.current_url
        submit_button = self.wait.until(EC.presence_of_element_located((By.XPATH, LOGIN_BUTTON)))
        submit_button.click()
        # self.wait.until(EC.url_changes(old_url))
        sleep(10)

        if self.driver.current_url != BASE_URL:
            logger.error("Login failed. Current URL equals %s", self.driver.current_url)
            raise ValueError(f"Login failed. Current URL equals {self.driver.current_url}")
        logger.info("Successfully logged in to Morningstar")



    @scraper_exception_handler
    def _check_page_loaded(self):
        self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'mdc-mo__button-image')))

    @scraper_exception_handler
    def find_ticker(self, ticker:str) -> TickerType:
        if self.driver.current_url.split("/")[-2].lower() != ticker.lower():
            search_field = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'mdc-search-field__input__mdc')))
            search_field.send_keys(ticker)
            try:
                old_url = self.driver.current_url
                self.wait.until(EC.visibility_of_all_elements_located((By.CLASS_NAME, 'mdc-site-search__result__mdc')))
                results = self.driver.find_elements(By.CLASS_NAME, 'mdc-site-search__result__mdc')
                results[0].click()
                self.wait.until(EC.url_changes(old_url))
            except selenium.common.exceptions.TimeoutException:
                logger.warning("Ticker %s not found in search recommendations. Trying direct URL", ticker)
        
        if self.driver.current_url.split("/")[-2].lower() != ticker.lower(): # Alternate way to find ticker for difficult funds like FGTXX
            self.driver.get(SEARCH_URL + ticker)
            search_all = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'search-all__section')))
            search_hits = search_all.find_elements(By.CLASS_NAME, 'search-all__hit')
            old_url = self.driver.current_url
            for hit in search_hits:
                link = hit.find_element(By.TAG_NAME, 'a')
                metadata = hit.find_element(By.CLASS_NAME, 'mdc-security-module__metadata')
                found_ticker = metadata.find_element(By.CLASS_NAME, 'mdc-security-module__ticker').text
                if found_ticker.lower() == ticker.lower():
                    link.click()
                    break
            self.wait.until(EC.url_changes(old_url))

        if self.driver.current_url.split("/")[-2].lower() != ticker.lower():
            logger.error("Failed to find ticker: %s. URL equaled %s", ticker, self.driver.current_url)
            raise ValueError(f"Failed to find ticker: {ticker}. URL equaled {self.driver.current_url}")
        try:
            self.driver.find_element(By.CLASS_NAME, 'mdc-metadata__list__mdc')
            return TickerType.STOCK
        except selenium.common.exceptions.NoSuchElementException:
            if ticker[-1].upper() == "X":
                return TickerType.MUTUAL_FUND
            return TickerType.ETF

    @scraper_exception_handler
    def get_trailing_returns(self, ticker_type:TickerType) -> TrailingReturns:
        if ticker_type == TickerType.STOCK:
            return self._get_stock_trailing_returns()
        return self._get_trailing_returns()

    @scraper_exception_handler
    def _navigate_to_span(self, span_name:str, validation_str:str):
        old_url = self.driver.current_url
        if validation_str.lower() in old_url.lower():
            return
        span = self.wait.until(EC.presence_of_element_located((By.XPATH, f"//ul/li/a/span[contains(text(), '{span_name}')]")))
        span.click()
        self.wait.until(EC.url_changes(old_url))
        if validation_str.lower() not in self.driver.current_url.lower():
            raise ValueError(f"Span navigation failed. URL equaled {self.driver.current_url} instead of {validation_str}")

    @scraper_exception_handler
    def _get_stock_trailing_returns(self) -> TrailingReturns:
        self._navigate_to_span("Trailing Returns", "trailing-returns")

        table = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "mds-table--fixed-column__sal")))
        thead = table.find_element(By.TAG_NAME, "thead")
        title_row = thead.find_element(By.TAG_NAME, "tr")
        tbody = table.find_element(By.TAG_NAME, "tbody")
        data_rows = tbody.find_elements(By.TAG_NAME, "tr")

        title_row_list = self._convert_table_row_to_list(title_row)
        data_row_list = self._convert_table_row_to_list(data_rows[0])
        returns = trailing_returns.etl(title_row_list, data_row_list)
        if trailing_returns.is_all_null(returns):
            raise ValueError("No trailing returns found for stock at url %s", self.driver.current_url)
        return returns

    @scraper_exception_handler
    def _get_trailing_returns(self) -> TrailingReturns:
        self._navigate_to_span("Performance", "performance")
        table = self.wait.until(EC.presence_of_element_located((By.XPATH, ".//table[contains(@class, 'mds-table--fixed-column__sal') and ancestor::sal-components[contains(@tab, 'trailing-returns')]]")))
        thead = table.find_element(By.TAG_NAME, "thead")
        title_row = thead.find_element(By.TAG_NAME, "tr")
        tbody = table.find_element(By.TAG_NAME, "tbody")
        data_rows = tbody.find_elements(By.TAG_NAME, "tr")

        title_row_list = self._convert_table_row_to_list(title_row)
        data_row_list = self._convert_table_row_to_list(data_rows[0])
        returns = trailing_returns.etl(title_row_list, data_row_list)
        if trailing_returns.is_all_null(returns):
            raise ValueError("No trailing returns found for fund/etf at url %s", self.driver.current_url)
        return returns
    
    @scraper_exception_handler
    def get_morningstar_rating(self, ticker_type:TickerType) -> int | None:
        if ticker_type == TickerType.STOCK:
            try:
                stock_stars_span = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "mdc-star-rating")))
            except selenium.common.exceptions.TimeoutException:
                logger.warning("No star rating found for %s", ticker_type.value)
                return None
            star_svgs = stock_stars_span.find_elements(By.CLASS_NAME, "mdc-star-rating__star__mdc")
            return len(star_svgs)
        non_stock_stars_div = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, "mdc-security-header__details")))
        try:
            non_stock_stars_span = non_stock_stars_div.find_element(By.CLASS_NAME, "mdc-security-header__star-rating")
            rating = non_stock_stars_span.get_attribute('title')[0]
            if rating.lower() == "u":
                return None
            return int(rating)
        except selenium.common.exceptions.NoSuchElementException:
            return None


    def _convert_table_row_to_list(self, row:WebElement) -> List[str]:
        output_list = []
        th = row.find_elements(By.TAG_NAME, 'th')
        td = row.find_elements(By.TAG_NAME, 'td')
        cells = []
        if th is not None:
            cells += th
        if td is not None:
            cells += td
        for cell in cells:
            output_list.append(cell.text)
        return output_list

    @scraper_exception_handler
    def go_to_screener(self, investment_type:ScreenerDownPresses):
        self.driver.get('https://www.morningstar.com/tools/screener')
        select_element = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'select')))
        select_element.click()
        for _ in range(3):
            select_element.send_keys(Keys.ARROW_UP)
        for _ in range(investment_type.value):
            select_element.send_keys(Keys.ARROW_DOWN)
        select_element.send_keys(Keys.ENTER)
        split_button = self.wait.until(EC.presence_of_element_located((By.CLASS_NAME, 'mdc-split-button__button__mdc')))
        split_button.click()
        temp_div = self.wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(text(), 'Temp')]")))
        temp_div.click()

        # self.wait.until(EC.presence_of_element_located((By.XPATH, "//legend[contains(text(), 'Morningstar Rating for')]/ancestor::fieldset//input[@type='checkbox']")))
        # checkboxes = self.driver.find_elements(By.XPATH, "//legend[contains(text(), 'Morningstar Rating for')]/ancestor::fieldset//input[@type='checkbox']")
        self.wait.until(EC.presence_of_element_located((By.XPATH, "//legend[contains(text(), 'Morningstar Rating for')]/ancestor::fieldset/label")))
        checkbox_labels = self.driver.find_elements(By.XPATH, "//legend[contains(text(), 'Morningstar Rating for')]/ancestor::fieldset/label")
        
        # fieldset = legend.find_element(By.XPATH, "./ancestor::fieldset")
        # checkboxes = fieldset.find_elements(By.XPATH, ".//input[@type='checkbox']")
        for checkbox_label in checkbox_labels:
            checkbox = checkbox_label.find_element(By.TAG_NAME, 'input')
            checked = len(checkbox_label.find_element(By.TAG_NAME, 'span').find_element(By.TAG_NAME, 'span').find_elements(By.XPATH, "./*")) > 0
            print(f"checkbox {checkbox.get_attribute('value')}: {checked}")
            try:
                value = int(checkbox.get_attribute('value'))
            except ValueError:
                continue
            if value >= 4 and not checked or value < 4 and checked:
                self.driver.execute_script("arguments[0].click();", checkbox)
                print("Clicked", value)

    @scraper_exception_handler
    def get_all_tickers_and_ratings(self):
        tbody = self.wait.until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
        rows = tbody.find_elements(By.TAG_NAME, 'tr')
        print("Row len", len(rows))

        fund_ratings = {}
        for row in rows:
            columns = row.find_elements(By.TAG_NAME, 'td')
            count = 0
            ticker = ""
            rating = 0
            for column in columns:
                count += 1
                if count == 1:
                    continue
                if count == 2:
                    ticker = column.find_element(By.TAG_NAME, 'div').find_element(By.TAG_NAME, 'div').find_element(By.TAG_NAME, 'span').text.strip()
                else:
                    rating = len(column.find_element(By.TAG_NAME, 'span').find_elements(By.TAG_NAME, 'span'))
            fund_ratings[ticker] = rating
        return fund_ratings
    
    @scraper_exception_handler
    def paginate_next(self):
        buttons = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, "//button")))
        next_button = None
        for button in buttons:
            try:
                if 'Next' in button.get_attribute('aria-label'):
                    next_button = button
                    break
            except TypeError:
                continue
        if next_button is None:
            raise ValueError("Next button not found")
        next_button.click()


    def screenshot(self, screenshot_source=""):
        try:
            if len(screenshot_source) > 0:
                screenshot_source = f"_{screenshot_source}"
            screenshot_folder = SCREENSHOTS_FOLDER
            if not os.path.exists(screenshot_folder):
                os.makedirs(screenshot_folder)

            screenshot_id = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

            screenshot_path = os.path.join(screenshot_folder, f'{screenshot_id}{screenshot_source}.png')
            self.driver.save_screenshot(screenshot_path)
        except Exception as e:
            logger.error("Failed to take screenshot: %s", repr(e))

    def clear_screenshots_folder(self):
        screenshot_folder = SCREENSHOTS_FOLDER
        if os.path.exists(screenshot_folder):
            for file in os.listdir(screenshot_folder):
                file_path = os.path.join(screenshot_folder, file)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                except Exception as e:
                    logger.error("Failed to delete file %s: %s", file_path, repr(e))
