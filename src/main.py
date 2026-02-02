import csv
from datetime import datetime
from logging.handlers import RotatingFileHandler
import os
import queue
import time
from typing import List

from constants import *
from database.query_processor import Processor
from enums.screener import ScreenerDownPresses
from enums.ticker_types import TickerType
from messenger.email import send_email_with_results
from models.screener_data import ScreenerData
from models.trailing_returns import TrailingReturns
from scraper.ms_scraper import Scraper
import logging
import sys
import winsound

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    handlers = [RotatingFileHandler(LOG_FILE_NAME, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)],
    format=LOG_FORMAT
)

def read_funds_csv() -> set[str]:
    funds:set[str] = set()
    file_path = None
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for root, dirs, files in os.walk(project_root + CSV_FILE_PATH):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                break
        break
    if not file_path:
        exception = FileNotFoundError("No fund file found")
        logger.exception("No fund file found: %s", repr(exception))
        raise exception
    with open(file_path, mode='r', encoding="utf-8-sig") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row:
                funds.add(row[0])
    return funds
# TODO fix browser run out of memory issue
def main_tickertracker():
    with Processor(reuse_db=True) as processor:
        # with Scraper(headless=False, keep_screenshots=False) as scraper:
        #     fund_to_scrape = [ScreenerDownPresses.ETF, ScreenerDownPresses.MUTUAL_FUND]
        #     for fund_type in fund_to_scrape:
        #         logger.info("Processing fund type %s", fund_type)
        #         scraper.go_to_screener(fund_type)
        #         if sys.platform == "win32":
        #             winsound.MessageBeep()
        #         input("Press Enter to continue...")
        #         current, maximum = 0, 1
        #         dont = True
        #         while current != maximum:
        #             if dont:
        #                 dont = False
        #             else:
        #                 current, maximum = scraper.paginate_next()
        #                 logger.info("Current: %s, Maximum: %s", current, maximum)
        #             screener_data:List[ScreenerData] = scraper.get_screener_data()
        #             for data in screener_data:
        #                 processor.add_screener_data(data.symbol, data)
        start_time = datetime.now()
        logger.info("TickerTracker started at %s", start_time.strftime('%Y-%m-%d %H:%M:%S'))
        while (True):
            tickers:List[str] = processor.get_non_filtered_tickers()
            total = len(tickers)
            logger.info("There are %s tickers to process", total)
            ticker_queue = queue.Queue()
            redrive_attempts = 10
            for ticker in tickers:
                ticker_queue.put(ticker)
            with Scraper(headless=True, keep_screenshots=False) as scraper:
                while not ticker_queue.empty():
                    ticker = ticker_queue.get()
                    try:
                        remaining = ticker_queue.qsize()
                        if (processed := max(0, total - remaining - 1)) > 0:
                            elapsed = (datetime.now() - start_time).total_seconds(); est = int((elapsed / processed) * remaining)
                            logger.info("Estimated time remaining: %02d:%02d (HH:MM) %s/%s", est // 3600, (est % 3600) // 60, remaining, total)
                    except Exception:
                        logger.exception("Failed to compute estimated time remaining")
                    if processor.has_ticker_been_processed(ticker):
                        # logger.info("Skipping %s as it has already been processed", ticker)
                        total -= 1
                        continue
                    try:
                        logger.info("Processing %s", ticker)
                        ticker_type:TickerType = scraper.find_ticker(ticker)
                        # TODO get brokerage availability
                        logger.info("Step 1/2 Complete - %s is a %s", ticker, ticker_type.value)
                        negative_returns = scraper.get_number_of_negative_returns(ticker_type)
                        processor.add_number_of_negative_years(ticker, negative_returns)
                        logger.info("Step 2/2 Complete - %s has negative returns %s", ticker, negative_returns)
                        # TODO get risk_score
                        # logger.info("Step 3/4 Complete - %s has a risk score of %s", ticker, 0)
                        processor.mark_ticker_as_processed_successfully(ticker)
                        logger.info("%s has been processed successfully", ticker)
                    except Exception as e:
                        logger.exception("Error processing %s: %s", ticker, repr(e))
                        processor.handle_processing_error(ticker, e)
                        ticker_queue.put(ticker)
            redrive_count = processor.redrive_dlq()
            redrive_attempts -= 1
            if redrive_count == 0 or redrive_attempts == 0:
                break
            logger.warning("Redrove %s tickers from the DLQ", redrive_count)
        processor.export_to_ticker_tracker_xlsx()
        failed_tickers = processor.get_failed_tickers()
        logger.info("The following tickers failed %s", failed_tickers)
        filtered_tickers = processor.get_filtered_tickers()
        logger.info("The following tickers were filtered %s", filtered_tickers)
        unfinished_tickers = processor.get_unfinished_tickers()
        logger.info("The following tickers were unfinished %s", unfinished_tickers)
        if sys.platform == "win32":
            winsound.MessageBeep()


def main():
    # TODO: Handle header row in CSV file
    # TODO: auto-convert BRK/B to BRK.B
    # TODO: Handle error with fund like DXC


    now = datetime.now()
    six_am = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now > six_am:
        six_am = six_am.replace(day=now.day + 1)
    seconds_until_six_am = (six_am - now).total_seconds()
    seconds_until_six_am_str = time.strftime('%H:%M:%S', time.gmtime(seconds_until_six_am))
    logger.info("Time until 6AM: %s", seconds_until_six_am_str)
    time.sleep(seconds_until_six_am)
    tickers:set[str] = read_funds_csv()
    ticker_queue = queue.Queue()
    for ticker in tickers:
        ticker_queue.put(ticker)
    while time.localtime().tm_hour < 6:
        logger.info("Waiting until 6AM to start processing. Current time: %s", time.strftime('%H:%M:%S', time.localtime()))
        time.sleep(60)
    progress:float = 0.0
    original_queue_size = ticker_queue.qsize()
    start_time:int = int(time.time())
    with Processor() as processor:
        processor.add_list_of_tickers(tickers)
        with Scraper() as scraper:
            while not ticker_queue.empty():
                progress = 1 - ticker_queue.qsize() / original_queue_size
                curr_time:int = int(time.time())
                elapsed_time:int = curr_time - start_time
                elapsed_time_str = time.strftime('%H:%M:%S', time.gmtime(elapsed_time))
                if progress == 0:
                    estimated_time_remaining_str = "N/A"
                else:
                    estimated_time_remaining:int = int((elapsed_time / progress) - elapsed_time)
                    estimated_time_remaining_str = time.strftime('%H:%M:%S', time.gmtime(estimated_time_remaining))
                logger.info("Progress: %.2f Percent Complete, Elapsed Time %s, Estimated time remaining %s", round(progress*100, 2), elapsed_time_str, estimated_time_remaining_str)
                ticker = ticker_queue.get()
                if processor.has_ticker_been_processed(ticker):
                    logger.info("Skipping %s as it has already been processed", ticker)
                    continue
                try:
                    logger.info("Processing %s", ticker)
                    ticker_type:TickerType = scraper.find_ticker(ticker)
                    logger.info("Step 1/3 Complete - %s is a %s", ticker, ticker_type.value)
                    trailing_returns:TrailingReturns = scraper.get_trailing_returns(ticker_type)
                    logger.info("Step 2/3 Complete - %s has trailing returns %s", ticker, trailing_returns)
                    processor.add_trailing_returns(ticker, trailing_returns)
                    morningstar_rating = scraper.get_morningstar_rating(ticker_type)
                    logger.info("Step 3/3 Complete - %s has an ms rating of %s", ticker, morningstar_rating)
                    processor.add_morningstar_rating(ticker, morningstar_rating)
                    processor.mark_ticker_as_processed_successfully(ticker)
                    logger.info("%s has been processed successfully", ticker)
                except Exception as e:
                    logger.exception("Error processing %s: %s", ticker, repr(e))
                    processor.handle_processing_error(ticker, e)
                    ticker_queue.put(ticker)
        processor.export_to_csv()
        failed_tickers = processor.get_failed_tickers()
        for ticker in IGNORED_FAILED_TICKERS:
            if ticker in failed_tickers:
                failed_tickers.remove(ticker)
        result_str = f"FundFinder Processing Completed at {datetime.now().strftime('%H:%M:%S')}"
        if len(failed_tickers) > 0:
            logger.info("The following tickers failed %s", failed_tickers)
            if len(failed_tickers) > 5:
                logger.error("More than 5 tickers failed skipping sending to clients.")
                return
            send_email_with_results(f"{result_str}\n\nData may be incomplete for the following tickers: {failed_tickers}")
        else:
            send_email_with_results(result_str)
    logger.info("Processing complete")
if __name__ == "__main__":
    main_tickertracker()
