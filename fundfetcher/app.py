import csv
from datetime import datetime
from logging.handlers import RotatingFileHandler
import os
import queue
import time
from typing import List

from fundfetcher.constants import CSV_FILE_PATH, IGNORED_FAILED_TICKERS
from fundfetcher.database.query_processor import Processor
from fundfetcher.enums.ticker_types import TickerType
from fundfetcher.messenger.email import send_email_with_results
from fundfetcher.models.trailing_returns import TrailingReturns
from fundfetcher.scraper.ms_scraper import Scraper
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    handlers = [RotatingFileHandler('logs/log.log', maxBytes=100000, backupCount=10)],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def read_funds_csv() -> list:
    funds = []
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
    with open(file_path, mode='r', encoding="utf-8") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if row:
                funds.append(row[0])
    return funds

def main():
    now = datetime.now()
    six_am = now.replace(hour=6, minute=0, second=0, microsecond=0)
    if now > six_am:
        six_am = six_am.replace(day=now.day + 1)
    seconds_until_six_am = (six_am - now).total_seconds()
    seconds_until_six_am_str = time.strftime('%H:%M:%S', time.gmtime(seconds_until_six_am))
    logger.info("Time until 6AM: %s", seconds_until_six_am_str)
    time.sleep(seconds_until_six_am)
    tickers:List[str] = read_funds_csv()
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
    main()
