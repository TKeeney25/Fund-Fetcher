import csv
from datetime import datetime
from logging.handlers import RotatingFileHandler
import os
import queue
import time
from typing import List

from constants import *
from database.query_processor import Processor
from enums.ticker_types import TickerType
from controls import check_data_controls
from messenger.email import send_email_with_results
from models.trailing_returns import TrailingReturns
from scraper.ms_scraper import Scraper
import logging
from datetime import timedelta

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

def main_tickertracker():
    with Processor(reuse_db=True) as processor:
        with Scraper(keep_screenshots=True) as scraper:
            tickers:List[str] = read_funds_csv()
            for ticker in tickers:
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
                except Exception as e:
                    logger.exception("Error processing %s: %s", ticker, repr(e))

def is_non_ticker(ticker:str) -> bool:
    if "symbol" in ticker.lower():
        return True
    if len(ticker) > 5:
        return True
    return False

def ticker_to_ms_ticker(ticker:str) -> str:
    ms_ticker = ticker.replace("/", ".")
    return ms_ticker

def sleep_until_time(hour:int):
    now = datetime.now()
    sleep_target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if now >= sleep_target:
        sleep_target += timedelta(days=1)
    seconds_until_six_am = (sleep_target - now).total_seconds() + 1
    logger.info("Sleeping until %s. Time remaining: %s", hour, time.strftime('%H:%M:%S', time.gmtime(seconds_until_six_am)))
    time.sleep(seconds_until_six_am)

    seconds_until_six_am = (sleep_target - datetime.now()).total_seconds() + 1
    while seconds_until_six_am > 0:
        logger.info("Sleeping until %s. Time remaining: %s", hour, time.strftime('%H:%M:%S', time.gmtime(seconds_until_six_am)))
        time.sleep(seconds_until_six_am)
        seconds_until_six_am = (sleep_target - datetime.now()).total_seconds() + 1


def get_next_nearest_process_hour():
    now = datetime.now()
    run_hours:list[int] = HEALTHCHECK_TIMES_HOUR[:]
    run_hours.append(TARGET_RUN_TIME)
    run_hours.sort()
    for hour in run_hours:
        if now.hour >= hour:
            continue
        return hour
    return run_hours[0]

def sleep_until_next_nearest_process_hour() -> bool:
    next_nearest_process_hour = get_next_nearest_process_hour()
    sleep_until_time(next_nearest_process_hour)
    now = datetime.now()
    if next_nearest_process_hour == TARGET_RUN_TIME and now.weekday() < 5:
        logger.info("Client run time reached. Results will be sent to clients.")
        return False
    logger.info("Healthcheck run time reached. Results will not be sent to clients.")
    return True

def main():
    while True:
        try:
            healthcheck = sleep_until_next_nearest_process_hour()
            tickers:set[str] = read_funds_csv()
            ticker_queue = queue.Queue()
            for ticker in tickers:
                ticker_queue.put(ticker)
            progress:float = 0.0
            original_queue_size = ticker_queue.qsize()
            start_time:int = int(time.time())
            with Processor() as processor:
                processor.add_list_of_tickers(tickers)
                with Scraper(headless=True) as scraper:
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
                        if is_non_ticker(ticker):
                            logger.info("Skipping %s as it is not a valid ticker", ticker)
                            continue
                        if processor.has_ticker_been_processed(ticker):
                            logger.info("Skipping %s as it has already been processed", ticker)
                            continue
                        try:
                            logger.info("Processing %s", ticker)
                            ticker_type:TickerType = scraper.find_ticker(ticker_to_ms_ticker(ticker))
                            logger.info("Step 1/3 Complete - %s is a %s", ticker, ticker_type.value)
                            trailing_returns:TrailingReturns = scraper.get_trailing_returns(ticker_type) # THIS HAS TO BE BEFORE RATING ELSE RATING WILL RETURN FALSE POSITIVES
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
                data_controls_failures = check_data_controls(processor)
                processor.export_to_csv()
                failed_tickers = processor.get_failed_tickers()
                result_str = f"FundFinder Processing Completed at {datetime.now().strftime('%H:%M:%S')}"
                if len(failed_tickers) > 0 or len(data_controls_failures) > 0:
                    logger.info("The following tickers failed %s", failed_tickers)
                    if not healthcheck:
                        if len(failed_tickers) > 5:
                            logger.error("More than 5 tickers failed skipping sending to clients.")
                            send_email_with_results(f"{result_str}\n\nMore than 30 tickers failed skipping sending to clients: {failed_tickers}", [ADMIN_EMAIL])
                        else:
                            send_email_with_results(f"{result_str}", CLIENT_EMAILS)
                            send_email_with_results(f"{result_str}\n\nHealthcheck shows unhealthy for the following tickers: {failed_tickers}\nAnd the following controls failed: {data_controls_failures}", [ADMIN_EMAIL])
                    else:
                        send_email_with_results(f"{result_str}\n\nHealthcheck shows unhealthy for the following tickers: {failed_tickers}\nAnd the following controls failed: {data_controls_failures}", [ADMIN_EMAIL])
                else:
                    if not healthcheck:
                        send_email_with_results(result_str, CLIENT_EMAILS)
                    else:
                        logger.info("Healthcheck run shows healthy.")
            logger.info("Processing complete")
        except Exception as e:
            logger.exception("Error in main loop: %s", repr(e))
            send_email_with_results(f"SERVICE IS UNHEALTHY. Error: {repr(e)}", [ADMIN_EMAIL])

if __name__ == "__main__":
    main()
