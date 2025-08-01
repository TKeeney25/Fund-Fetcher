import logging
import os
import time

from sqlalchemy import Engine
from sqlmodel import Session, SQLModel, create_engine, select
import pandas as pd

from constants import MAX_PROCESSING_ATTEMPTS, OUTPUT_CSV_FILE_PATH, OUTPUT_XLSX_FILE_PATH
from database.models import Ticker
from models.screener_data import ScreenerData
from models.trailing_returns import TrailingReturns
# pylint: disable=C0121

logger = logging.getLogger(__name__)

class Processor():
    engine:Engine
    session:Session
    def __init__(self, in_memory:bool = False, reuse_db:bool = False):
        if in_memory:
            self.engine = create_engine('sqlite+pysqlite:///:memory:')
        else:
            if not reuse_db:
                if os.path.exists('database.db'):
                    os.remove('database.db')
            self.engine = create_engine('sqlite:///database.db')
        SQLModel.metadata.create_all(self.engine)

    def __enter__(self):
        self.session = Session(self.engine)
        return self

    def __exit__(self, *_):
        self.session.close()

    def has_ticker_been_processed(self, ticker: str) -> bool:
        statement = select(Ticker).where(Ticker.symbol == ticker).where(Ticker.processing_complete != None)
        return self.session.exec(statement).first() is not None
    
    def add_list_of_tickers(self, tickers: list[str]):
        logger.info("Adding %s tickers to database", len(tickers))
        for ticker_symbol in tickers:
            self.session.add(Ticker(symbol=ticker_symbol))
        self.session.commit()

    def add_trailing_returns(self, ticker: str, trailing_returns: TrailingReturns):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.return_ytd = trailing_returns.ytd
        ticker.return_1y = trailing_returns.one_year
        ticker.return_3y = trailing_returns.three_year
        ticker.return_5y = trailing_returns.five_year
        ticker.return_10y = trailing_returns.ten_year
        ticker.return_15y = trailing_returns.fifteen_year
        ticker.inception = trailing_returns.inception
        self.session.commit()

    def add_screener_data(self, ticker: str, screener_data: ScreenerData):
        ticker = Ticker(
            symbol=ticker,
            name=screener_data.name,
            category=screener_data.category,
            return_ytd=screener_data.return_ytd,
            return_1m=screener_data.return_1m,
            return_1y=screener_data.return_1y,
            return_3y=screener_data.return_3y,
            return_5y=screener_data.return_5y,
            return_10y=screener_data.return_10y,
            yield_ttm=screener_data.ttm_yield,
            twelve_b_one_fee=screener_data.twelve_b_one_fee,
            morningstar_rating=screener_data.morningstar_rating
        )
        self.session.add(ticker)
        self.judge_screener_data(ticker, screener_data)
        self.session.commit()
        if ticker.filter_failures is not None:
            self.mark_ticker_as_processed_unsuccessfully(ticker.symbol, Exception("Ticker failed filter"))

    def judge_screener_data(self, ticker: Ticker, screener_data: ScreenerData):
        if screener_data.twelve_b_one_fee is not None and screener_data.twelve_b_one_fee > 0:
            logger.warning("Ticker %s has a 12b-1 fee greater than 0", ticker.symbol)
            self.add_filter_failure(ticker, "12b-1 fee greater than 0")
        if screener_data.return_10y is None:
            logger.warning("Ticker %s has no 10y return", ticker.symbol)
            self.add_filter_failure(ticker, "No 10y return")
        elif screener_data.return_10y is not None and screener_data.return_10y <= 0:
            logger.warning("Ticker %s has a 10y return less than or equal to 0", ticker.symbol)
            self.add_filter_failure(ticker, "10y return less than or equal to 0")
        elif screener_data.return_5y is not None and screener_data.return_5y <= 0:
            logger.warning("Ticker %s has a 5y return less than or equal to 0", ticker.symbol)
            self.add_filter_failure(ticker, "5y return less than or equal to 0")
        elif screener_data.return_3y is not None and screener_data.return_3y <= 0:
            logger.warning("Ticker %s has a 3y return less than or equal to 0", ticker.symbol)
            self.add_filter_failure(ticker, "3y return less than or equal to 0")
        elif screener_data.return_1y is not None and screener_data.return_1y <= 0:
            logger.warning("Ticker %s has a 1y return less than or equal to 0", ticker.symbol)
            self.add_filter_failure(ticker, "1y return less than or equal to 0")
        if screener_data.ttm_yield is None:
            logger.warning("Ticker %s has no TTM yield", ticker.symbol)
            self.add_filter_failure(ticker, "No TTM yield")

    def add_filter_failure(self, ticker: Ticker, failure: str):
        if ticker.filter_failures is None:
            ticker.filter_failures = f"{failure}"
        else:
            ticker.filter_failures += f", {failure}"

    def get_non_filtered_tickers(self) -> list[str]:
        statement = select(Ticker).where(Ticker.filter_failures == None)
        tickers = self.session.exec(statement).all()
        return [ticker.symbol for ticker in tickers]
    
    def add_number_of_negative_years(self, ticker: str, negative_years: int):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.negative_years = negative_years
        self.session.commit()

    def add_morningstar_rating(self, ticker: str, rating: int):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.morningstar_rating = rating
        self.session.commit()

    def uncomplete_the_healthy_tickers(self):
        statement = select(Ticker).where(Ticker.processing_complete != None).where(Ticker.processing_error == None)
        tickers = self.session.exec(statement).all()
        for ticker in tickers:
            ticker.processing_complete = None
            ticker.processing_error = None
            ticker.processing_attempts = 0
            ticker.filter_failures = None
        self.session.commit()

    def redrive_dlq(self):
        statement = select(Ticker).where(Ticker.processing_complete != None).where(Ticker.processing_attempts == 4)
        tickers = self.session.exec(statement).all()
        for ticker in tickers:
            ticker.processing_complete = None
            ticker.processing_error = None
            ticker.processing_attempts = 0
            ticker.filter_failures = None
        self.session.commit()

    def handle_processing_error(self, ticker: str, error: Exception):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.processing_error = repr(error)
        if ticker.processing_attempts >= MAX_PROCESSING_ATTEMPTS:
            logger.error("Exceeded maximum processing attempts for %s", ticker)
            ticker.processing_complete = int(time.time())
        ticker.processing_attempts += 1
        self.session.commit()

    def mark_ticker_as_processed_unsuccessfully(self, ticker: str, error: Exception):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.processing_complete = int(time.time())
        ticker.processing_error = repr(error)
        self.session.commit()

    def mark_ticker_as_processed_successfully(self, ticker: str):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.processing_complete = int(time.time())
        ticker.processing_error = None
        self.session.commit()

    def get_failed_tickers(self) -> list[str]:
        statement = select(Ticker).where(Ticker.processing_error != None)
        tickers = self.session.exec(statement).all()
        return [ticker.symbol for ticker in tickers]
    
    def get_filtered_tickers(self) -> list[str]:
        statement = select(Ticker).where(Ticker.filter_failures != None)
        tickers = self.session.exec(statement).all()
        return [ticker.symbol for ticker in tickers]
    
    def get_unfinished_tickers(self) -> list[str]:
        statement = select(Ticker).where(Ticker.processing_complete == None)
        tickers = self.session.exec(statement).all()
        return [ticker.symbol for ticker in tickers]
    
    def export_to_ticker_tracker_xlsx(self):
        statement = select(Ticker).where(Ticker.processing_error == None)
        tickers = self.session.exec(statement).all()
        # TODO handle commas in names
        os.makedirs(os.path.dirname(OUTPUT_CSV_FILE_PATH), exist_ok=True)
        os.makedirs(os.path.dirname(OUTPUT_XLSX_FILE_PATH), exist_ok=True)
        with open(OUTPUT_CSV_FILE_PATH, 'w', encoding="utf-8") as file:
            file.write("Ticker,Name,Category,YTD Return,1 month,1 year,3 year,5 year,10 year,yield,Number of Negative Years (Within Past 10 Years)\n")
            for ticker in tickers:
                file.write(f"{ticker.symbol},{ticker.name},{ticker.category},{ticker.return_ytd},{ticker.return_1m},"
                            f"{ticker.return_1y},{ticker.return_3y},{ticker.return_5y},{ticker.return_10y},"
                            f"{ticker.yield_ttm},{ticker.negative_years}\n")
        read_file = pd.read_csv(OUTPUT_CSV_FILE_PATH)
        read_file.to_excel(OUTPUT_XLSX_FILE_PATH, index=False)
    
    def export_to_csv(self):
        statement = select(Ticker)
        tickers = self.session.exec(statement).all()
        symbols_row_string = 'symbol'
        ytd_row_string = 'ytd'
        one_year_row_string = 'oneYear'
        three_year_row_string = 'threeYear'
        five_year_row_string = 'fiveYear'
        ten_year_row_string = 'tenYear'
        fifteen_year_row_string = 'fifteenYear'
        inception_row_string = 'inception'
        star_rating_row_string = 'starRating'
        for ticker in tickers:
            symbols_row_string += f',{ticker.symbol}'
            ytd_row_string += f',{self._csv_cell(ticker.return_ytd)}'
            one_year_row_string += f',{self._csv_cell(ticker.return_1y)}'
            three_year_row_string += f',{self._csv_cell(ticker.return_3y)}'
            five_year_row_string += f',{self._csv_cell(ticker.return_5y)}'
            ten_year_row_string += f',{self._csv_cell(ticker.return_10y)}'
            fifteen_year_row_string += f',{self._csv_cell(ticker.return_15y)}'
            inception_row_string += f',{self._csv_cell(ticker.inception)}'
            star_rating_row_string += f',{self._csv_cell(ticker.morningstar_rating)}'

        os.makedirs(os.path.dirname(OUTPUT_CSV_FILE_PATH), exist_ok=True)
        with open(OUTPUT_CSV_FILE_PATH, 'w', encoding="utf-8") as csv:
            csv.write(f'{symbols_row_string}\n')
            csv.write(f'{ytd_row_string}\n')
            csv.write(f'{one_year_row_string}\n')
            csv.write(f'{three_year_row_string}\n')
            csv.write(f'{five_year_row_string}\n')
            csv.write(f'{ten_year_row_string}\n')
            csv.write(f'{fifteen_year_row_string}\n')
            csv.write(f'{inception_row_string}\n')
            csv.write(f'{star_rating_row_string}\n')

    @staticmethod
    def _csv_cell(value: float | int | None) -> str:
        if value is None:
            return ''
        return str(value)

