import logging
import os
import re
import time

from sqlalchemy import Engine
from sqlmodel import Session, SQLModel, create_engine, select

from constants import MAX_PROCESSING_ATTEMPTS, OUTPUT_CSV_FILE_PATH
from database.models import Ticker
from models.trailing_returns import TrailingReturns

# pylint: disable=C0121

logger = logging.getLogger(__name__)

class Processor():
    engine:Engine
    session:Session
    reuse_db:bool
    def __init__(self, in_memory:bool = False, reuse_db:bool = False):
        self.reuse_db = reuse_db
        if in_memory:
            self.engine = create_engine('sqlite+pysqlite:///:memory:')
        else:
            self.engine = create_engine('sqlite:///database.db')
        SQLModel.metadata.create_all(self.engine)

    def __enter__(self):
        self.session = Session(self.engine)
        if not self.reuse_db:
            self.clear_database()
        return self

    def __exit__(self, *_):
        self.session.close()

    def clear_database(self):
        logger.info("Clearing database")
        statement = select(Ticker)
        tickers = self.session.exec(statement).all()
        for ticker in tickers:
            self.session.delete(ticker)
        self.session.commit()

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

    def add_morningstar_rating(self, ticker: str, rating: int):
        statement = select(Ticker).where(Ticker.symbol == ticker)
        ticker:Ticker = self.session.exec(statement).first()
        ticker.morningstar_rating = rating
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

