import pytest
from sqlmodel import select

from fundfetcher.constants import MAX_PROCESSING_ATTEMPTS
from fundfetcher.database.models import Ticker
from fundfetcher.database.query_processor import Processor
from fundfetcher.tests.constants import TEST_ETF, TEST_FUND, TEST_STOCK, TICKERS_LIST

@pytest.fixture
def processor() -> Processor:
    test_processor = Processor(in_memory=True)
    test_processor.__enter__()
    return test_processor

@pytest.fixture
def tickers(processor):
    processor.add_list_of_tickers(TICKERS_LIST)
    return TICKERS_LIST

def test_add_list_of_tickers(processor, tickers):
    for ticker in tickers:
        statement = select(Ticker).where(Ticker.symbol == ticker)
        assert processor.session.exec(statement).first() is not None

def test_handle_processing_error(processor, tickers):
    ticker = tickers[0]
    processor.handle_processing_error(ticker, Exception("Test Error"))
    statement = select(Ticker).where(Ticker.symbol == ticker)
    ticker_object:Ticker = processor.session.exec(statement).first()
    assert ticker_object.processing_error is not None
    assert ticker_object.processing_attempts == 1

def test_handle_many_processing_errors(processor, tickers):
    ticker = tickers[0]
    for _ in range(0, MAX_PROCESSING_ATTEMPTS+1):
        processor.handle_processing_error(ticker, Exception("Test Error"))
    statement = select(Ticker).where(Ticker.symbol == ticker)
    ticker_object:Ticker = processor.session.exec(statement).first()
    assert ticker_object.processing_error is not None
    assert ticker_object.processing_attempts == MAX_PROCESSING_ATTEMPTS+1
    assert ticker_object.processing_complete is not None

def test_mark_ticker_as_processed_unsuccessfully(processor, tickers):
    ticker = tickers[0]
    processor.mark_ticker_as_processed_unsuccessfully(ticker, Exception("Test Error"))
    statement = select(Ticker).where(Ticker.symbol == ticker)
    ticker_object:Ticker = processor.session.exec(statement).first()
    assert ticker_object.processing_error is not None
    assert ticker_object.processing_complete is not None

def test_mark_ticker_as_processed_successfully(processor, tickers):
    ticker = tickers[0]
    processor.mark_ticker_as_processed_successfully(ticker)
    statement = select(Ticker).where(Ticker.symbol == ticker)
    ticker_object:Ticker = processor.session.exec(statement).first()
    assert ticker_object.processing_error is None
    assert ticker_object.processing_complete is not None

def test_processor_has_ticker_been_processed(processor, tickers):
    ticker = tickers[0]
    assert not processor.has_ticker_been_processed(ticker)
    processor.mark_ticker_as_processed_successfully(ticker)
    assert processor.has_ticker_been_processed(ticker)