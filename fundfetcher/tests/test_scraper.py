import logging
from time import time
import pytest

from enums.ticker_types import TickerType
from models.trailing_returns import TrailingReturns
from scraper.ms_scraper import Scraper
from tests.constants import TEST_ETF, TEST_FUND, TEST_STOCK
from enums.screener import ScreenerDownPresses
import csv
from time import sleep


@pytest.fixture(scope="module")
def scraper():
    login_scraper = Scraper()
    login_scraper.login()
    return login_scraper

def test_go_to_screener(scraper):
    
    print(scraper.get_number_of_negative_returns(scraper.find_ticker(TEST_ETF)))
    return
    fund_ratings = {}
    fund_to_scrape = [ScreenerDownPresses.ETF, ScreenerDownPresses.MUTUAL_FUND]
    for fund in fund_to_scrape:
        print(fund)
        scraper.go_to_screener(fund)
        print(scraper.get_screener_data())
        return
        do = True
        old_len = 0
        while do:
            new_funds = scraper.get_all_tickers_and_ratings()
            print("Intersection:", set(fund_ratings.keys()).intersection(set(new_funds.keys())))
            fund_ratings.update(new_funds)
            scraper.paginate_next()
            do = len(fund_ratings) != old_len
            old_len = len(fund_ratings)
            print(len(fund_ratings))

    print(fund_ratings)
    with open('fund_ratings.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Ticker', 'Rating'])
        for ticker, rating in fund_ratings.items():
            writer.writerow([ticker, rating])

# def test_scraper_initialization(scraper):
#     assert scraper is not None

# def test_scraper_find_ticker(scraper):
#     assert TickerType.STOCK == scraper.find_ticker(TEST_STOCK)
#     assert TickerType.STOCK == scraper.find_ticker("COST")
#     assert TickerType.MUTUAL_FUND == scraper.find_ticker(TEST_FUND)
#     assert TickerType.MUTUAL_FUND == scraper.find_ticker("FGTXX")
#     assert TickerType.ETF == scraper.find_ticker(TEST_ETF)

# def test_scraper_get_trailing_returns(scraper):
#     scraper.find_ticker(TEST_STOCK)
#     trailing_returns: TrailingReturns = scraper.get_trailing_returns(TickerType.STOCK)
#     validate_trailing_returns(trailing_returns)
#     assert trailing_returns.inception is None

#     scraper.find_ticker(TEST_FUND)
#     trailing_returns: TrailingReturns = scraper.get_trailing_returns(TickerType.MUTUAL_FUND)
#     validate_trailing_returns(trailing_returns)
#     assert trailing_returns.inception is not None

#     scraper.find_ticker(TEST_ETF)
#     trailing_returns: TrailingReturns = scraper.get_trailing_returns(TickerType.ETF)
#     validate_trailing_returns(trailing_returns)
#     assert trailing_returns.inception is not None

# def validate_trailing_returns(trailing_returns: TrailingReturns):
#     assert trailing_returns.ytd is not None
#     assert trailing_returns.one_year is not None
#     assert trailing_returns.three_year is not None
#     assert trailing_returns.five_year is not None
#     assert trailing_returns.ten_year is not None
#     assert trailing_returns.fifteen_year is not None

# def test_morningstar_rating_acquisition(scraper):
#     scraper.find_ticker(TEST_STOCK)
#     assert scraper.get_morningstar_rating(TickerType.STOCK) >= 1
#     scraper.find_ticker(TEST_FUND)
#     assert scraper.get_morningstar_rating(TickerType.MUTUAL_FUND) >= 1
#     scraper.find_ticker(TEST_ETF)
#     assert scraper.get_morningstar_rating(TickerType.ETF) >= 1
