# pylint: disable=W0212

import time
import unittest

from fundfetcher.scraper import auth

class TestAuth(unittest.TestCase):
    def test_get_bearer(self):
        self.assertIn('Bearer ', auth.get_bearer())

    def test_update_bearer(self):
        auth.get_bearer()

        self.assertIn('Bearer ', auth._session_bearer)
        self.assertLessEqual(abs(auth._last_update - time.time()), 1)

    # def test_auto_update_bearer(self):
    #     first_token = auth.get_bearer()
    #     second_token = auth.get_bearer()
    #     self.assertEqual(first_token, second_token)

    #     auth._last_update = 0
    #     third_token = auth.get_bearer()

    #     self.assertNotEqual(first_token, third_token)

if __name__ == '__main__':
    unittest.main()