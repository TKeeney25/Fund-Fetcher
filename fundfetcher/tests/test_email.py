import pytest

from fundfetcher.messenger.email import send_email_with_results

def test_send_email_with_results():
    send_email_with_results("Test")
