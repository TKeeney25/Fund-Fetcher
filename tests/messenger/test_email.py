import pytest

from constants import ADMIN_EMAIL
from messenger.email import send_email_with_results

@pytest.mark.aws
def test_send_email_with_results():
    print(send_email_with_results("Test", [ADMIN_EMAIL]))
