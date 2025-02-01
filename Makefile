init:
	pipenv install --dev

test:
	pipenv run pytest

playwright:
	playwright install chromium