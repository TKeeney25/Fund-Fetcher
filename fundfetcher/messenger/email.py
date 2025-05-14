from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3

from constants import EMAIL_SOURCE, OUTPUT_CSV_FILE, OUTPUT_CSV_FILE_PATH
from datetime import datetime

def send_email_with_results(body: str, recipients: list[str]):
    msg = MIMEMultipart()
    current_date = datetime.now().strftime("%b. %d, %Y")
    msg['Subject'] = f'FundFetcher Results: {current_date}'
    msg['From'] = EMAIL_SOURCE
    msg['To'] = str(recipients).replace('[', '').replace(']', '').replace("'", '')
    body = MIMEText(body, "plain")
    msg.attach(body)

    with open(OUTPUT_CSV_FILE_PATH, 'rb') as attachment:
        output_file = MIMEApplication(attachment.read())
        output_file.add_header('Content-Disposition', 'attachment', filename=OUTPUT_CSV_FILE)
        msg.attach(output_file)


    ses = boto3.client('ses')
    response = ses.send_raw_email(
        Source=EMAIL_SOURCE,
        Destinations=recipients,
        RawMessage={"Data": msg.as_bytes()}
    )
