import requests
import csv
import os
import io
import smtplib
from azure.storage.blob import BlobServiceClient
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Use environment variables
api_key = os.environ['API_KEY']
azure_connection_string = os.environ['AZURE_CONNECTION_STRING']
container_name = 'ams'
blob_folder = 'Quick Stats'
blob_name = f'{blob_folder}/hemp_nass_data.csv'
gmail_user = os.environ['GMAIL_USER']
gmail_app_password = os.environ['GMAIL_APP_PASSWORD']
recipient = os.environ['RECIPIENT']

url = 'http://quickstats.nass.usda.gov/api/api_GET/'
params = {
    'key': api_key,
    'commodity_desc': 'HEMP',
    'year__GE': '2019',
    'format': 'JSON'
}

def send_notification_email(subject, body):
    msg = MIMEMultipart()
    msg['From'] = gmail_user
    msg['To'] = recipient
    msg['Subject'] = subject  # (fixed typo)

    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(gmail_user, gmail_app_password)
        server.sendmail(gmail_user, recipient, msg.as_string())
        server.quit()
        print('Email sent!')
    except Exception as e:
        print(f'Failed to send email: {e}')

response = requests.get(url, params=params)
if response.status_code == 200:
    data = response.json()
    records = data['data']

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=records[0].keys())
    writer.writeheader()
    writer.writerows(records)
    csv_content = output.getvalue().encode('utf-8')

    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_content, overwrite=True)

    print(f"Done! Uploaded {len(records)} hemp records to Azure blob: {container_name}/{blob_name}")

    subject = "Hemp NASS Data Upload Successful"
    body = f"""
    The hemp NASS data script has completed successfully.
    {len(records)} records were uploaded to Azure Blob Storage at: {container_name}/{blob_name}

    This is an automated notification, do not reply.
    """
    send_notification_email(subject, body)
else:
    error_subject = "Hemp NASS Data Script Failed"
    error_body = f"Request failed with status: {response.status_code}, message: {response.text}"
    send_notification_email(error_subject, error_body)
    print(error_body)
