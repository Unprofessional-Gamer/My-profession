import time
import datetime
import smtplib
from email.mime.text import MIMEText
from google.cloud import storage

def send_email_notification(sender_email, recipient_email, subject, message):
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = recipient_email

    # Configure SMTP server
    smtp_server = smtplib.SMTP('smtp.example.com', 587)  # Update with your SMTP server details
    smtp_server.starttls()
    smtp_server.login('your_username', 'your_password')  # Update with your SMTP credentials
    smtp_server.send_message(msg)
    smtp_server.quit()

def monitor_bucket(bucket_name, folder_path, sender_email, recipient_email):
    checking_number = 1  # Initialize checking number
    print(f"Monitoring folder '{folder_path}' in bucket '{bucket_name}' for new files...")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Keep track of files seen so far
    seen_files = set()

    while True:
        print(f"Checking #{checking_number} - {datetime.datetime.now()}")  # Print checking number and current date/time
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            if blob.name not in seen_files:
                print(f"New file detected: {blob.name}")
                # Send email notification
                subject = f"New file detected: {blob.name}"
                message = f"New file '{blob.name}' detected in folder '{folder_path}' of bucket '{bucket_name}'."
                send_email_notification(sender_email, recipient_email, subject, message)
                # Handle the new file here, e.g., process it, move it, etc.
                seen_files.add(blob.name)

        # Increment checking number
        checking_number += 1
        
        # Sleep for a certain interval before checking again
        time.sleep(60)  # Poll every 60 seconds

def main():
    # Replace 'raw-zone-bucket' and 'folder/path' with your bucket name and folder path
    monitor_bucket('raw-zone-bucket', 'folder/path', 'sender@example.com', 'recipient@example.com')

if __name__ == "__main__":
    main()
