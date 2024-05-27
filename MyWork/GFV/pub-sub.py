from google.cloud import pubsub_v1, storage
import google.auth

# Initialize variables
project_id = 'YOUR_PROJECT_ID'
bucket_name = 'rawzone'
folder_prefix = 'cphpi/received/monthly/'
topic_name = 'monitor-monthly-folder'
subscription_name = 'monitor-monthly-folder-sub'

# Authenticate using the default service account
credentials, project_id = google.auth.default()

# Create a Pub/Sub topic
publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_name)

try:
    publisher.create_topic(request={"name": topic_path})
    print(f"Topic created: {topic_path}")
except Exception as e:
    print(f"Topic creation failed or already exists: {e}")

# Create a Pub/Sub subscription
subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path(project_id, subscription_name)

try:
    subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path}
    )
    print(f"Subscription created: {subscription_path}")
except Exception as e:
    print(f"Subscription creation failed or already exists: {e}")

# Set up Cloud Storage notification to publish to the Pub/Sub topic
client = storage.Client(credentials=credentials, project=project_id)
bucket = client.bucket(bucket_name)

# Define the notification configuration
notification = bucket.notification(
    topic_name,
    custom_attributes={'eventType': 'OBJECT_FINALIZE'},
    payload_format='JSON_API_V1',
)

try:
    notification.create()
    print(f"Notification created for bucket {bucket_name} on folder {folder_prefix}")
except Exception as e:
    print(f"Notification creation failed: {e}")

print("Setup complete.")