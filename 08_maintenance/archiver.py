from google.cloud import pubsub_v1
from google.cloud import storage
from google.oauth2 import service_account
import json
import uuid

# GCP Configuration Details
generated_project_id = 'de-activity-420606'
generated_subscription_name = 'sub1'
generated_service_account_key_path = 'C:\\Users\\KAMINENI MONIKA\\Downloads\\de-activity-420606-549bd118addd.json'
generated_bucket_name = 'my_buck_et'

class DataProcessor:
    def __init__(self, project_id, subscription_name, service_account_key_path, bucket_name):
        self.project_id = project_id
        self.subscription_name = subscription_name
        self.service_account_key_path = service_account_key_path
        self.bucket_name = bucket_name
        self.pubsub_client = None
        self.storage_client = None
        self.bucket = None

    def initialize_clients(self):
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_key_path,
            scopes=["https://www.googleapis.com/auth/pubsub", "https://www.googleapis.com/auth/cloud-platform"]
        )
        self.pubsub_client = pubsub_v1.SubscriberClient(credentials=credentials)
        self.storage_client = storage.Client(credentials=credentials)
        self.bucket = self.storage_client.bucket(self.bucket_name)

    def create_subscription_path(self):
        return self.pubsub_client.subscription_path(self.project_id, self.subscription_name)

    def process_message(self, message):
        try:
            data = json.loads(message.data.decode('utf-8'))
            message_id = str(uuid.uuid4())
            blob_name = f"data_{message_id}.json"
            blob = self.bucket.blob(blob_name)
            blob.upload_from_string(json.dumps(data))
            print(f"Message successfully stored in Cloud Storage as {blob.name}")
            message.ack()
        except Exception as e:
            print(f"Error processing message: {e}")
            message.nack()

    def subscribe_to_topic(self):
        subscription_path = self.create_subscription_path()

        def callback(message):
            self.process_message(message)

        streaming_pull_future = self.pubsub_client.subscribe(subscription_path, callback=callback)
        print(f"Now listening for incoming messages on subscription: {subscription_path}...\n")
        with self.pubsub_client:
            try:
                streaming_pull_future.result()
            except Exception as e:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
                raise e

def execute_data_processing():
    processor = DataProcessor(generated_project_id, generated_subscription_name, generated_service_account_key_path, generated_bucket_name)
    processor.initialize_clients()
    processor.subscribe_to_topic()

if __name__ == "__main__":
    execute_data_processing()
