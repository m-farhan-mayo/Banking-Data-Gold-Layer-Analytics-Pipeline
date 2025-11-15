import json
import csv
import io
import random
import time
import uuid
from datetime import datetime
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
from azure.core.credentials import AzureNamedKeyCredential

fake = Faker()

# -------------------------------
# Azure Event Hub Configuration
# -------------------------------
EVENTHUB_NAMESPACE = "kafka-namespace.servicebus.windows.net"
EVENTHUB_NAMES = ["customers-topic", "accounts-topic", "transactions-topic", "branches-topic"]
PRIMARY_KEY = "YOUR KEY"
credential = AzureNamedKeyCredential(name="RootManageSharedAccessKey", key=PRIMARY_KEY)

# -------------------------------
# Create producer clients
# -------------------------------
producers = {}
for hub_name in EVENTHUB_NAMES:
    producers[hub_name] = EventHubProducerClient(
        fully_qualified_namespace=EVENTHUB_NAMESPACE,
        eventhub_name=hub_name,
        credential=credential
    )

# -------------------------------
# Generate valid events
# -------------------------------
def generate_customer_event():
    event = {
        "CustomerID": f"CUST-{random.randint(1000,9999)}",
        "FirstName": fake.first_name(),
        "LastName": fake.last_name(),
        "Email": fake.email(),
        "Phone": fake.phone_number(),
        "Address": fake.address(),
        "City": fake.city(),
        "Country": fake.country(),
        "CreatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    return json.dumps(event)

def generate_account_event():
    event = {
        "account_id": f"ACC-{random.randint(1000,9999)}",
        "customer_id": f"CUST-{random.randint(1000,9999)}",
        "account_type": random.choice(["Savings", "Current", "Fixed"]),
        "balance": round(random.uniform(1000,50000),2),
        "currency": random.choice(["USD","EUR","PKR"]),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": random.choice(["active", "inactive", "blocked"])
    }
    return json.dumps(event)

def generate_transaction_event():
    event = {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC-{random.randint(1000,9999)}",
        "transaction_type": random.choice(["credit","debit"]),
        "amount": round(random.uniform(10,5000),2),
        "currency": random.choice(["USD","EUR","PKR"]),
        "transaction_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "description": fake.sentence()
    }
    return json.dumps(event)

def generate_branch_event():
    event = {
        "branch_id": f"BR-{random.randint(10,99)}",
        "branch_name": fake.company(),
        "address": fake.address(),
        "city": fake.city(),
        "country": fake.country(),
        "phone": fake.phone_number(),
        "manager_name": fake.name()
    }
    return json.dumps(event)

# -------------------------------
# Send events continuously
# -------------------------------
def send_events():
    print("🔥 Azure Event Hub Producer started...")
    while True:
        for topic in EVENTHUB_NAMES:
            batch = producers[topic].create_batch()
            if topic == "customers-topic":
                batch.add(EventData(generate_customer_event()))
            elif topic == "accounts-topic":
                batch.add(EventData(generate_account_event()))
            elif topic == "transactions-topic":
                batch.add(EventData(generate_transaction_event()))
            elif topic == "branches-topic":
                batch.add(EventData(generate_branch_event()))
            producers[topic].send_batch(batch)
        print("✔ Batch sent to all topics")
        time.sleep(random.uniform(0.5, 2))  # simulate streaming

if __name__ == "__main__":
    send_events()
