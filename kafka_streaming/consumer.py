import pyodbc
import threading
import json
from azure.eventhub import EventHubConsumerClient
from azure.core.credentials import AzureNamedKeyCredential

# -------------------------------
# Azure Event Hub Configuration
# -------------------------------
EVENTHUB_NAMESPACE = "kafka-namespace.servicebus.windows.net"
EVENTHUB_NAMES = ["customers-topic", "accounts-topic", "transactions-topic", "branches-topic"]
CONSUMER_GROUP = "$Default"
PRIMARY_KEY = "YOUR KEY"
credential = AzureNamedKeyCredential(name="RootManageSharedAccessKey", key=PRIMARY_KEY)

# -------------------------------
# SQL Server Configuration
# -------------------------------
SQL_SERVER = "SERVER-NAME.database.windows.net"
SQL_DB = "DATABASENAME"
SQL_USERNAME = "USERNAME"
SQL_PASSWORD = "PASSWORD"
CONN_STR = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={SQL_SERVER};Database={SQL_DB};Uid={SQL_USERNAME};Pwd={SQL_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)

# -------------------------------
# SQL Server Table Creation Queries
# -------------------------------
TABLES = {
    "customers": """
        IF OBJECT_ID('customers', 'U') IS NULL
        BEGIN
            CREATE TABLE customers (
                CustomerID NVARCHAR(MAX) NULL,
                FirstName NVARCHAR(MAX) NULL,
                LastName NVARCHAR(MAX) NULL,
                Email NVARCHAR(MAX) NULL,
                Phone NVARCHAR(MAX) NULL,
                Address NVARCHAR(MAX) NULL,
                City NVARCHAR(MAX) NULL,
                Country NVARCHAR(MAX) NULL,
                CreatedAt NVARCHAR(MAX) NULL,
                raw_data NVARCHAR(MAX) NULL
            );
        END
    """,
    "accounts": """
        IF OBJECT_ID('accounts', 'U') IS NULL
        BEGIN
            CREATE TABLE accounts (
                account_id NVARCHAR(MAX) NULL,
                customer_id NVARCHAR(MAX) NULL,
                account_type NVARCHAR(MAX) NULL,
                balance NVARCHAR(MAX) NULL,
                currency NVARCHAR(MAX) NULL,
                created_at NVARCHAR(MAX) NULL,
                status NVARCHAR(MAX) NULL,
                raw_data NVARCHAR(MAX) NULL
            );
        END
    """,
    "transactions": """
        IF OBJECT_ID('transactions', 'U') IS NULL
        BEGIN
            CREATE TABLE transactions (
                transaction_id NVARCHAR(MAX) NULL,
                account_id NVARCHAR(MAX) NULL,
                transaction_type NVARCHAR(MAX) NULL,
                amount NVARCHAR(MAX) NULL,
                currency NVARCHAR(MAX) NULL,
                transaction_date NVARCHAR(MAX) NULL,
                description NVARCHAR(MAX) NULL,
                raw_data NVARCHAR(MAX) NULL
            );
        END
    """,
    "branches": """
        IF OBJECT_ID('branches', 'U') IS NULL
        BEGIN
            CREATE TABLE branches (
                branch_id NVARCHAR(MAX) NULL,
                branch_name NVARCHAR(MAX) NULL,
                address NVARCHAR(MAX) NULL,
                city NVARCHAR(MAX) NULL,
                country NVARCHAR(MAX) NULL,
                phone NVARCHAR(MAX) NULL,
                manager_name NVARCHAR(MAX) NULL,
                raw_data NVARCHAR(MAX) NULL
            );
        END
    """
}

# -------------------------------
# Ensure Tables Exist
# -------------------------------
def ensure_tables():
    try:
        with pyodbc.connect(CONN_STR, autocommit=True) as conn:
            cursor = conn.cursor()
            for name, query in TABLES.items():
                try:
                    cursor.execute(query)
                    print(f"✔ Ensured table exists: {name}")
                except Exception as e:
                    print(f"❌ Error creating table {name}: {e}")
    except Exception as e:
        print(f"❌ SQL Connection Error during table creation: {e}")

# -------------------------------
# Insert Raw Data
# -------------------------------
def insert_raw(table_name, data_dict):
    try:
        with pyodbc.connect(CONN_STR, autocommit=True) as conn:
            cursor = conn.cursor()
            columns = ', '.join(data_dict.keys())
            placeholders = ', '.join(['?'] * len(data_dict))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            params = tuple(str(v) if v is not None else None for v in data_dict.values())
            cursor.execute(sql, params)
            print(f"✔ Inserted record into {table_name}")
    except Exception as e:
        print(f"❌ SQL Execution Error in {table_name}: {e}")

# -------------------------------
# Topic-specific insert functions
# -------------------------------
def insert_customer(data):
    insert_raw("customers", data)

def insert_account(data):
    insert_raw("accounts", data)

def insert_transaction(data):
    insert_raw("transactions", data)

def insert_branch(data):
    insert_raw("branches", data)

# -------------------------------
# Event Hub Handler
# -------------------------------
def on_event(partition_context, event):
    body = event.body_as_str()
    if not body or body.strip() == "":
        return

    topic = partition_context.eventhub_name
    try:
        if topic in ["customers-topic", "transactions-topic"]:
            try:
                data = json.loads(body)
            except:
                data = {"raw_data": body}
        else:
            data = {"raw_data": body}

        if topic == "customers-topic":
            insert_customer(data)
        elif topic == "accounts-topic":
            insert_account(data)
        elif topic == "transactions-topic":
            insert_transaction(data)
        elif topic == "branches-topic":
            insert_branch(data)

    except Exception as e:
        print(f"❌ Consumer Error: {e}")

# -------------------------------
# Start Consumer Threads
# -------------------------------
def start_consumer(hub_name):
    client = EventHubConsumerClient(
        fully_qualified_namespace=EVENTHUB_NAMESPACE,
        eventhub_name=hub_name,
        consumer_group=CONSUMER_GROUP,
        credential=credential
    )
    with client:
        client.receive(on_event=on_event, starting_position="-1")

# -------------------------------
# Main
# -------------------------------
if __name__ == "__main__":
    ensure_tables()

    threads = []
    for hub in EVENTHUB_NAMES:
        thread = threading.Thread(target=start_consumer, args=(hub,))
        thread.start()
        threads.append(thread)

    print("🔥 Event Hub Consumer started — listening to all topics...")

    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("🛑 Consumer stopped by user")
