# UPI Real-Time Transaction Processing & Fraud Detection

A real-time UPI transaction processing and fraud detection pipeline built using **Python, Apache Kafka, PostgreSQL, and Tableau Public**.

The project simulates UPI transactions, streams them through Kafka, validates and stores them in PostgreSQL, detects suspicious transaction patterns, and visualizes transaction activity through a Tableau dashboard "https://public.tableau.com/views/upi_tran_dashboard/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link".

---

## 1. Project Overview

This project demonstrates how a real-time financial transaction monitoring pipeline can be designed using modern data engineering technologies.

The system:

- Generates simulated UPI transactions using Python.
- Streams transactions through Apache Kafka.
- Processes and validates incoming events using Python.
- Stores valid transactions in PostgreSQL.
- Detects potentially fraudulent transactions using predefined rules.
- Sends invalid messages to a Dead Letter Queue (DLQ).
- Publishes fraud alerts to a separate Kafka topic.
- Stores fraud alerts in PostgreSQL.
- Visualizes transaction metrics using Tableau Public.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Transaction generation and processing |
| Apache Kafka | Real-time event streaming |
| confluent-kafka | Python Kafka producer and consumer |
| Docker | Running Kafka locally |
| PostgreSQL | Transaction and fraud-alert storage |
| psycopg2 | Python–PostgreSQL connectivity |
| Faker | Generating simulated UPI transactions |
| Tableau Public | Dashboard and visualization |

## 3. Architecture Diagram

```text
                  UPI TRANSACTION PRODUCER
                           |
                           |
                     Python + Faker
                           |
                           ▼
                  +------------------+
                  |   Apache Kafka   |
                  |                  |
                  | transactions     |
                  +------------------+
                           |
                           ▼
                  +------------------+
                  |  Python Consumer |
                  +------------------+
                           |
                           ▼
                   Transaction Validation
                           |
                   +-------+-------+
                   |               |
                Invalid           Valid
                   |               |
                   ▼               ▼
             +-----------+   +----------------+
             | Dead      |   | Fraud Detection|
             | Letter    |   |     Engine     |
             | Queue     |   +----------------+
             +-----------+           |
                                     |
                         +-----------+-----------+
                         |           |           |
                         ▼           ▼           ▼
                      Amount     Transaction   Different
                      > ₹50K     Velocity      Cities
                         |           |           |
                         +-----------+-----------+
                                     |
                                     ▼
                              +-------------+
                              | Kafka Topic |
                              |   alerts    |
                              +-------------+
                                     |
                                     ▼
                              +-------------+
                              | PostgreSQL  |
                              |             |
                              | transactions|
                              | fraud_alerts|
                              +-------------+
                                     |
                                     ▼
                              +-------------+
                              | SQL Views   |
                              +-------------+
                                     |
                                     ▼
                              +-------------+
                              | Tableau     |
                              | Public      |
                              +-------------+
                                     |
                                     ▼
                              UPI MONITORING
                                DASHBOARD
```

## 4. Fraud Detection Rules

The system checks incoming transactions against three fraud rules.

### Rule 1: High Transaction Amount

Flag a transaction when:

```python
amount > 50000
```

Example:

```json
{
    "txn_id": "TXN1001",
    "sender": "user1@upi",
    "amount": 65000
}
```

Expected result: Fraud alert.

### Rule 2: Transaction Velocity

Flag a transaction when a sender makes more than 5 transactions within 60 seconds.

Example:

```text
Sender: user1@upi

Transaction 1
Transaction 2
Transaction 3
Transaction 4
Transaction 5
Transaction 6  <-- FRAUD ALERT
```

### Rule 3: Different Transaction Cities

Flag a transaction when the same sender makes transactions from two different cities within 2 minutes.

Example:

```text
Sender: user1@upi

Transaction 1: Mumbai
Transaction 2: Pune

Result: FRAUD ALERT
```

**Note:** The current implementation maintains recent transaction history in consumer memory. Restarting the consumer resets this history.

## 5. Kafka Topics

The project uses three Kafka topics.

| Topic | Purpose |
|---|---|
| transactions | Incoming UPI transactions |
| alerts | Transactions flagged by fraud detection |
| dead-letter | Invalid messages and processing errors |

Transactions use the sender as their Kafka message key:

```python
producer.produce(
    "transactions",
    key=transaction["sender"],
    value=json.dumps(transaction)
)
```

This helps route transactions from the same sender to the same partition, preserving their order within that partition.

## 6. PostgreSQL Database

Database name:

```text
upi_db
```

### Transactions Table

```sql
CREATE TABLE transactions (
    txn_id VARCHAR(100) PRIMARY KEY,
    sender VARCHAR(255) NOT NULL,
    receiver VARCHAR(255) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL
);
```

### Fraud Alerts Table

```sql
CREATE TABLE fraud_alerts (
    txn_id VARCHAR(100) PRIMARY KEY,
    sender VARCHAR(255) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    city VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    fraud_reasons JSONB NOT NULL
);
```

### SQL Views for Tableau

The project uses SQL views to prepare data for visualization.

| View | Description |
|---|---|
| vw_transactions_per_minute | Transactions grouped by minute |
| vw_top_senders | Top 10 senders by transaction count |
| vw_flagged_transactions | Total flagged transactions |
| vw_failure_rate | Failed transaction percentage |

## 7. Tableau Dashboard

The Tableau dashboard provides an overview of transaction activity and potential fraud.

### Dashboard Components

**1. Transactions per Minute**
- Line chart.
- Displays transaction volume over time.

**2. Top 10 Senders**
- Horizontal bar chart.
- Displays senders with the highest transaction counts.

**3. Fraud Alerts**
- KPI card.
- Displays the total number of flagged transactions.

**4. Transaction Failure Rate**
- KPI card.
- Displays the percentage of failed transactions.

### Data Flow to Tableau

```text
PostgreSQL
    |
    ▼
SQL Views
    |
    ▼
CSV Export
    |
    ▼
Tableau Public
    |
    ▼
UPI Monitoring Dashboard
```

Tableau Public is used with CSV exports because the PostgreSQL connector may not be available in the installed Tableau Public application.

**Important:** Tableau Public dashboards and published data are publicly accessible. Use simulated data only.

## 8. Project Setup and Installation

### Prerequisites

Install the following:

- Python 3.12
- Docker Desktop
- PostgreSQL
- Tableau Public Desktop
- Git

### Step 1: Clone the Repository

```powershell
git clone <YOUR_GITHUB_REPOSITORY_URL>
```

Navigate to the project:

```powershell
cd kafka26
```

### Step 2: Create a Virtual Environment

```powershell
python -m venv .venv
```

Activate it:

```powershell
.venv\Scripts\activate
```

### Step 3: Install Dependencies

```powershell
pip install confluent-kafka psycopg2-binary Faker
```

Alternatively, if a requirements file is available:

```powershell
pip install -r requirements.txt
```

### Step 4: Start Kafka Using Docker

Start Docker Desktop.

From the directory containing your Docker Compose file, execute:

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

### Step 5: Create Kafka Topics

Create the transactions topic:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Create the alerts topic:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Create the dead-letter topic:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic dead-letter --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify topics:

```powershell
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 6: Configure PostgreSQL

Create the database:

```sql
CREATE DATABASE upi_db;
```

Connect to `upi_db` and execute the table creation SQL from Section 6.

Create the four SQL views described in that section.

### Step 7: Configure Database Credentials

Set your PostgreSQL connection configuration in your Python scripts.

```python
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "upi_db"
DB_USER = "postgres"
DB_PASSWORD = "YOUR_POSTGRES_PASSWORD"
```

Do not commit actual database credentials to GitHub.

### Step 8: Run the Transaction Producer

Open a terminal and activate the virtual environment.

Run:

```powershell
python upi_producer.py
```

The producer generates simulated UPI transactions and publishes them to Kafka.

### Step 9: Run the Transaction Consumer

Open another terminal.

Activate the virtual environment and execute:

```powershell
python upi_consumer.py
```

The consumer:

1. Reads transactions from Kafka.
2. Validates incoming JSON.
3. Sends invalid messages to the dead-letter topic.
4. Stores valid transactions in PostgreSQL.
5. Checks fraud detection rules.
6. Publishes flagged transactions to the alerts topic.
7. Stores fraud alerts in PostgreSQL.

### Step 10: Export Data for Tableau

Export the SQL views to CSV files using PostgreSQL.

Example:

```sql
COPY (
    SELECT * FROM vw_transactions_per_minute
)
TO 'C:/temp/transactions_per_minute.csv'
WITH (FORMAT CSV, HEADER);
```

Repeat for the remaining views.

In Tableau Public Desktop:

1. Select **Text File**.
2. Import the exported CSV files.
3. Create the four worksheets.
4. Assemble the dashboard.
5. Publish the dashboard to Tableau Public.

## 9. Project Folder Structure

```text
kafka26/
│
├── .venv/
│
├── docker-compose.yml
│
├── upi_producer.py
├── upi_consumer.py
├── db_test.py
│
├── requirements.txt
├── README.md
├── .gitignore
│
└── dashboard/
    ├── transactions_per_minute.csv
    ├── top_senders.csv
    ├── flagged_transactions.csv
    └── failure_rate.csv
```

Adjust the filenames and folder structure to match your actual repository.

## 10. Testing

Test the following scenarios:

| Test Case | Expected Result |
|---|---|
| Normal transaction | Stored in PostgreSQL |
| Invalid JSON | Sent to dead-letter |
| Amount above ₹50,000 | Fraud alert |
| Sixth transaction within 60 seconds | Fraud alert |
| Different cities within 2 minutes | Fraud alert |
| Failed transaction | Failure rate increases |

## 11. Future Improvements

Potential improvements include:

- Implement persistent fraud detection state.
- Add Redis for distributed transaction tracking.
- Build a real-time fraud monitoring dashboard.
- Add machine learning-based fraud detection.
- Implement Kafka consumer lag monitoring.
- Add transaction geolocation analysis.
- Deploy the pipeline using cloud infrastructure.
- Implement automated Tableau data refresh.

## 12. Learning Outcomes

Through this project, the following concepts are demonstrated:

- Real-time data streaming with Apache Kafka.
- Kafka producers, consumers, partitions, and consumer groups.
- Python-based event processing.
- PostgreSQL database integration.
- Data validation and error handling.
- Dead Letter Queue implementation.
- Rule-based fraud detection.
- SQL aggregation and analytical views.
- Data visualization using Tableau.

---

**Project:** UPI Real-Time Transaction Processing & Fraud Detection

**Technologies:** Python | Apache Kafka | Docker | PostgreSQL | Tableau Public

**Purpose:** Data Engineering and Real-Time Fraud Detection Learning Project.