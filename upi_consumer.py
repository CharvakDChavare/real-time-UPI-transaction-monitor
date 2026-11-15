import json
from datetime import datetime

import psycopg2 
from confluent_kafka import Consumer, Producer, KafkaException

from collections import defaultdict, deque
from datetime import datetime, timedelta

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

SOURCE_TOPIC = "transactions"
DLQ_TOPIC = "dead-letter"
ALERTS_TOPIC = "alerts"

GROUP_ID = "transaction-postgres-consumer-test"


DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "upi_db"
DB_USER =  "postgres"
DB_PASSWORD = "Matrix@2026"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit":False
})

# dlq product
dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

# alerts product
alerts_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

conn = psycopg2.connect(
    host="localhost",
           port=5432,
           database="upi_db",
           user="postgres",
           password="Matrix@2026"
)

cursor = conn.cursor()
print("Connected to PostgreSql:", DB_NAME)

def validate_transactions(txn):

    required_filed = [

        "txn_id",
        "sender",
        "receiver",
        "amount",
        "city",
        "timestamp",
        "status"
    ]

    for filed in required_filed:

        if filed not in txn:
            return False, f"Missing field: {filed}"

        if not isinstance(txn["amount"], (int, float)):
            return False, "Amount Must be a number"

        if txn["amount"] <=0:
            return False, "Amount must be greater than 0"

        valid_statuses = [
            "SUCCESS",
            "FAILED",
            "PENDING"
        ]

        if txn["status"] not in valid_statuses:
            return False, f"Invalid status: {txn['status']}"

        try:
            datetime.fromisoformat(
                txn["timestamp"].replace("z", "+00:00")
            )

        except Exception:
            return False, "Invalid timestamp"

        return True, "Valid"

def insert_transaction(txn):

    transaction_time = datetime.fromisoformat(
        txn["timestamp"].replace("z", "+00:00")
    )

    query = """

            INSERT INTO transactions
            (
            txn_id,
            sender,
            receiver,
            amount,
            city,
            timestamp,
            status
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (txn_id) DO NOTHING
            """

    cursor.execute(

        query,
        (
            txn["txn_id"],
            txn["sender"],
            txn["receiver"],
            txn["amount"],
            txn["city"],
            transaction_time,
            txn["status"]
        )
    )

    conn.commit()

def send_to_dead_letter(message, reason):

    dlq_message = {
        "original_message": message,
        "error": reason
    }

    dlq_producer.produce(

        DLQ_TOPIC,
        key= message.get("txn_id","unknown"),
        value= json.dumps(dlq_message)
    )

    dlq_producer.flush()

    print(
        f" sent to dead-letter topic | "
        f"Reason: {reason}")


sender_transactions = defaultdict(deque)
# The fraud check function

def check_fraud(transaction):
    sender = transaction["sender"]
    amount = float(transaction["amount"])
    city = transaction["city"]

    transaction_time = datetime.fromisoformat(
        transaction["timestamp"].replace("Z", "+00:00")
    )

    fraud_reasons = []


    # Rule 1: Amount above 50000 Rs
    if amount > 50000:
        fraud_reasons.append(
            f"High transaction amount: Rs{amount}"
        )

    history = sender_transactions[sender] # Remove transactions older than 2 mins

    while history:

        old_transaction = history[0]

        if transaction_time - old_transaction["timestamp"] > timedelta(minutes=2):
           history.popleft()
        else:
            break


    # Rule 2: More than 5 transactions in 60 seconds
    transaction_last_60_seconds = [
        txn for txn in history
        if transaction_time - txn["timestamp"] <= timedelta(seconds=60)
    ]

    if len(transaction_last_60_seconds) >= 5:
        fraud_reasons.append(
            f"More than 5 transactions from the same sender within 60 seconds"
        )

    # Rule 3: Different citys within 2 minutes

    recent_cities = {
        txn["city"]
        for txn in history
        if transaction_time - txn["timestamp"] <= timedelta(minutes=2)
    }

    if recent_cities and city not in recent_cities:
        fraud_reasons.append(
            f"Transcations from diffrent citites within 2 minutes:"
            f"{recent_cities} -> {city}"
        )

    history.append({
        f"timestamp": transaction_time,
        "city": city
    })

    return fraud_reasons

# publish alert function

def publish_alert(transaction, fraud_reasons):

    alert = {
        "txn_id": transaction["txn_id"],
        "sender": transaction["sender"],
        "receiver": transaction["receiver"],
        "amount": transaction["amount"],
        "city": transaction["city"],
        "timestamp": transaction["timestamp"],
        "status": transaction["status"],
        "fraud_reasons": fraud_reasons,
        "alert_type": "FRAUD"
    }

    alerts_producer.produce(
        ALERTS_TOPIC,
        key=transaction["sender"],
        value=json.dumps(alert)
    )

    alerts_producer.flush()

     # Store alert in PostgreSQL
    query = """
        INSERT INTO fraud_alerts
        (txn_id, sender, amount, city, timestamp, fraud_reasons)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (txn_id) DO NOTHING
    """

    transaction_time = datetime.fromisoformat(
        transaction["timestamp"].replace("Z", "+00:00")
    )

    cursor.execute(query, (
        transaction["txn_id"],
        transaction["sender"],
        transaction["amount"],
        transaction["city"],
        transaction_time,
        json.dumps(fraud_reasons)
    ))

    conn.commit()

    print(f" FRAUD ALERT: {transaction['txn_id']}")


consumer.subscribe([SOURCE_TOPIC])

print("Transaction Consumer Started...")
print("Kafka topic:", SOURCE_TOPIC)
print("PostgreSQL database:", DB_NAME)
print("Dead Letter Topic:", DLQ_TOPIC)
print("Press CTRL+C to stop\n")

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():

            raise KafkaException(msg.error())

        try:

            value = msg.value().decode("utf-8")

            transaction = json.loads(value)

            print("\n Received:")
            print(transaction)

            valid, reason = validate_transactions(transaction)

            if not valid:

                send_to_dead_letter(
                    transaction,
                    reason
                )

                consumer.commit(
                    message=msg,
                    asynchronous=False
                )

                continue

            insert_transaction(transaction)

            print(
                f"Inserted into PostgreSQL | "
                f"txn_id={transaction['txn_id']}"
            )

            consumer.commit(
                message=msg,
                asynchronous=False
            )

        except json.JSONDecodeError as e:

            print(" Invalid JSON")

            try:

                raw_message = msg.value().decode(
                    "utf-8",
                    errors="replace"
                )

                send_to_dead_letter(
                    {
                        "raw_message":raw_message
                    },
                    f"Invalid JSON: {str(e)}"
                )

                consumer.commit(
                    message=msg,
                    asynchronous=False
                )

            except Exception as dlq_error:

                print(
                    f"Failed to send to DLQ: {dlq_error}"
                )

        except Exception as e:

            print(
                f"Processing error: {e}"
            )
            import traceback

            traceback.print_exc()

            conn.rollback()

except KeyboardInterrupt:

    print("\n Consumer stopped by user.")

finally:

    consumer.close()

    cursor.close()
    conn.close()

    dlq_producer.flush()

    print("Connections closed.")

fraud_reasons = check_fraud(transaction)

if fraud_reasons:
    publish_alert(transaction, fraud_reasons)