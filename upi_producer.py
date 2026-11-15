import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer

fake = Faker("en_IN")

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions"

producer = Producer({
    "bootstrap.servers":"localhost:9092",
    "client.id": "transactions-group"
})

def delivery_callback(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")
    else:

        print(
            f"Delivered.."
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}"
        )

def generate_transaction():
    """Generate Fake UPI transaction."""

    sender = f"{fake.user_name()}@upi"
    receiver = f"{fake.user_name()}@upi"

    transaction = {

        "txn_id": fake.uuid4(),
        "sender": sender,
        "receiver": receiver,
        "amount": round(random.uniform(10, 50000), 2),
        "city": fake.city(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status":random.choice([
            "SUCCESS",
            "SUCCESS",
            "SUCCESS",
            "FAILED",
            "PENDING"
        ])
    }

    return transaction

print("UPI Transaction Producer Started..")
print("Press CTRL+C to stop\n")

try:
    while True:

        transaction_per_second = random.randint(5, 10)

        delay = 1 / transaction_per_second

        for _ in range(transaction_per_second):

            transaction = generate_transaction()

            producer.produce(
                TOPIC,
                key=transaction["sender"],
                value=json.dumps(transaction),
                callback=delivery_callback
            )

            producer.poll(0)

            print(f"Transaction: {transaction}")
            time.sleep(delay)

        producer.flush()

except KeyboardInterrupt:
    print("\n Producer stopped by user.")

finally:
    producer.flush()