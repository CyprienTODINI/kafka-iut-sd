from kafka import KafkaProducer
import json
import time
import datetime

# Define the Kafka broker and topic
broker = 'kafka.todini2u-dev.svc.cluster.local:9092'
topic = 'my-first-topic'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='MoKZGLP3Fh',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


i = 1
while True:
    today = datetime.datetime.now()


    # Define the message to send
    message = {
        'key': 'tiens-une-autre-clee',
        'value': f'{i}',
        'time_stamps': f'{today.strftime("%d/%m/%Y - %H:%M:%S")}'
    }
    # Send the message to the Kafka topic
    producer.send(topic, value=message)

    # Ensure all messages are sent before closing the producer
    producer.flush()

    print(f"Message {i} sent to topic {topic}")

    i += 1
    time.sleep(1)