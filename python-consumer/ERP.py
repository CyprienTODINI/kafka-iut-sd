from kafka import KafkaConsumer
import json

# Define the Kafka broker and topic
broker = 'my-kafka.todini2u-dev.svc.cluster.local:9092'
topic = 'partitionned'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='9BOLdamB0c',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ERP'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")