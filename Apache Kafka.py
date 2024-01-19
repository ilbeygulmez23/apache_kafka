"""
Demonstration of how to integrate Kafka with a sample application for real-time data processing.
To achieve this, we open a console and first launch zookeeper:

(Note that we execute all commands in a new console)

.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

Now that we started zookeeper, lets start a server:

.\bin\windows\kafka-server-start.bat .\config\server.properties

A server is successfully started.

Finally we create a topic.

.\bin\windows\kafka-topics.bat --create --topic python_topic --bootstrao-server localhost:9092
"""


# Task : Convert a message to uppercase and write it to output topic.
from confluent_kafka import Consumer, Producer
from confluent_kafka import KafkaError


#Create producer and consumers
producer_conf = {
    'bootstrap.servers' : 'localhost:9092',
    'client.id' : 'python_producer',
}
consumer_conf = {
    'bootstrap.servers' : 'localhost:9092',
    'group.id' : 'python_consumer',
    'auto.offset.reset' : 'earliest'
}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)

consumer.subscribe(['python_topic'])

producer.produce('python_topic', key = b'key', value = b'Hello, Kafka!')

message_processed = False
while not message_processed:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print(f"Received message: {msg.value().decode('utf-8')}")

    # Process the message
    processed_message = msg.value().decode('utf-8').upper()

    # Produce the processed message to the output topic
    producer.produce('output_python_topic', value=processed_message)
    print(f"Processed message: {processed_message}")
    message_processed = True

# Close down consumer and producer
consumer.close()
producer.flush()