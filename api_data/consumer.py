from kafka import KafkaConsumer
import json

bootstrap_servers = ['localhost:9091','localhost:9092','localhost:9093']
topicName='practice'

if __name__ == "__main__":
    consumer = KafkaConsumer(
        topicName,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset = 'earliest',
        group_id = "consumer-group-a"
    )
    print("Start Consumer")
    for msg in consumer:
        print("Result = {}".format(json.loads(msg.value)))
