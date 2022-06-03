import json
from io import StringIO

import boto3
import pandas as pd
from kafka import KafkaConsumer

# import pandas as pd
BUCKET_NAME = "rtc25"

s3 = boto3.resource("s3").Bucket(BUCKET_NAME)


bootstrap_servers = ["localhost:9091", "localhost:9092", "localhost:9093"]


if __name__ == "__main__":

    topicName = "practice"

    consumer = KafkaConsumer(
        topicName,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="consumer-group-a",
        # value_deserializer=json_deserializer
        value_deserializer=lambda m: json.loads(m.decode("euc-kr")),
    )

    print("Start Consumer")
    args = {'ACL': 'public-read'}

    for msg in consumer:
        try:
            s3_path = f"{msg.value['key']}_{msg.value['year']}_{msg.value['page']}.csv"
            raw_data = pd.json_normalize(msg.value["data"])
            csv_buffer = StringIO()
            # msg = json.dumps(msg.value["data"])
            raw_data.to_csv(csv_buffer, encoding="euc-kr")

            s3.put_object(Body=csv_buffer.getvalue(), Bucket="rtc25", Key=s3_path)
        except:
            pass
