from kafka import KafkaConsumer
import json
import boto3
BUCKET_NAME = 'rtc25'

s3 = boto3.resource("s3").Bucket(BUCKET_NAME)

json.dump_s3 = lambda obj, f: s3.Object(key=f).put(Body=json.dumps(obj))
# client.create_bucket(Bucket='rtc25')
# def upload_files(file_name, bucket, object_name=None, args=None):
#     if object_name is None:
#         object_name = file_name

#     reponse = client.upload_file(file_name, bucket, object_name, ExtraArgs = args)

bootstrap_servers = ['localhost:9091','localhost:9092','localhost:9093']


if __name__ == "__main__":
    for i in range(1,10):
        for j in range(2018,2022):
            for k in range(0,5):
                topicName = str(i)+'_'+str(j)+'_'+str(k)
                print(topicName)

    # topicName = "1_2019_1"

                consumer = KafkaConsumer(
                    topicName,
                    bootstrap_servers=bootstrap_servers,
                    auto_offset_reset = 'earliest',
                    group_id = "consumer-group-a",
                    # value_deserializer=json_deserializer
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                print("Start Consumer")
                # args = {'ACL': 'public-read'}
                for msg in consumer:
                    json.dump_s3(msg,topicName+".csv")
                    #timestamp = time.mktime(datetime.today().timetuple())
                    # print("Result = {}".format(json.loads(msg.value)))
                    # json_msg = json.dumps(msg).decode('utf8')
                    # print(type(json_msg))
                    # with io.BytesIO() as msg:
                    #     client.upload_fileobj(msg,Bucket = BUCKET_NAME,Key = topicName+".csv")

                    # print(msg.topic)
                    print(type(msg.value))
                    # print(msg.value)
