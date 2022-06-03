import json
import time

from kafka import KafkaProducer

from nemo import get_nemo_api
from seoul_api import get_all_seoul_data
from utils import SEOUL_DATA_API_KEYS, SEOUL_MUNICIPALITY_CODE


def json_serializer(data):
    return json.dumps(data).encode("euc-kr")

bootstrap_servers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topicName = "practice"

producer = KafkaProducer(acks=0, bootstrap_servers=bootstrap_servers, value_serializer=json_serializer)

if __name__ == "__main__":
    """
    서울시 데이터
    """
    for key, data_api_key in SEOUL_DATA_API_KEYS.items():
        for year in [2018, 2019, 2020, 2021, 2022]:
            for data in get_all_seoul_data(key, data_api_key, year):
                result = {"key": key, **data}
                if "year" not in result:
                    result.update({"year": year})
                # print(result)
                # 여기에 producer  연결하는 코드 작성

                print(result.get("key"), result.get("year"), result.get("page"))

                producer.send(topicName, result)
               
    #             # 여기에 producer  연결하는 코드 작성

    # """
    # 네모 데이터
    # - 매물 정보 crawling
    # """
    # for municipality_code in SEOUL_MUNICIPALITY_CODE.values():
    #     for data in get_nemo_api(municipality_code):
    #         result = {"key": municipality_code, **data}
    #         # print(result)
    #         # 여기에 producer  연결하는 코드 작성
    #         producer.send(topicName, result)
    #         time.sleep(2)
