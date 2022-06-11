import json
import time

from kafka import KafkaProducer
from utils.nemo import get_nemo_api
# from nemo import get_nemo_api
from utils.seoul_api import get_all_seoul_data
from utils.utils import (SEOUL_DATA_API_KEYS, SEOUL_MUNICIPALITY_CODE,
                         TOPIC_NAME)

bootstrap_servers = ["localhost:9091", "localhost:9092", "localhost:9093"]
topicName = TOPIC_NAME

producer = KafkaProducer(
    acks=0,
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda m: json.dumps(m).encode("euc-kr"),
)

if __name__ == "__main__":
    """
    서울시 데이터
    - https://data.seoul.go.kr/ 참고
    """
    for key, data_api_key in SEOUL_DATA_API_KEYS.items():
        for year in [2018, 2019, 2020, 2021, 2022]:
            for data in get_all_seoul_data(key, data_api_key, year):
                if not data:
                    print(f"{year}년도 자료가 존재하지 않음")
                    continue

                result = {"key": key, **data}
                print(f"data sending : {result.get('key')}_{result.get('year')}_{result.get('page')}")

                producer.send(topicName, result)
                time.sleep(2)

    """
    네모 데이터
    - 매물 정보 crawling 
    """
    for municipality_code in SEOUL_MUNICIPALITY_CODE.values():
        for data in get_nemo_api(municipality_code):
            result = {"key": municipality_code, **data}
            # print(result)
            # 여기에 producer  연결하는 코드 작성
            producer.send(topicName, result)
            time.sleep(2)
