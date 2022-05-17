from data_go import get_odcloud_gov_data
from seoul_api import get_openapi_seoul_data
from utils import DATA_GO_KR_API_KEYS, SEOUL_DATA_API_KEYS


def get_all_openapi_data():
    result = list()
    ## 서울시 데이터는 정형화 되어 있음.
    for key, data_api_key in SEOUL_DATA_API_KEYS.items():
        try:
            start, limit = 0, 20
            while True:
                for year in [2017, 2018, 2019, 2020, 2021, 2022]:
                    res = get_openapi_seoul_data(
                        service_key=data_api_key,
                        start=start,
                        end=start + limit,
                        year=year,
                    )
                    if not res.get("data"):
                        break
                    res = {"key": key, **res}

                    result.append(res)  # 아니면 여기를 kafka producer와 연결해야 한다.
                start += limit

        except Exception as e:
            print(e)
            continue

    for key, data_api_key in DATA_GO_KR_API_KEYS.items():
        try:
            page, limit = 1, 20
            while True:
                res = get_odcloud_gov_data(
                    service_key=data_api_key,
                    page=page,
                    size=limit,
                )

                if not res.get("data"):
                    break
                res = {"key": key, **res}
                result.append(res)  # 아니면 여기를 kafka producer와 연결해야 한다.
            start += limit

        except Exception as e:
            print(e)
            continue

    return result
