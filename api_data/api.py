from data_go import get_odcloud_gov_data
from seoul_api import get_openapi_seoul_data
from utils import DATA_GO_KR_API_KEYS, SEOUL_DATA_API_KEYS


def get_all_openapi_data(key, data_api_key, start, end, year):
    ## 서울시 데이터는 정형화 되어 있음.
    try:
        start, limit = 0, 20
        while True:
            if start == 100:
                break
            for year in [2022]:  # 2017, 2018, 2019, 2020, 2021,
                res = get_openapi_seoul_data(
                    service_key=data_api_key,
                    start=start,
                    end=start + limit,
                    year=year,
                )
                if not res.get("data"):
                    break
                res = {"key": key, "year": year, "data": res}
                yield year
            start += limit

    except Exception as e:
        print(e)


"""

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

"""
