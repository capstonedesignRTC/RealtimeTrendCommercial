from seoul_api import get_openapi_seoul_data


def get_all_openapi_data(key, data_api_key, start, end, year):
    ## 서울시 데이터는 정형화 되어 있음.
    try:
        start, limit = 0, 20
        while True:
            if start == 100:
                break
            for year in [2017, 2018, 2019, 2020, 2021, 2022]:
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
