from seoul_api import get_openapi_seoul_data
from utils import SEOUL_DATA_API_KEYS


def get_all_openapi_data():
    """
    이렇게 scheduler가 돌려야 하는 형식으로 구현
    """
    result = list()
    for data_api_key in SEOUL_DATA_API_KEYS.values():
        start, limit = 0, 20
        while True:
            res = get_openapi_seoul_data(data_api_key, start, start + limit)
            if not res.get("data"):
                break
            result.append(res)
            start += limit
    return res


get_all_openapi_data()
