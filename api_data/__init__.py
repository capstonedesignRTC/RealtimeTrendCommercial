from api_data.nemo import get_nemo_api, get_submunicipality_code
from api_data.seoul_api import get_all_seoul_data
from api_data.utils import DATA_GO_KR_API_KEYS, SEOUL_DATA_API_KEYS, SEOUL_MUNICIPALITY_CODE

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

                # 여기에 producer  연결하는 코드 작성

    """
    네모 데이터
    - 매물 정보 crawling
    """
    # for municipality_code in SEOUL_MUNICIPALITY_CODE.values():
    #     for data in get_nemo_api(municipality_code):
    #         result = {"key": municipality_code, **data}
    #
    #         # 여기에 producer  연결하는 코드 작성
