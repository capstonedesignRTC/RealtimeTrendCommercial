import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from this import d


def seoul_one(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15568/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-생활인구)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준 년코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총_생활인구_수").alias("TOT_FLPOP_CO"),
        col("남성_생활인구_수").alias("ML_FLPOP_CO"),
        col("여성_생활인구_수").alias("FML_FLPOP_CO"),
        # 연령대
        col("연령대_10_생활인구_수").alias("AGRDE_10_FLPOP_CO"),
        col("연령대_20_생활인구_수").alias("AGRDE_20_FLPOP_CO"),
        col("연령대_30_생활인구_수").alias("AGRDE_30_FLPOP_CO"),
        col("연령대_40_생활인구_수").alias("AGRDE_40_FLPOP_CO"),
        col("연령대_50_생활인구_수").alias("AGRDE_50_FLPOP_CO"),
        col("연령대_60_이상_생활인구_수").alias("AGRDE_60_ABOVE_FLPOP_CO"),
        # 요일
        col("월요일_생활인구_수").alias("MON_FLPOP_CO"),
        col("화요일_생활인구_수").alias("TUES_FLPOP_CO"),
        col("수요일_생활인구_수").alias("WED_FLPOP_CO"),
        col("목요일_생활인구_수").alias("THUR_FLPOP_CO"),
        col("금요일_생활인구_수").alias("FRI_FLPOP_CO"),
        col("토요일_생활인구_수").alias("SAT_FLPOP_CO"),
        col("일요일_생활인구_수").alias("SUN_FLPOP_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_two(df_spark) -> pd.DataFrame():
    # 서울시 우리마을가게 상권분석서비스(상권-직장인구)
    # https://data.seoul.go.kr/dataList/OA-15569/S/1/datasetView.do
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년월_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
        col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
        col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
        # 연령대
        col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
        col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
        col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
        col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
        col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
        col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_three(df_spark) -> pd.DataFrame():
    # http://data.seoul.go.kr/dataList/OA-15582/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권배후지-생활인구)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총_생활인구_수").alias("TOT_FLPOP_CO"),
        col("남성_생활인구_수").alias("ML_FLPOP_CO"),
        col("여성_생활인구_수").alias("FML_FLPOP_CO"),
        # 연령대
        col("연령대_10_생활인구_수").alias("AGRDE_10_FLPOP_CO"),
        col("연령대_20_생활인구_수").alias("AGRDE_20_FLPOP_CO"),
        col("연령대_30_생활인구_수").alias("AGRDE_30_FLPOP_CO"),
        col("연령대_40_생활인구_수").alias("AGRDE_40_FLPOP_CO"),
        col("연령대_50_생활인구_수").alias("AGRDE_50_FLPOP_CO"),
        col("연령대_60_이상_생활인구_수").alias("AGRDE_60_ABOVE_FLPOP_CO"),
        # 요일
        col("월요일_생활인구_수").alias("MON_FLPOP_CO"),
        col("화요일_생활인구_수").alias("TUES_FLPOP_CO"),
        col("수요일_생활인구_수").alias("WED_FLPOP_CO"),
        col("목요일_생활인구_수").alias("THUR_FLPOP_CO"),
        col("금요일_생활인구_수").alias("FRI_FLPOP_CO"),
        col("토요일_생활인구_수").alias("SAT_FLPOP_CO"),
        col("일요일_생활인구_수").alias("SUN_FLPOP_CO"),
    )

    # df_spark.show(2)
    return df_spark


def seoul_four(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15570/S/1/datasetVieeew.do
    # 서울시 우리마을가게 상권분석서비스(상권배후지-직장인구)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총_직장_인구_수").alias("TOT_WRC_POPLTN_CO"),
        col("남성_직장_인구_수").alias("ML_WRC_POPLTN_CO"),
        col("여성_직장_인구_수").alias("FML_WRC_POPLTN_CO"),
        # 연령대
        col("연령대_10_직장_인구_수").alias("AGRDE_10_WRC_POPLTN_CO"),
        col("연령대_20_직장_인구_수").alias("AGRDE_20_WRC_POPLTN_CO"),
        col("연령대_30_직장_인구_수").alias("AGRDE_30_WRC_POPLTN_CO"),
        col("연령대_40_직장_인구_수").alias("AGRDE_40_WRC_POPLTN_CO"),
        col("연령대_50_직장_인구_수").alias("AGRDE_50_WRC_POPLTN_CO"),
        col("연령대_60_이상_직장_인구_수").alias("AGRDE_60_ABOVE_WRC_POPLTN_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_five(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15584/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권_상주인구)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 인구수
        col("총 상주인구 수").alias("TOT_REPOP_CO"),
        col("남성 상주인구 수").alias("ML_REPOP_CO"),
        col("여성 상주인구 수").alias("FML_REPOP_CO"),
        # 연령대
        col("연령대 10 상주인구 수").alias("AGRDE_10_REPOP_CO"),
        col("연령대 20 상주인구 수").alias("AGRDE_20_REPOP_CO"),
        col("연령대 30 상주인구 수").alias("AGRDE_30_REPOP_CO"),
        col("연령대 40 상주인구 수").alias("AGRDE_40_REPOP_CO"),
        col("연령대 50 상주인구 수").alias("AGRDE_50_REPOP_CO"),
        col("연령대 60 이상 상주인구 수").alias("AGRDE_60_ABOVE_REPOP_CO"),
        # 총 가구 수
        col("총 가구 수").alias("TOT_HSHLD_CO"),
        col("아파트 가구 수").alias("APT_HSHLD_CO"),
        col("비 아파트 가구 수").alias("NON_APT_HSHLD_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_six(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-21278/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-소득소비)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 금액
        col("월_평균_소득_금액").alias("MT_AVRG_INCOME_AMT"),
        col("소득_구간_코드").alias("INCOME_SCTN_CD"),
        col("지출_총금액").alias("EXPNDTR_TOTAMT"),
        col("식료품_지출_총금액").alias("FDSTFFS_EXPNDTR_TOTAMT"),
        col("의류_신발_지출_총금액").alias("CLTHS_FTWR_EXPNDTR_TOTAMT"),
        col("생활용품_지출_총금액").alias("LVSPL_EXPNDTR_TOTAMT"),
        col("의료비_지출_총금액").alias("MCP_EXPNDTR_TOTAMT"),
        col("교통_지출_총금액").alias("TRNSPORT_EXPNDTR_TOTAMT"),
        col("여가_지출_총금액").alias("LSR_EXPNDTR_TOTAMT"),
        col("문화_지출_총금액").alias("CLTUR_EXPNDTR_TOTAMT"),
        col("교육_지출_총금액").alias("EDC_EXPNDTR_TOTAMT"),
        col("유흥_지출_총금액").alias("PLESR_EXPNDTR_TOTAMT"),
    )

    df_spark.show(2)
    return df_spark


def seoul_seven(df_spark) -> pd.DataFrame():
    # http://data.seoul.go.kr/dataList/OA-15572/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-추정매출)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
        # 비율
        col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
        col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
        col("월요일_매출_비율").alias("MON_SELNG_RATE"),
        col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
        col("수요일_매출_비율").alias("WED_SELNG_RATE"),
        col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
        col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
        col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
        col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
        col("남성_매출_비율").alias("ML_SELNG_RATE"),
        col("여성_매출_비율").alias("FML_SELNG_RATE"),
        # 금액
        col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
        col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
        col("월요일_매출_금액").alias("MON_SELNG_AMT"),
        col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
        col("수요일_매출_금액").alias("WED_SELNG_AMT"),
        col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
        col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
        col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
        col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
        col("남성_매출_금액").alias("ML_SELNG_AMT"),
        col("여성_매출_금액").alias("FML_SELNG_AMT"),
        # 건수
        col("주중_매출_건수").alias("MDWK_SELNG_CO"),
        col("주말_매출_건수").alias("WKEND_SELNG_CO"),
        col("월요일_매출_건수").alias("MON_SELNG_CO"),
        col("화요일_매출_건수").alias("TUES_SELNG_CO"),
        col("수요일_매출_건수").alias("WED_SELNG_CO"),
        col("목요일_매출_건수").alias("THUR_SELNG_CO"),
        col("금요일_매출_건수").alias("FRI_SELNG_CO"),
        col("토요일_매출_건수").alias("SAT_SELNG_CO"),
        col("일요일_매출_건수").alias("SUN_SELNG_CO"),
        col("남성_매출_건수").alias("ML_SELNG_CO"),
        col("여성_매출_건수").alias("FML_SELNG_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_eight(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15573/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권배후지-추정매출)
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
        # 비율
        col("주중_매출_비율").alias("MDWK_SELNG_RATE"),
        col("주말_매출_비율").alias("WKEND_SELNG_RATE"),
        col("월요일_매출_비율").alias("MON_SELNG_RATE"),
        col("화요일_매출_비율").alias("TUES_SELNG_RATE"),
        col("수요일_매출_비율").alias("WED_SELNG_RATE"),
        col("목요일_매출_비율").alias("THUR_SELNG_RATE"),
        col("금요일_매출_비율").alias("FRI_SELNG_RATE"),
        col("토요일_매출_비율").alias("SAT_SELNG_RATE"),
        col("일요일_매출_비율").alias("SUN_SELNG_RATE"),
        col("남성_매출_비율").alias("ML_SELNG_RATE"),
        col("여성_매출_비율").alias("FML_SELNG_RATE"),
        # 금액
        col("주중_매출_금액").alias("MDWK_SELNG_AMT"),
        col("주말_매출_금액").alias("WKEND_SELNG_AMT"),
        col("월요일_매출_금액").alias("MON_SELNG_AMT"),
        col("화요일_매출_금액").alias("TUES_SELNG_AMT"),
        col("수요일_매출_금액").alias("WED_SELNG_AMT"),
        col("목요일_매출_금액").alias("THUR_SELNG_AMT"),
        col("금요일_매출_금액").alias("FRI_SELNG_AMT"),
        col("토요일_매출_금액").alias("SAT_SELNG_AMT"),
        col("일요일_매출_금액").alias("SUN_SELNG_AMT"),
        col("남성_매출_금액").alias("ML_SELNG_AMT"),
        col("여성_매출_금액").alias("FML_SELNG_AMT"),
        # 건수
        col("주중_매출_건수").alias("MDWK_SELNG_CO"),
        col("주말_매출_건수").alias("WKEND_SELNG_CO"),
        col("월요일_매출_건수").alias("MON_SELNG_CO"),
        col("화요일_매출_건수").alias("TUES_SELNG_CO"),
        col("수요일_매출_건수").alias("WED_SELNG_CO"),
        col("목요일_매출_건수").alias("THUR_SELNG_CO"),
        col("금요일_매출_건수").alias("FRI_SELNG_CO"),
        col("토요일_매출_건수").alias("SAT_SELNG_CO"),
        col("일요일_매출_건수").alias("SUN_SELNG_CO"),
        col("남성_매출_건수").alias("ML_SELNG_CO"),
        col("여성_매출_건수").alias("FML_SELNG_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_nine(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15560/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권영역)
    """
    상권 코드랑 시군구 코드, 행정동 코드 변환
    """
    df_spark = df_spark.select(
        # 구분 코드
        col("상권_구분_코드").alias("TRDAR_SE_CD"),
        col("상권_구분_코드_명").alias("TRDAR_SE_CD_NM"),
        col("상권_코드").alias("TRDAR_CD"),
        col("시군구_코드").alias("SIGNGU_CD"),
        col("행정동_코드").alias("ADSTRD_CD"),
    )

    df_spark.show(2)
    return df_spark


def seoul_ten(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15566/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-아파트)

    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 아파트
        col("아파트_단지_수").alias("APT_HSMP_CO"),
        col("아파트_평균_면적").alias("AVRG_AE"),
        col("아파트_평균_시가").alias("AVRG_MKTC"),
    )

    df_spark.show(2)
    return df_spark


def seoul_eleven(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15574/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권배후지-아파트)

    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 아파트
        col("아파트_단지_수").alias("APT_HSMP_CO"),
        col("아파트_평균_면적").alias("AVRG_AE"),
        col("아파트_평균_시가").alias("AVRG_MKTC"),
    )

    df_spark.show(2)
    return df_spark


def seoul_twelve(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15567/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(자치구별 상권변화지표)
    """
    LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    """
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("시군구_코드").alias("SIGNGU_CD"),
        # 상권 지표
        col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
        col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
        col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
        col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
        col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
    )

    df_spark.show(2)
    return df_spark


def seoul_thirteen(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15575/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(행정동별 상권변화지표)
    """
    LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    """
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("행정동_코드").alias("ADSTRD_CD"),
        # 상권 지표
        col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
        col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
        col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
        col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
        col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
    )

    df_spark.show(2)
    return df_spark


def seoul_fourteen(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15576/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-상권변화지표)
    """
    LL(다이나믹) : 도시재생 및 신규 개발 상권으로 창업 진출입시 세심한 주의가 필요한 상권(지역), 특정시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    LH(상권확장) : 경쟁력 있는 신규 창업 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 낮고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    HL(상권축소) : 경쟁력 있는 기존 업체 우위 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 낮은 상권(지역)
    HH(정체) : 창업 진출입시 세심한 주의 상권(지역), 특정 시점 사업체의 영업기간이 서울시 평균 생존영업기간보다 높고, 서울시 평균 폐업영업기간보다 높은 상권(지역)
    """
    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        # 상권 지표
        col("상권_변화_지표").alias("TRDAR_CHNGE_IX"),
        col("운영_영업_개월_평균").alias("OPR_SALE_MT_AVRG"),
        col("폐업_영업_개월_평균").alias("CLS_SALE_MT_AVRG"),
        col("서울_운영_영업_개월_평균").alias("SU_OPR_SALE_MT_AVRG"),
        col("서울_폐업_영업_개월_평균").alias("SU_CLS_SALE_MT_AVRG"),
    )

    df_spark.show(2)
    return df_spark


def seoul_fifteen(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-15577/S/1/datasetView.do
    # 서울시 우리마을가게 상권분석서비스(상권-점포)

    df_spark = df_spark.select(
        # 구분 코드
        col("기준_년_코드").alias("STDR_YY_CD"),
        col("기준_분기_코드").alias("STDR_QU_CD"),
        col("상권_코드").alias("TRDAR_CD"),
        col("서비스_업종_코드").alias("SVC_INDUTY_CD"),
        # 점표 지표
        col("점포_수").alias("STOR_CO"),
        col("유사_업종_점포_수").alias("SIMILR_INDUTY_STOR_CO"),
        col("개업_율").alias("OPBIZ_RT"),
        col("개업_점포_수").alias("OPBIZ_STOR_CO"),
        col("폐업_률").alias("CLSBIZ_RT"),
        col("폐업_점포_수").alias("CLSBIZ_STOR_CO"),
        col("프랜차이즈_점포_수").alias("FRC_STOR_CO"),
    )

    df_spark.show(2)
    return df_spark


def seoul_sixteen(df_spark) -> pd.DataFrame():
    # https://data.seoul.go.kr/dataList/OA-20471/S/1/datasetView.do
    # 서울시 불법주정차/전용차로 위반 단속 CCTV 위치정보

    df_spark = df_spark.select(
        # 구분 코드
        col("자치구").alias("PSTINST_CD"),
        col("위도").alias("LATITUDE"),
        col("경도").alias("LONGITUDE"),
    )

    df_spark.show(2)
    return df_spark


# def seoul_seventeen(df_spark) -> pd.DataFrame():
#     # https://data.seoul.go.kr/dataList/OA-13122/S/1/datasetView.do
#     # 서울시 공영주차장 안내 정보

#     df_spark = df_spark.select(
#         # 구분 코드
#         col("자치구").alias("PSTINST_CD"),
#         col("위도").alias("LATITUDE"),
#         col("경도").alias("LONGITUDE"),
#     )

#     df_spark.show(2)
#     return df_spark


def seoul_seventeen(df_spark) -> pd.DataFrame():
    # http://data.seoul.go.kr/dataList/OA-15410/S/1/datasetView.do
    # 서울시 건축물대장 법정동 코드정보

    df_spark = df_spark.select(
        # 구분 코드
        col("자치구").alias("PSTINST_CD"),
        col("위도").alias("LATITUDE"),
        col("경도").alias("LONGITUDE"),
    )

    df_spark.show(2)
    return df_spark


# def seoul_eighteen(df_spark) -> pd.DataFrame():
#     # http://data.seoul.go.kr/dataList/OA-15410/S/1/datasetView.do
#     # 서울시 건축물대장 법정동 코드정보

#     df_spark = df_spark.select(
#         # 구분 코드
#         col("자치구").alias("PSTINST_CD"),
#         col("위도").alias("LATITUDE"),
#         col("경도").alias("LONGITUDE"),
#     )

#     df_spark.show(2)
#     return df_spark
