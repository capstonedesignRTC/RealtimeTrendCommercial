import json

from django.http import HttpResponse, JsonResponse
from django.shortcuts import redirect, render

from main.models import Request
from main.result import show_result
from main.s3storage import get_files
from main.serializers import RequestsSerializer
from main.utils import CODE, SEOUL_MUNICIPALITY_CODE


# 메인 페이지
def welcome_page(request):
    try:
        return render(request, "main/main.html")
    except:
        return render(request, "main/error.html")


# 선택 페이지임
def select_code_page(request):
    try:
        return render(request, "main/code.html")
    except:
        return render(request, "main/error.html")


# 결과 기다리기
def loading_page(request):
    try:
        if request.method == "GET":
            params = request.GET
            data = {
                "year": int(params.get("year")),
                "quarter": int(params.get("quarter")),
                "signgu_code": int(params.get("signgu_code")),
                "adstrd_cd": int(params.get("adstrd_cd")),
            }

            serializer = RequestsSerializer(data=data)
            if serializer.is_valid():
                serializer.save()

        return render(request, "main/loading.html")
    except:
        return render(request, "main/error.html")


# 결과를 보여줌
def result_page(request, code):
    try:
        obj = Request.objects.get(id=int(code))
        results, rank, api_data = show_result(obj)
        nearbys, rank_neary, _ = show_result(obj, True)
        return render(
            request,
            "main/result.html",
            {
                "year": [obj.year],
                "quarter": [obj.quarter],
                "rank": [rank],
                "rank_neary": [rank_neary],
                "results": results,
                "nearbys": nearbys,
                "sig": [SEOUL_MUNICIPALITY_CODE.get(str(obj.signgu_code), "서울")],
                "ads": [CODE.get(str(obj.signgu_code), {}).get(str(obj.adstrd_cd), "여러동")],
                "nemos": api_data,
            },
        )
    except:
        return render(request, "main/error.html")


def calculation_process(request):
    obj = Request.objects.order_by("-id")[0]
    get_files(obj)
    return JsonResponse({"message": "finish", "code": obj.id})


def test(request):
    obj = Request.objects.order_by("-id")[0]
    results, rank, api_data = show_result(obj)
    nearbys, rank_neary, _ = show_result(obj, True)
    return render(
        request,
        "main/result.html",
        {
            "year": [obj.year],
            "quarter": [obj.quarter],
            "rank": [rank],
            "rank_neary": [rank_neary],
            "results": results,
            "nearbys": nearbys,
            "sig": [SEOUL_MUNICIPALITY_CODE.get(str(obj.signgu_code), "서울")],
            "ads": [CODE.get(str(obj.signgu_code), {}).get(str(obj.adstrd_cd), "여러동")],
            "nemos": api_data,
        },
    )
