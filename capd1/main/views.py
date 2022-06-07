from django.http import JsonResponse
from django.shortcuts import redirect, render

from main.models import Request
from main.result import show_result
from main.s3storage import get_files
from main.serializers import RequestsSerializer
from main.utils import CODE, SEOUL_MUNICIPALITY_CODE


# 메인 페이지
def welcome_page(request):
    return render(request, "main/main.html")


# 선택 페이지임
def select_code_page(request):
    return render(request, "main/code.html")


# 결과 기다리기
def loading_page(request):
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


# 결과를 보여줌
def result_page(request, code):
    obj = Request.objects.get(id=int(code))

    obj.signgu_code
    obj.adstrd_cd
    results, api_data = show_result(obj)

    return render(
        request,
        "main/result.html",
        {
            "results": results,
            "sig": [SEOUL_MUNICIPALITY_CODE.get(str(obj.signgu_code), "서울")],
            "ads": [CODE.get(str(obj.signgu_code), {}).get(str(obj.adstrd_cd), "여러동")],
            "nemos": api_data,
        },
    )


def calculation_process(request):
    obj = Request.objects.order_by("-id")[0]
    get_files(obj)
    return JsonResponse({"message": "finish", "code": obj.id})
