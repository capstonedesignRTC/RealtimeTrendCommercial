from rest_framework import serializers

from main.models import Request, Result


class RequestsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Request
        fields = ["year", "quarter", "signgu_code", "adstrd_cd"]


class ResultsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Result
