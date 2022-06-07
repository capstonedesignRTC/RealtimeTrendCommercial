import json

from django.db import models


class Request(models.Model):
    id = models.BigAutoField(auto_created=True, primary_key=True, serialize=True)
    created_at = models.DateTimeField(auto_now_add=True, editable=False)  # 생성 일자
    year = models.PositiveIntegerField(null=True)
    quarter = models.PositiveIntegerField(null=True)
    signgu_code = models.PositiveIntegerField(null=True)
    adstrd_cd = models.PositiveIntegerField(null=True)

    def __str__(self):
        return str(self.id)

    class Meta:
        ordering = ["-id"]


class Result(models.Model):
    id = models.BigAutoField(auto_created=True, primary_key=True, serialize=True)
    requests_id = models.PositiveIntegerField(null=True)
    trdar_cd = models.CharField(max_length=200, null=True)  # 파일 이름
    signgu_code = models.CharField(max_length=200, null=True)  # 파일 이름
    adstrd_cd = models.CharField(max_length=200, null=True)  # 파일 이름
    rank = models.PositiveIntegerField(null=True)  # 순위
    rank_func = models.PositiveIntegerField(null=True)  # 순위
    stage = models.PositiveIntegerField(null=True)  # 단계

    def __str__(self):
        return str(self.id)

    def load_json(self):
        return json.loads(self.api_data)


class Nemo(models.Model):
    signgu_code = models.CharField(max_length=200, null=True)  # 파일 이름
    adstrd_cd = models.CharField(max_length=200, null=True)  # 파일 이름
    data = models.CharField(max_length=10000, null=True)

    def __str__(self):
        return f"{self.signgu_code}_{self.adstrd_cd}"
