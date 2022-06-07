from django.urls import include, path

from . import views

urlpatterns = [
    path("", views.welcome_page, name="main page"),
    path("code/", views.select_code_page, name="select page"),
    path("result/<code>", views.result_page, name="result page"),
    path("processing/", views.calculation_process, name="calculation_process"),
    path("loading/", views.loading_page, name="more info"),
    path("info/", views.result_page, name="more info"),
    path("test/", views.test, name="more info"),
]
