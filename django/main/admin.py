from django.contrib import admin

from .models import Nemo, Request, Result

# Register your models here.
admin.site.register(Request)
admin.site.register(Result)
admin.site.register(Nemo)
