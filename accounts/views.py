from django.shortcuts import render

# Create your views here.
# accounts/views.py
from django.contrib.auth.views import LoginView

class UserLoginView(LoginView):
    template_name = "accounts/login.html"