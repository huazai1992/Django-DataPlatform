"""Platform_Server URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from Server import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^submit/$', views.scheduler),
    url(r'^updateMysql/$', views.updateParaToMysql),
    url(r'^sendinformation/$', views.sendSparkInformation),
    url(r'^processinformation/$', views.processInformation),
    url(r'^sendresultinformation/$', views.sendResultInformation),
    url(r'^visualization/$', views.showResult),
    url(r'^redraw/$', views.recovery),
    url(r'^get_history/$', views.diaplay),
    url(r'^uploadfile/$', views.receiveFile)
]
