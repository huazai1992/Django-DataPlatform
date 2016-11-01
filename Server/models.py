from __future__ import unicode_literals

from django.db import models
from datetime import datetime

# Create your models here.
class Algorithm(models.Model):
    algorithmID = models.IntegerField()
    algorithmName = models.CharField(max_length=30)
    tags = models.CharField(max_length=20)
    jarPath = models.CharField(max_length=100)
    className = models.CharField(max_length=100)
    inputNumber = models.IntegerField()
    outputNumber = models.IntegerField()
    inputSort = models.CharField(max_length=100)
    description = models.CharField(max_length=100, default="")

class AlgorithmParameters(models.Model):
    paraName = models.CharField(max_length=20)
    paraTags = models.CharField(max_length=10)
    valType = models.CharField(max_length=10)
    val = models.CharField(max_length=100)
    description = models.CharField(max_length=100, blank=True)
    # algorithmID = models.IntegerField()
    algorithm = models.ForeignKey(Algorithm, null=True)

    class Meta:
        unique_together = ("paraName", "paraTags", "algorithm")

class file(models.Model):
    fileID = models.IntegerField()
    fileName = models.CharField(max_length=50)
    filePath = models.CharField(max_length=100)

class Mission(models.Model):
    missionName = models.CharField(max_length=50)
    missionOwner = models.CharField(max_length=50)
    missionDate = models.DateTimeField(default=None)
    missionStatus = models.IntegerField(default=0)

class ResultFile(models.Model):
    resultName = models.CharField(max_length=50)
    resultPath = models.CharField(max_length=100)
    missionId = models.ForeignKey(Mission, null=True)

class LogFile(models.Model):
    logName = models.CharField(max_length=50)
    logPath = models.CharField(max_length=100)
    logFlowId = models.IntegerField()
    missionId = models.ForeignKey(Mission, null=True)