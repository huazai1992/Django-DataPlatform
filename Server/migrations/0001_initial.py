# -*- coding: utf-8 -*-
# Generated by Django 1.10.2 on 2016-10-29 06:00
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Algorithm',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('algorithmID', models.IntegerField()),
                ('algorithmName', models.CharField(max_length=30)),
                ('tags', models.CharField(max_length=20)),
                ('jarPath', models.CharField(max_length=100)),
                ('className', models.CharField(max_length=100)),
                ('inputNumber', models.IntegerField()),
                ('outputNumber', models.IntegerField()),
                ('inputSort', models.CharField(max_length=100)),
                ('description', models.CharField(default='', max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='AlgorithmParameters',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('paraName', models.CharField(max_length=20)),
                ('paraTags', models.CharField(max_length=10)),
                ('valType', models.CharField(max_length=10)),
                ('val', models.CharField(max_length=100)),
                ('description', models.CharField(blank=True, max_length=100)),
                ('algorithm', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='Server.Algorithm')),
            ],
        ),
        migrations.CreateModel(
            name='file',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('fileID', models.IntegerField()),
                ('fileName', models.CharField(max_length=50)),
                ('filePath', models.CharField(max_length=100)),
            ],
        ),
        migrations.CreateModel(
            name='Mission',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('missionName', models.CharField(max_length=50)),
                ('missionOwner', models.CharField(max_length=50)),
                ('missionDate', models.DateTimeField(default=None)),
                ('missionStatus', models.IntegerField(default=0)),
            ],
        ),
        migrations.CreateModel(
            name='ResultFile',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('resultName', models.CharField(max_length=50)),
                ('resultPath', models.CharField(max_length=100)),
                ('missionId', models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, to='Server.Mission')),
            ],
        ),
        migrations.AlterUniqueTogether(
            name='algorithmparameters',
            unique_together=set([('paraName', 'paraTags', 'algorithm')]),
        ),
    ]
