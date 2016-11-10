from django.shortcuts import render
from django.http import HttpResponse, HttpResponseRedirect
import json
import time
from Scheduler.spark import Spark_Scheduler
from Scheduler.monitor import SparkMonitor
from Server.models import Algorithm, AlgorithmParameters, file, Mission, ResultFile
from datetime import datetime
from Scheduler.convert import readFile


from django.views.decorators.csrf import csrf_exempt

# Create your views here.

def byteify(input):
    if isinstance(input, dict):
        return {byteify(key): byteify(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input

@csrf_exempt
def scheduler(request):
    dict = {}
    info = 'OK'
    missionId = -1
    try:
        print request.method
        if request.method == 'POST':
            body = json.loads(request.body)
            req = byteify(body)

            missionOwner = "zhu"
            missionFlowPath = "/home/spark/FlowGraph/"+missionOwner+"_"+req["taskName"]+".txt"
            mission = Mission(missionName=req["taskName"], missionOwner=missionOwner, missionStartDate=datetime.now(), missionFlowPath=missionFlowPath)
            mission.save()
            missionId = str(Mission.objects.get(missionName=req["taskName"]).id)

            with open(missionFlowPath,"w") as f:
                f.write(req)

            print req
            File = []
            for source in req['sources']:
                fileID = int(source['id'])
                filePath = file.objects.get(fileID=fileID).filePath
                File.append((source['flowID'], filePath))

            paras = {}
            opParas = ["--num-executors", "--executor-cores", "--executor-memory"]
            for process in req['processes']:
                processID = int(process['id'])
                processinfo = Algorithm.objects.get(algorithmID=processID)
                parasinfo = processinfo.algorithmparameters_set

                flowID = process['flowID']
                paras[flowID] = {}
                paras[flowID]['jar'] = processinfo.jarPath
                paras[flowID]['op_para'] = {}
                paras[flowID]['op_para']['--class'] = processinfo.className
                paras[flowID]['op_para']['--master'] = 'yarn'
                paras[flowID]['op_para']['--deploy-mode'] = 'cluster'
                for i in opParas:
                    paras[flowID]['op_para'][i] = parasinfo.get(paraName=i).val

                paras[flowID]['al_para'] = {}
                paras[flowID]['al_para']['sort'] = processinfo.inputSort.split(',')
                for i in paras[flowID]['al_para']['sort']:
                    if i == "flowID":
                        paras[flowID]['al_para'][i] = flowID
                    else:
                        if parasinfo.get(paraName=i).valType == 'select':
                            paras[flowID]['al_para'][i] = parasinfo.get(paraName=i).description.split(',')[int(parasinfo.get(paraName=i).val)]
                        else:
                            if parasinfo.get(paraName=i).paraTags == 'ouPara':
                                paras[flowID]['al_para'][i] = parasinfo.get(paraName=i).val + "_" + missionId +"_" +flowID
                            elif parasinfo.get(paraName=i).val == 'None':
                                if i.find("input") >=0 or i.find("output") >=0:
                                    for j in req['paths']:
                                        if flowID == j.split('->')[1]:
                                            for t in req['processes']:
                                                if t['flowID'] == j.split('->')[0]:
                                                    col = Algorithm.objects.get(algorithmID=int(t['id'])).algorithmparameters_set.get(paraTags='ouPara')

                                                    if Algorithm.objects.get(algorithmID=int(t['id'])).outputNumber == 1:
                                                        if i.replace('input', 'output') in Algorithm.objects.get(
                                                            algorithmID=int(t['id'])).inputSort.split(','):
                                                            paras[flowID]['al_para'][i] = col.val + "_" + missionId + "_" + \
                                                                                          t['flowID']
                                                        elif col.paraName.replace('output', 'input') in paras[flowID]['al_para']['sort']:
                                                            paras[flowID]['al_para'][
                                                                col.paraName.replace('output', 'input')] = col.val + "_" + missionId + "_" + t[
                                                                "flowID"]
                                                        else:
                                                            paras[flowID]['al_para'][i] = Algorithm.objects.get(
                                                                algorithmID=int(t['id'])).algorithmName
                                                    elif Algorithm.objects.get(algorithmID=int(t['id'])).outputNumber > 1:
                                                        for oupara in col:
                                                            if i.replace('input', 'output') in Algorithm.objects.get(
                                                            algorithmID=int(t['id'])).inputSort.split(','):
                                                                paras[flowID]['al_para'][
                                                                    i] = oupara.val + "_" + missionId + "_" + \
                                                                         t['flowID']
                                                            elif oupara.paraName.replace('output', 'input') in paras[flowID]['al_para']['sort']:
                                                                paras[flowID]['al_para'][
                                                                    oupara.paraName.replace('output', 'input')] = oupara.val + "_" + missionId + "_" + t[
                                                                    "flowID"]
                                                            else:
                                                                paras[flowID]['al_para'][i] = Algorithm.objects.get(
                                                                    algorithmID=int(t['id'])).algorithmName
                                else:
                                    paras[flowID]['al_para'][i] = "Unknown"

                                                # if i.replace('input', 'output') in Algorithm.objects.get(algorithmID=int(t['id'])).inputSort.split(','):
                                                #     if Algorithm.objects.get(algorithmID=int(t['id'])).outputNumber == 1:
                                                #         paras[flowID]['al_para'][i] = col.val + "_" + missionId +"_" + t['flowID']
                                                #     else:
                                                #         for oupara in col:
                                                #             if  i.replace('input', 'output') in Algorithm.objects.get(algorithmID=int(t['id'])).inputSort.split(','):
                                                #                 paras[flowID]['al_para'][i] = oupara.val + "_" + missionId + "_" + t['flowID']
                                                # elif col.paraName.replace('output', 'input') in paras[flowID]['al_para']['sort']:
                                                #     paras[flowID]['al_para'][col.paraName.replace('output', 'input')] = col.val + t["flowID"]
                                                # else:
                                                #     paras[flowID]['al_para'][i] = Algorithm.objects.get(algorithmID=int(t['id'])).algorithmName
                            else:
                                paras[flowID]['al_para'][i] = parasinfo.get(paraName=i).val

            for i in File:
                for j in req['paths']:
                    if i[0] == j[0]:
                        paras[j.split('->')[1]]['al_para']['inputRaw'] = i[1]
            paras["missionID"] = missionId
            paras = byteify(paras)
            print paras
            SpScheduler = Spark_Scheduler()
            status = SpScheduler.run(req, paras)
            mission = Mission.objects.get(id=missionId)
            mission.missionStatus = 3
            mission.missionEndDate = datetime.now()
            mission.save()
            dict = status

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
        mission = Mission.objects.get(id=missionId)
        mission.missionStatus = 2
        mission.missionEndDate = datetime.now()
        mission.save()
        print info
    dict["message"] = info
    dict["createTime"] = str(time.ctime())

    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "*"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"

    return response

@csrf_exempt
def sendSparkInformation(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            # body = json.loads(request.body)
            # req = byteify(body)
            # print req
            File = []
            file_objects = file.objects.all()
            for source in file_objects:
                obj = {}
                obj["id"] = str(source.fileID)
                obj["label"] = source.fileName
                obj["description"] = ""
                File.append(obj)

            Process = []
            algorithm_objects = Algorithm.objects.all()
            for algorithm in algorithm_objects:
                obj = {}
                obj["id"] = str(algorithm.algorithmID)
                obj["label"] = algorithm.algorithmName
                obj["description"] = algorithm.description
                obj["parameters"] = []
                algorithmP_objects = algorithm.algorithmparameters_set.all()
                for algorithmP in algorithmP_objects:
                    if algorithmP.paraTags != "alPara" or algorithmP.paraName.find("input") >= 0:
                        continue
                    obj_p = {}
                    obj_p["label"] = algorithmP.paraName
                    obj_p["val"] = algorithmP.val
                    obj_p["slug"] = algorithmP.paraName
                    obj_p["controlType"] = algorithmP.valType
                    if obj_p["controlType"] == "select":
                        obj_p["options"] = algorithmP.description.split(",")
                    obj["parameters"].append(obj_p)
                Process.append(obj)

            paras = {}
            paras["sources"] = File
            paras["processes"] = Process
            dict = paras

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
        dict["message"] = info
        dict["createTime"] = str(time.ctime())

    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

@csrf_exempt
def updateParaToMysql(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'POST':
            body = json.loads(request.body)
            req = byteify(body)
            for source in req['sources']:
                File = file(fileID=int(source['id']), fileName=source['label'], filePath=source['filepath'])

                File.save()

            for process in req['processes']:
                algorithm = Algorithm(algorithmID=int(process['id']), algorithmName=process['name'],
                                      tags=process['tags'], jarPath=process['jarpath'], className=process['className'],
                                      inputNumber=int(process['inputNumber']), outputNumber=int(process['outputNumber']),
                                      inputSort=process['sort'], description=process['description'])
                algorithm.save()
                for para in process['parameters']:
                    algorithmpara = AlgorithmParameters(paraName=para['label'], paraTags=para['tags'],
                                                        valType=para['type'], val=para['val'], description=para['description'],
                                                        algorithm=algorithm)
                    algorithmpara.save()
    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
    dict["message"] = info
    dict["createTime"] = str(time.ctime())
    response = json.dumps(dict)
    return HttpResponse(response)

@csrf_exempt
def sparkMonitor(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            body = json.loads(request.body)
            req = byteify(body)
            appID = req['processID']
            monitor = SparkMonitor('localhost', '8088')
            dict['app'] = monitor.appInfo(appID)[2]

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
    dict["message"] = info
    dict["createTime"] = str(time.ctime())
    response = json.dumps(dict)
    return HttpResponse(response)

@csrf_exempt
def processInformation(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            print request.GET['taskName']
            missionid = str(Mission.objects.get(missionName=request.GET['taskName']).id)
            flowid = request.GET["flowID"]
            dict["url"] = "ws://10.5.0.222:8888/connect?request_path=MISSION_"+missionid+"_"+flowid

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
    dict["message"] = info
    dict["createTime"] = str(time.ctime())

    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

@csrf_exempt
def sendResultInformation(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            mission = Mission.objects.get(missionName=request.GET['taskName'])
            # print req
            resultFile = []
            file_objects = mission.resultfile_set.all()

            for source in file_objects:
                obj = {}
                obj["id"] = str(source.id)
                obj["label"] = source.resultName
                resultFile.append(obj)

            dict = resultFile

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
        dict["message"] = info
        dict["createTime"] = str(time.ctime())

    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

@csrf_exempt
def showResult(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            mission = Mission.objects.get(missionName=request.GET['taskName'])
            # print req
            resultPath = mission.resultfile_set.get(resultName=request.GET['label']).resultPath
            resultdata = readFile(resultPath)

            dict = resultdata

    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
        dict["message"] = info
        dict["createTime"] = str(time.ctime())

    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

@csrf_exempt
def receive(request):
    jarPath = "/home/spark/JAR/"
    if request.method == "POST":
        ag = Algorithm(request.POST, request.FILES)
        if ag.is_valid():
            ags = Algorithm.objects.all()
            id = []
            for i in ags:
                id.append(int(i.algorithmID))
            algorithm_id = str(max(id) + 1)
            algorithm_name = ag.cleaned_data['algorithmName']
            algorithm_tags = ag.cleaned_data['tags']
            algorithm_jarPath = jarPath + ag.cleaned_data['jarName']
            algorithm_className = ag.cleaned_data['className']
            algorithm_inputNumber = ag.cleaned_data['inputNumber']
            algorithm_outputNumber = ag.cleaned_data['outputNumber']
            algorithm_inputSort = ag.cleaned_data['inputSort']
            algorithm_text = ag.cleaned_data['description']
            a=Algorithm(algorithm_name=algorithm_name, algorithmID=algorithm_id, tags=algorithm_tags, description=algorithm_text,
                        className=algorithm_className, jarPath=algorithm_jarPath, inputNumber=algorithm_inputNumber,
                        outputNumber=algorithm_outputNumber, inputSort=algorithm_inputSort)
            a.save()
            return HttpResponse('upload ok!')
        return HttpResponse("algorithm_name is invalid")
    return HttpResponse("error")


def diaplay(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            missionName=request.GET['taskName']
            mission= Mission.objects.get(missionName=missionName)
            missionOwner =mission.missionOwner
            missionStartDate = mission.missionStartDate
            missionEndDate = mission.missionEndDate
            missionStatus = mission.missionStatus
            dict["missionName"] = missionName
            dict["missionOwner"] = missionOwner
            dict["missionStartDate"] = missionStartDate
            dict["missionEndDate"] = missionEndDate
            dict["missionStatus"] = missionStatus
    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
    dict["message"] = info
    dict["createTime"] = str(time.ctime())
    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response

def recovery(request):
    dict = {}
    info = 'OK'
    try:
        if request.method == 'GET':
            missionName = request.GET['taskName']
            missionFlowPath=Mission.objects.get(missionName=missionName).missionFlowPath
            file=open(missionFlowPath,'r')
            content=file.readline()
            dict=json.loads(content)
            file.close()
    except:
        import sys
        info = "%s || %s" % (sys.exc_info()[0], sys.exc_info()[1])
    dict["message"] = info
    dict["createTime"] = str(time.ctime())
    response = HttpResponse(json.dumps(dict), content_type="application/json")
    response["Access-Control-Allow-Origin"] = "*"
    response["Access-Control-Allow-Methods"] = "POST,GET"
    response["Access-Control-Max-Age"] = "1000"
    response["Access-Control-Allow-Headers"] = "*"
    return response