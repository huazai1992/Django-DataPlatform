import subprocess
import Queue
import thread
import threading
import os
import monitor
import json
from Server.models import ResultFile, Mission

class SchedulerThread(threading.Thread):

    def __init__(self, missionid ,dataflownode, para, name):
        threading.Thread.__init__(self)
        self.dataflownode = dataflownode
        self.para = para
        self.name = name
        self.mission = missionid

    def run(self):
        scheduler = Spark_Scheduler()
        scheduler.scheduler(self.mission, self.dataflownode, self.para)


class Spark_Scheduler:
    def __init__(self):
        self.graph_node = []
        self.graph_edge = []
        self.matrix = None

    def getNodeSet(self, nodeset):
        return self.graph_node

    def getEdgeSet(self, edgeset):
        return self.graph_edge

    def byteify(self, input):
        if isinstance(input, dict):
            return {self.byteify(key): self.byteify(value) for key, value in input.iteritems()}
        elif isinstance(input, list):
            return [self.byteify(element) for element in input]
        elif isinstance(input, unicode):
            return input.encode('utf-8')
        else:
            return input

    def split(self, dataflow):
        node_temp = dataflow["processes"]
        for i in node_temp:
            self.graph_node.append(i["flowID"])

        datanode = dataflow["sources"]
        edge_temp = dataflow["paths"]
        for i in edge_temp:
            node = i.split("->")
            for j in datanode:
                if j["flowID"] not in node:
                    self.graph_edge.append((node[0], node[1]))

    def updatePara(self, paras, nodeinfo):
        for i in nodeinfo:
            for j in i["parameters"]:
                if paras.has_key(i["flowID"]) and j["slug"] in paras[i["flowID"]]["al_para"]["sort"]:
                    print paras
                    if j["val"] != paras[i["flowID"]]["al_para"][j["slug"]] and j["controlType"] != 'select':
                        paras[i["flowID"]]["al_para"][j["slug"]] = j["val"]
                    elif j["controlType"] == 'select' and j["options"][int(j['val'])] != paras[i["flowID"]]["al_para"][j["slug"]]:
                        paras[i["flowID"]]["al_para"][j["slug"]] = j["options"][int(j['val'])]
        return paras

    def submit(self, missionid, flowID,jar, al_args, re_args={}):
        app_ID = ""
        cmd_header = "sudo -u spark spark-submit "
        cmd_repara = ["--class", "--master", "--deploy-mode", "--num-executors", "--executor-cores",
                      "--executor-memory"]

        for i in range(len(cmd_repara)):
            value = re_args.get(cmd_repara[i])
            cmd_header = cmd_header + " " + cmd_repara[i] + " " + value

        cmd_header = cmd_header + " " + jar

        for i in range(len(al_args)):
            cmd_header = cmd_header + " " + al_args[i]

        filename = '/root/applicationID_in_Yarn_'+ missionid

        websocketPath = "/home/spark/Log/"
        if os.path.exists(websocketPath+"MISSION_"+missionid+"_"+flowID):
            os.remove(websocketPath+"MISSION_"+missionid+"_"+flowID)

        appfile = open(filename, 'a')

        proc = subprocess.Popen(cmd_header, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while True:
            line = proc.stdout.readline()


            with open(websocketPath+"MISSION_"+missionid+"_"+flowID,"a") as log:
                log.write(line)

            flag = line.find("Submitted application application_")
            if flag >= 0:
                start = line.find("application_")
                app_ID = line[start:]
                appfile.write(flowID+","+app_ID)
            if not line:
                break
            print line
        appfile.close()
        # for l in iter(proc.stdout.readline(), ''):
        #     sys.stdout.write(l)
        print  app_ID
        proc.wait()

    def setTomatrix(self):
        self.matrix = [[0]*len(self.graph_node) for i in range(len(self.graph_node))]
        for i in range(len(self.graph_node)):
            for j in range(len(self.graph_node)):
                if (self.graph_node[i], self.graph_node[j]) in self.graph_edge:
                    self.matrix[i][j] = 1
        return

    def dataflowgraph(self):
        queue = Queue.Queue()
        stack = []

        outdegree = []
        endpoints = []
        for i in range(len(self.graph_node)):
            outdegree.append(0)
            endpoints.append(0)

        for i in range(len(self.graph_node)):
            for j in range(len(self.graph_node)):
                if self.matrix[i][j] == 1:
                    outdegree[i] += 1

        for i in range(len(self.graph_node)):
            if outdegree[i] == 0:
                endpoints[i] = 1
                stack.append(self.graph_node[i])
                queue.put(i)

        while not queue.empty():
            curr = queue.get()
            for i in range(len(self.graph_node)):
                if self.matrix[i][curr] == 1:
                    outdegree[i] -= 1
                    if outdegree[i] == 0 and self.graph_node[i] not in stack:
                        stack.append(self.graph_node[i])
                    if outdegree[i] == 0:
                        queue.put(i)

        return stack


    def dataflowgraph2(self):
        queue = Queue.Queue()
        stack = []
        dataflow = []

        outdegree = []
        endpoints = []
        for i in range(len(self.graph_node)):
            outdegree.append(0)
            endpoints.append(0)

        for i in range(len(self.graph_node)):
            for j in range(len(self.graph_node)):
                if self.matrix[i][j] == 1:
                    outdegree[i] += 1

        temp = []
        for i in range(len(self.graph_node)):
            if outdegree[i] == 0:
                endpoints[i] = 1
                stack.append(self.graph_node[i])
                temp.append(self.graph_node[i])
                queue.put(i)
        dataflow.append(temp)

        while not queue.empty():
            curr = queue.get()
            temp = []
            for i in range(len(self.graph_node)):
                if self.matrix[i][curr] == 1:
                    outdegree[i] -= 1
                    if outdegree[i] == 0 and self.graph_node[i] not in stack:
                        stack.append(self.graph_node[i])
                        temp.append(self.graph_node[i])
                    if outdegree[i] == 0:
                        queue.put(i)
            if temp != []:
                dataflow.append(temp)

        return dataflow

    def scheduler(self, missionid, dataflowgraph=[], paras={}):
        print dataflowgraph
        while len(dataflowgraph) != 0:
            curr_proc = dataflowgraph.pop()
            para = paras.get(curr_proc)

            jar = para["jar"]

            repara = ["--class", "--master", "--deploy-mode", "--num-executors", "--executor-cores",
                      "--executor-memory"]
            re_args = {}
            for i in range(len(repara)):
                re_args[repara[i]] = para["op_para"][repara[i]]

            al_args = []
            for i in range(len(para["al_para"]["sort"])):
                al_args.append(para["al_para"][para["al_para"]["sort"][i]])

            print al_args
            print re_args


            self.submit(missionid, curr_proc, jar, al_args, re_args)


    def run(self, NodeEdge, paras):

        missionID = paras["missionID"]
        self.updatePara(paras, NodeEdge["processes"])
        self.split(NodeEdge)
        self.setTomatrix()
        print self.matrix, self.graph_node
        dataflowgraph = self.dataflowgraph2()
        print dataflowgraph
        print paras
        finalNode = []
        flowlen = len(dataflowgraph)
        for i in range(len(dataflowgraph)):
            Thread = []
            lis = dataflowgraph.pop()
            for j in lis:
                para = {}
                dataflownode = []
                para[j] = paras[j]
                dataflownode.append(j)
                Thread.append(SchedulerThread(missionID, dataflownode, para, "scheduler" + j))
                mission = Mission.objects.get(id=missionID)
                mission.missionStatus = 1
                mission.save()
            for t in range(len(Thread)):
                thr = Thread[t]
                thr.start()
            for t in range(len(Thread)):
                thr = Thread[t]
                thr.join()

            if i == flowlen - 1:
                finalNode = lis


        status = {'status':{}}
        with open('/root/applicationID_in_Yarn_'+ missionID, 'r') as appfile:
            for line in appfile.readlines():
                tup = line.split(',')
                mot = monitor.SparkMonitor('10.5.0.223','8088')
                ret = mot.appInfo(tup[1].replace("\n",""))
                appinfo = self.byteify(json.loads(ret[2]))
                status['status'][tup[0]] = appinfo['app']['finalStatus']
        print 1111111

        if os.path.exists("/root/applicationID_in_Yarn_"+missionID):
            os.remove("/root/applicationID_in_Yarn_"+missionID)
        print finalNode
        for i in finalNode:
            if status["status"][i] == "SUCCEEDED":
                for j in paras[i]["al_para"]["sort"]:
                    if j.find("output") >= 0 :
                        mission = Mission.objects.get(id=int(missionID))
                        print paras[i]["al_para"][j].split("/")[-1],paras[i]["al_para"][j]
                        resultfile = ResultFile(resultName=paras[i]["al_para"][j].split("/")[-1], resultPath=paras[i]["al_para"][j],
                                                missionId=mission)
                        resultfile.save()
        return status


# if __name__ == "__main__":
#     NodeEdge = {
#         "sources": [
#             {
#                 "id": "1",
#                 "label": "data",
#                 "description": "data",
#                 "flowID": "0"
#             }
#         ],
#         "processes": [
#             {
#                 "id": "1",
#                 "label": "naiveBayes",
#                 "description": "naive bayes",
#                 "flowID": "1",
#                 "parameters": [
#                     {
#                         "label": "a",
#                         "slug":"featureNumber",
#                         "controlType": "text",
#                         "val": "5"
#                     },
#                     {
#                         "label": "lemda",
#                         "slug":"lemda",
#                         "controlType": "float",
#                         "val": "1.0"
#                     },
#                     {
#                         "label": "modeltype",
#                         "slug":"modeltype",
#                         "controlType": "select",
#                         "val": "0",
#                         "options": [
#                             "multi-nominal",
#                             "bernoulli"
#                         ]
#                     }
#                 ]
#             },
#             {
#                 "id": "1",
#                 "label": "naiveBayes",
#                 "description": "naive bayes",
#                 "flowID": "2",
#                 "algorithmParameters": [
#                     {
#                         "label": "a",
#                         "slug": "featureNumber",
#                         "controlType": "text",
#                         "val": "5"
#                     },
#                     {
#                         "label": "lemda",
#                         "slug": "lemda",
#                         "controlType": "float",
#                         "val": "1.0"
#                     },
#                     {
#                         "label": "modeltype",
#                         "slug": "modeltype",
#                         "controlType": "select",
#                         "val": "0",
#                         "options": [
#                             "multi-nominal",
#                             "bernoulli"
#                         ]
#                     }
#                 ]
#             },
#             {
#                 "id": "1",
#                 "label": "naiveBayes",
#                 "description": "naive bayes",
#                 "flowID": "3",
#                 "algorithmParameters": [
#                     {
#                         "label": "a",
#                         "slug": "featureNumber",
#                         "controlType": "text",
#                         "val": "5"
#                     },
#                     {
#                         "label": "lemda",
#                         "slug": "lemda",
#                         "controlType": "float",
#                         "val": "1.0"
#                     },
#                     {
#                         "label": "modeltype",
#                         "slug": "modeltype",
#                         "controlType": "select",
#                         "val": "0",
#                         "options": [
#                             "multi-nominal",
#                             "bernoulli"
#                         ]
#                     }
#                 ]
#             }
#         ],
#         "paths": [
#             "0->1", "1->2", "3->2"
#         ]
#     }
#
#     dataflowgraph = ["3", "2", "1"]
#     paras = {"1": {"jar": "myalgorithm2.jar",
#                    "op_para": {"--class": "com.idf", "--master": "yarn", "--deploy-mode": "cluster",
#                                "--num-executors": "1", "--executor-cores": "2", "--executor-memory": "1G"},
#                    "al_para": {"sort": ["input", "output-train"
#                        , "output-test", "flag"], "input": "/user/spark/input/Sample_data.txt",
#                                "output-train": "/user/spark/tempfile/traindata"
#                        , "output-test": "/user/spark/tempfile/testdata", "flag": "1"}},
#              "2": {"jar": "myalgorithm2.jar",
#                    "op_para": {"--class": "com.naivebayes", "--master": "yarn", "--deploy-mode": "cluster",
#                                "--num-executors": "1", "--executor-cores": "2", "--executor-memory": "1G"},
#                    "al_para": {"sort": ["input-train",
#                                         "output-model"], "input-train": "/user/spark/tempfile/traindata.csv",
#                                "output-model": "/user/spark/model/NaiveBayes"}},
#              "3": {"jar": "myalgorithm2.jar",
#                    "op_para": {"--class": "com.loadmodel", "--master": "yarn", "--deploy-mode": "cluster",
#                                "--num-executors": "1", "--executor-cores": "2", "--executor-memory": "1G"},
#                    "al_para": {"sort": ["input-test",
#                                         "input-model"], "input-test": "/user/spark/tempfile/testdata.csv",
#                                "input-model": "/user/spark/model/NaiveBayes"}}
#              }
#
#     scheduler = Spark_Scheduler()
#
#     # cmd_header = "spark-submit --class com.StreamProcess.classifierStreaming /home/cloudera/IdeaProjects/Anomaly_Detection/out/artifacts/Anomaly_Detection_jar/Anomaly_Detection.jar"
#     # proc = subprocess.Popen(cmd_header, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#     # while True:
#     #     line = proc.stdout.readline()
#     #     flag = line.find("Submitted application application_")
#     #     if flag >= 0:
#     #         start = line.find("application_")
#     #     if not line:
#     #         break
#     #     print line
#     #
#     scheduler.updatePara(paras, NodeEdge["processes"])
#     scheduler.split(NodeEdge)
#     scheduler.setTomatrix()
#     print scheduler.matrix, scheduler.graph_node
#     dataflowgraph = scheduler.dataflowgraph2()
#     print dataflowgraph
#
#
#     for i in range(len(dataflowgraph)):
#         Thread = []
#         lis = dataflowgraph.pop()
#         for j in lis:
#             para = {}
#             dataflownode = []
#             para[j] = paras[j]
#             dataflownode.append(j)
#             Thread.append(SchedulerThread(dataflownode, para, "scheduler"+j))
#         for t in range(len(Thread)):
#             thr = Thread[t]
#             thr.start()
#         for t in range(len(Thread)):
#             thr = Thread[t]
#             thr.join()
#         print "dsdfafd"
#
#
#     # scheduler.scheduler(dataflowgraph, paras)
