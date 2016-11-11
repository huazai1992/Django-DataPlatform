import subprocess

def readFile(path):
    resultdata = []
    print path
    cmd_header = "sudo -u spark hdfs dfs -cat " + path + ".csv"
    proc = subprocess.Popen(cmd_header, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    while True:
        line = proc.stdout.readline()
        if line == '':
            break
        line = line.replace(")","").replace("(","").replace("\n","").split(",")
        if line == '':
            break
        elem = {}
        elem['NAME'] = line[0]
        elem['VAL'] = int(line[1])
        resultdata.append(elem)
    return resultdata