# Databricks notebook source
# MAGIC %pip install pdfplumber

# COMMAND ----------

containerName = "landing"
storageAccountName = "sharifstdataplatform"
sas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-06-13T15:51:25Z&st=2023-06-13T07:51:25Z&spr=https&sig=tqREQW2xqtbMIb%2BT%2F4mDTLeeBp4K5DIm%2BgGyapgkP0s%3D"
url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
mountPoint = "/mnt/demo"

dbutils.fs.unmount("/mnt/demo")
dbutils.fs.mount(
  source = url,
  mount_point = mountPoint,
  extra_configs = {config:sas})

display(dbutils.fs.ls("/mnt/demo"))

# COMMAND ----------

import pdfplumber
import re
import pandas as pd

text = ""

stringDegrees = ""
listDegrees = []
listStats = []

listFiles = dbutils.fs.ls("/mnt/demo/ges/nus/")

for fileName in listFiles:
    path = r'/dbfs/mnt/demo/ges/nus/' + fileName.name
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() + ' '
            list = re.finditer(r'([\.\d]+%\s){2}(\$[,\d]+\s){6}',text)
            list2 = re.finditer(r'(^(Bachelor)[a-zA-Z\(\)\s]+)|(^[^\d\n]*)',text, flags=re.MULTILINE)
            for row in list:
                listStats.append(row.group(0).strip())

            tempStr = ""
            for row in list2:
                if("school" in row.group(0).strip().lower() or 
                    "faculty" in row.group(0).strip().lower() or
                    "programme" in row.group(0).strip().lower() or  
                    row.group(0).strip() == ""):
                    continue
                else:
                    if (row.group(0).strip().startswith("Bachelor")):
                        tempStr = tempStr + "\n" + row.group(0).strip().replace('*','')
                    else:
                        tempStr = tempStr + " " + row.group(0).strip().replace('*','')
            splitList = tempStr.strip().split("Employed Percentile Percentile Employment")
            splitList = splitList[1].strip().split("Source: Graduate Employment Survey jointly conducted")
            listDegrees.extend(splitList[0].split('\n'))

    dataFrame = pd.DataFrame(columns=['Degree','Employment','Full-Time Employment','Basic Mean','Basic Median','Gross Mean','Gross Median','Gross 25', 'Gross 75'])
    for x in range(len(listDegrees)):
        statList = listStats[x].split(" ")
        row = [listDegrees[x]]
        row.extend(statList)
        dataFrame.loc[len(dataFrame.index)] = row

    print(dataFrame)
    
