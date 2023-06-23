# Databricks notebook source
# MAGIC %pip install pdfplumber
# MAGIC %pip install psycopg2-binary==2.9.5
# MAGIC %pip install sqlalchemy

# COMMAND ----------

containerName = "landing"
storageAccountName = "sharifstdataplatform"
sas = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-06-23T08:14:57Z&st=2023-06-23T00:14:57Z&spr=https,http&sig=dYQkpp%2B3du%2B0pMhnxtaPMDH0yEDiFcfgJ2nC7h6YcQ8%3D"
url = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
mountPoint = "/mnt/demo"

if any(mount.mountPoint == "/mnt/demo" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount("/mnt/demo")
dbutils.fs.mount(
  source = url,
  mount_point = mountPoint,
  extra_configs = {config:sas})

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

import pdfplumber
import re
import pandas as pd
import numpy as np

text = ""

stringDegrees = ""
listDegrees = []
listStats = []

listFiles = dbutils.fs.ls("/mnt/demo/ges/nus/")

dataFrame = pd.DataFrame(columns=['university','survey_year','degree','employment_percentage','ft_employment_percentage','basic_mean','basic_median','gross_mean','gross_median','gross_low', 'gross_high'])

for fileName in listFiles:
    stringDegrees = ""
    listDegrees = []
    listStats = []
    path = r'/dbfs/mnt/demo/ges/nus/' + fileName.name
    with pdfplumber.open(path) as pdf:
        count = 0
        for page in pdf.pages:
            text = page.extract_text() + ' '
            if(count == 0):
                year = int(re.search(r'(?<=NUS:\s)\d+',text).group(0))
                print("year = ", year)
            count = 1
            list = re.finditer(r'(([\.\d]+%\s){2}(\$[,\d]+\s){6})|((N\.A\.\s){8})',text)
            list2 = re.finditer(r'(^(Bachelor)[a-zA-Z\(\)\s]+(\s{2,}))|(^[^\d\n]*)',text, flags=re.MULTILINE)
            for row in list:
                listStats.append(row.group(0).replace(',','').replace('$','').replace('%','').strip())

            tempStr = ""
            for row in list2:
                if("school" in row.group(0).strip().lower() or 
                    "faculty" in row.group(0).strip().lower() or
                    "programme" in row.group(0).strip().lower() or
                    "n.a." in row.group(0).strip().lower() or
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
    for x in range(len(listDegrees)):
        statList = listStats[x].split(" ")
        row = ["NUS", year]
        row.append(listDegrees[x])
        row.extend(statList)
        dataFrame.loc[len(dataFrame.index)] = row
dataFrame.replace('N.A.', '0', inplace=True)
dataFrame = dataFrame.astype({'employment_percentage':'float','ft_employment_percentage':'float','basic_mean':'int','basic_median':'int','gross_mean':'int','gross_median':'int','gross_low':'int','gross_high':'int'})

print(dataFrame)
    

# COMMAND ----------

import sqlalchemy as sql

serverType="postgresql"
username="psqladmin"
password="HASh1CoR3!"
hostname="sharif-psqlflexibleserver.postgres.database.azure.com"
port="5432"
database="dataplatform"
connectionStr = serverType + "://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database
try:
    engine = sql.create_engine(connectionStr,connect_args={'sslmode' : 'require'})
except Exception as e:
        raise Exception('Error establishing connection with Database: ' + str(e))

dataFrame.to_sql('ges_salary_table',engine, if_exists='append',index=False)

# COMMAND ----------

for filename in dbutils.fs.ls("/mnt/demo/ges/nus/"):
    dbutils.fs.mv(filename.path,'/mnt/demo/ges-archive/nus/' + filename.name)
