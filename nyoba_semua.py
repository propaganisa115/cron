from concurrent import futures
from datetime import datetime, timedelta, date
import requests
import concurrent.futures
from requests.structures import CaseInsensitiveDict
import json
import threading
from threading import *
import time
from concurrent.futures import ThreadPoolExecutor
import collections
from google.cloud import storage
import math

url = "http://api.seonindonesia.net/sekolah"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "Basic dXNlcDp1c2Vw"


resp = requests.get(url, headers=headers)

resp = json.loads(resp.text)

datas=[]
def ambilData(key):
    datas.append({'urlSeonpoint':"https://"+key['sekolah_domain']+"/api/main/seonpoint",\
    'urlActivity_user':"https://"+key['sekolah_domain']+"/api/main/activity",\
    'urlLoguseractive':"https://"+key['sekolah_domain']+"/api/main/log",\
    'urlSekolah':"https://"+key['sekolah_domain']+"/api/main/sekolah",\
    'urlModul':"https://"+key['sekolah_domain']+"/api/main/modul"})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData,key)for key in resp]


for key in datas:
    urlSeonpoint = key['urlSeonpoint']
    headers = CaseInsensitiveDict()
    headers["Authorization"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiU0VPTkFETTAwMSIsImZ1bGxuYW1lIjoiU2VvbiBBZG1pbiIsImVtYWlsIjoic2VvbmFkbWluQHNlb24uY29tIiwidHlwZSI6InN1cGVyX2FkbWluIiwiQVBJX1RJTUUiOjE2MjcwMjM3Mzl9.4HiC4XnwiQ6CpfpCg_IVqVQeLr0pwqT-pIEjRPb6dvQ"
    headers["Content-Type"] = "application/json"
    headers["Content-Length"] = "0"
    
    today=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        resp = requests.post(urlSeonpoint, headers=headers)


        urlLogSchedulerGlobal = "https://api.seonindonesia.net/log_scheduler_global/create"
        headersPython2 = CaseInsensitiveDict()
        headersPython2["Accept"] = "application/json"
        headersPython2["Authorization"] = "Basic YW5pc2E6YW5pc2E="
        headersPython2["Content-Type"] = "application/json"
        
        if(resp.status_code==200):
            print("jalan")
        else:
            log_scheduler_global = {"domain":"dapurseon.xyz","record_time": today ,"status":"failed","error_message":"response return http error with status code"+resp.status_code  }
            log_scheduler_global =str(log_scheduler_global).replace("'", '"')
            resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler_global = {"domain":"dapurseon.xyz","record_time": today ,"status":"failed","error_message":error }
        log_scheduler_global =str(log_scheduler_global).replace("'", '"')
        resp = requests.post(urlLogSchedulerGlobal, headers=headersPython2, data=log_scheduler_global)



