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

urlSeonpoint = "https://dapurseon.xyz/api/main/seonpoint"
urlActivity_user = "https://dapurseon.xyz/api/main/activity"
urlLoguseractive = "https://dapurseon.xyz/api/main/log"
urlSekolah = "https://dapurseon.xyz/api/main/sekolah"
urlModul = "https://dapurseon.xyz/api/main/modul"


headers = CaseInsensitiveDict()
headers["Authorization"] = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiU0VPTkFETTAwMSIsImZ1bGxuYW1lIjoiU2VvbiBBZG1pbiIsImVtYWlsIjoic2VvbmFkbWluQHNlb24uY29tIiwidHlwZSI6InN1cGVyX2FkbWluIiwiQVBJX1RJTUUiOjE2MjcwMjM3Mzl9.4HiC4XnwiQ6CpfpCg_IVqVQeLr0pwqT-pIEjRPb6dvQ"
headers["Content-Type"] = "application/json"
headers["Content-Length"] = "0"

yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
filter = {"start_date":yesterday,"end_date":yesterday}
filter = str(filter).replace("'", '"')

yesterdayOnlydate = (datetime.now().date() - timedelta(1)).strftime('%Y-%m-%d')
filterOnlydate = {"date":yesterdayOnlydate}
filterOnlydate = str(filterOnlydate).replace("'", '"')

resp = requests.post(urlSeonpoint, headers=headers,data=filter)
#resp = requests.post(urlSeonpoint, headers=headers)
resp_json = json.loads(resp.text)

resp_activityuser = requests.post(urlActivity_user, headers=headers, data=filter)
#resp_activityuser = requests.post(urlActivity_user, headers=headers)
resp_json_activityuser = json.loads(resp_activityuser.text)

resp_loguseractive = requests.post(urlLoguseractive, headers=headers, data=filter)
#resp_loguseractive = requests.post(urlLoguseractive, headers=headers)
resp_json_loguseractive = json.loads(resp_loguseractive.text)

resp_modul = requests.post(urlModul, headers=headers,data=filterOnlydate)
#resp_loguseractive = requests.post(urlLoguseractive, headers=headers)
resp_json_modul = json.loads(resp_modul.text)


resp_sekolah = requests.get(urlSekolah, headers=headers)
resp_json_sekolah = json.loads(resp_sekolah.text)

if type(resp_json_loguseractive['data']=="<class 'dict'>"):
    resp_json_loguseractive['data']=[resp_json_loguseractive['data']]

#if type(resp_json_sekolah['data']=="<class 'dict'>"):
#    resp_json_sekolah['data']=[resp_json_sekolah['data']]

datas=[]
def ambilData(key):
    datas.append({'id':key['id'],'id_user':key['id_user'],'type_user':key['type_user'],'action':key['action'],'point':key['point'],'keterangan':key['keterangan'],'record_time':key['record_time']})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData,key)for key in resp_json['data']]

datas_activityuser=[]
def ambilData_activityuser(key):
    datas_activityuser.append({'id_activity':key['id_activity'],'tipe_user':key['tipe_user'],'user_id':key['user_id'],'aktivitas':key['aktivitas'],'id_ref_activity':key['id_ref_activity'],'id_jadwal_pelajaran':key['id_jadwal_pelajaran'],'datetime':key['datetime'],'type_activity':key['type_activity'],'tb_reff':key['tb_reff'],'id_user':key['id_user'],'fullname':key['fullname'],'foto_profile':key['foto_profile'],'cover':key['cover'],'level_user':key['level_user'],'deskripsi':key['deskripsi']})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData_activityuser,key)for key in resp_json_activityuser['activities']]

datas_loguseractive=[]
def ambilData_loguseractive(key):
    datas_loguseractive.append({'users_active':key['users_active'],'sekolah':key['sekolah'],'token_sekolah':key['token_sekolah']})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData_loguseractive,key)for key in resp_json_loguseractive['data']]

datas_modul=[]
def ambilData_modul(key):
    datas_modul.append({'subject_id':key['subject_id'],'name':key['name'],'id_schedule':key['id_schedule'],'guru':key['guru'],'id_modul':key['id'],'title':key['title'],'description':key['description'],'file_name':key['file_name'],'extension':key['extension'],'tipe_modul':key['tipe_modul'],'id_subject':key['id_subject'],'id_guru':key['id_guru'],'id_class':key['id_class'],'id_jadwal_pelajaran':key['id_jadwal_pelajaran'],'link':key['link'],'cover':key['cover'],'upload':key['upload'],'tipe_link':key['tipe_link'],'deleted':key['deleted'],'type':key['type'],'created_at':key['created_at'],'cover_path':key['cover_path'],'file_path':key['file_path'],'subject_name':key['subject_name']})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData_modul,key)for key in resp_json_modul['data']]

datas_sekolah=[]
def ambilData_sekolah(key):
    if key['id_sekolah']==None :
        key['id_sekolah']=''
    if key['seon_token']==None :
        key['seon_token']=''      
    if key['npsn']==None :
        key['npsn']=''
    if key['nm_sekolah']==None :
        key['nm_sekolah']=''
    if key['kepala_sekolah']==None :
        key['kepala_sekolah']=''
    if key['logo']==None :
        key['logo']=''
    if key['alamat']==None :
        key['alamat']=''
    if key['email']==None :
        key['email']=''
    if key['phone']==None :
        key['phone']=''
    if key['location']==None :
        key['location']=''
    if key['nip_kepsek']==None :
        key['nip_kepsek']=''
    if key['visi']==None :
        key['visi']=''
    if key['id_bendahara']==None :
        key['id_bendahara']=''
    if key['facebook']==None :
        key['facebook']=''
    if key['twitter']==None :
        key['twitter']=''
    if key['instagram']==None :
        key['instagram']=''
    if key['youtube']==None :
        key['youtube']=''
    if key['access_token']==None:
        key['access_token']=''
    if key['banner']==None :
        key['banner']=''
    if key['cover_sekolah']==None :
        key['cover_sekolah']=''
    if key['logo_depan']==None :
        key['logo_depan']=''
    if key['judul_depan']==None :
        key['judul_depan']=''
    if key['des_depan']==None :
        key['des_depan']=''
    datas_sekolah.append({'id_sekolah':key['id_sekolah'],'seon_token':key['seon_token'],'npsn':key['npsn'],'nm_sekolah':key['nm_sekolah'],'kepala_sekolah':key['kepala_sekolah'],'logo':key['logo'],'alamat':key['alamat'],'email':key['email'],'phone':key['phone'],'location':key['location'],'nip_kepsek':key['nip_kepsek'],'visi':key['visi'],'id_bendahara':key['id_bendahara'],'facebook':key['facebook'],'twitter':key['twitter'],'instagram':key['instagram'],'youtube':key['youtube'],'access_token':key['access_token'],'banner':key['banner'],'cover_sekolah':key['cover_sekolah'],'logo_depan':key['logo_depan'],'judul_depan':key['judul_depan'],'des_depan':key['des_depan']})

with ThreadPoolExecutor(max_workers=None) as exec:
    fut = [exec.submit(ambilData_sekolah,key)for key in resp_json_sekolah['data']]

urlSeon = "https://api.seonindonesia.net/seon_point/create"
urlLog_scheduler = "https://api.seonindonesia.net/log_scheduler/create"
urlActivityuser = "https://api.seonindonesia.net/activity_user/create"
urlLoguseractive = "https://api.seonindonesia.net/log_user_active/create"
urlSekolah = "https://api.seonindonesia.net/sekolah_detail/update/1" #ganti update setelah ngambil semua data


headersPython = CaseInsensitiveDict()
headersPython["Accept"] = "application/json"
headersPython["Authorization"] = "Basic YW5pc2E6YW5pc2E="
headersPython["Content-Type"] = "application/json"


def tambahData(key):
    TambahData = {"id_seon":key['id'] ,"id_user": key['id_user'] ,"type_user":  key['type_user'] ,"action":  key['action'] ,"point":  key['point'] ,"keterangan": key['keterangan'] ,"record_time":  key['record_time'] }
    tambahdatas=str(TambahData).replace("'", '"')
    resp = requests.post(urlSeon, headers=headersPython, data=tambahdatas)

def tambahData_activityuser(key):
    TambahData_activityuser = {'id_activity':key['id_activity'],'tipe_user':key['tipe_user'],'user_id':key['user_id'],'aktivitas':key['aktivitas'],'id_ref_activity':key['id_ref_activity'],'id_jadwal_pelajaran':key['id_jadwal_pelajaran'],'datetime':key['datetime'],'type_activity':key['type_activity'],'tb_reff':key['tb_reff'],'id_user':key['id_user'],'fullname':key['fullname'],'foto_profile':key['foto_profile'],'cover':key['cover'],'level_user':key['level_user'],'deskripsi':key['deskripsi']}
    tambahdatas_activityuser=str(TambahData_activityuser).replace("'", '"')
    resp = requests.post(urlActivityuser, headers=headersPython, data=tambahdatas_activityuser)

def tambahData_sekolah(key):
    TambahData_sekolah ={'id_sekolah':key['id_sekolah'],'seon_token':key['seon_token'],'npsn':key['npsn'],'nm_sekolah':key['nm_sekolah'],'kepala_sekolah':key['kepala_sekolah'],'logo':key['logo'],'alamat':key['alamat'],'email':key['email'],'phone':key['phone'],'location':key['location'],'nip_kepsek':key['nip_kepsek'],'visi':key['visi'],'id_bendahara':key['id_bendahara'],'facebook':key['facebook'],'twitter':key['twitter'],'instagram':key['instagram'],'youtube':key['youtube'],'access_token':key['access_token'],'banner':key['banner'],'cover_sekolah':key['cover_sekolah'],'logo_depan':key['logo_depan'],'judul_depan':key['judul_depan'],'des_depan':key['des_depan']}
    tambahdatas_sekolah =str(TambahData_sekolah).replace("'", '"')
    resp = requests.post(urlSekolah, headers=headersPython, data=tambahdatas_sekolah)

def tambahData_loguseractive(key):
    TambahData_loguseractive ={'users_active':key['users_active'],'sekolah':key['sekolah'],'token_sekolah':key['token_sekolah']}
    tambahdatas_loguseractive =str(TambahData_loguseractive).replace("'", '"')
    resp = requests.post(urlLoguseractive, headers=headersPython, data=tambahdatas_loguseractive)

today=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

urlModul = "https://api.seonindonesia.net/modul/create" 
def tambahData_modul(key):
    if key['subject_id']:
        key['subject_id']=int(key['subject_id'])
    else:
        key['subject_id']=0
        #print(key['subject_id'])
    if key['id_modul']:
        key['id_modul']=int(key['id_modul'])
    else:
        key['id_modul']=0
    if key['id_subject']:
        key['id_subject']=int(key['id_subject'])
    else:
        key['id_subject']=0
    if key['id_class']:
        key['id_class']=int(key['id_class'])
    else:
        key['id_class']=0

    TambahData_modul ={'subject_id':key['subject_id'],'name':key['name'],'id_schedule':key['id_schedule'],'guru':key['guru'],'id_modul':key['id_modul'],'title':key['title'],'description':key['description'],'file_name':key['file_name'],'extension':key['extension'],'tipe_modul':key['tipe_modul'],'id_subject':key['id_subject'],'id_guru':key['id_guru'],'id_class':key['id_class'],'id_jadwal_pelajaran':key['id_jadwal_pelajaran'],'link':key['link'],'cover':key['cover'],'upload':key['upload'],'tipe_link':key['tipe_link'],'deleted':key['deleted'],'type':key['type'],'created_at':key['created_at'],'cover_path':key['cover_path'],'file_path':key['file_path'],'subject_name':key['subject_name'],'share':'off'}
    tambahdatas_modul =str(TambahData_modul).replace("'", '"')
    resp = requests.post(urlModul, headers=headersPython, data=tambahdatas_modul)

with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        error=""
        futures = [exec.submit(tambahData,key)for key in datas]
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "seon_point" ,"record_time": today ,"status":"sucess","error_message":"" }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "seon_point" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)#print("finish recorded scheduler")

with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        error=""
        futures_activityuser = [exec.submit(tambahData_activityuser,key)for key in datas_activityuser]
        log_scheduler_activityuser = {"domain":"dapurseon.xyz" ,"kode_scheduler": "activity_user" ,"record_time": today ,"status":"sucess","error_message":""  }
        log_scheduler_activityuser =str(log_scheduler_activityuser).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler_activityuser)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "activity_user" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)

with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        error=""
        futures_loguseractive = [exec.submit(tambahData_loguseractive,key)for key in datas_loguseractive]
        log_scheduler_loguseractive = {"domain":"dapurseon.xyz" ,"kode_scheduler": "log_user_active" ,"record_time": today ,"status":"sucess","error_message":""  }
        log_scheduler_loguseractive =str(log_scheduler_loguseractive).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler_loguseractive)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "log_user_active" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)

with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        error=""
        futures_sekolah = [exec.submit(tambahData_sekolah,key)for key in datas_sekolah]
        log_scheduler_sekolah = {"domain":"dapurseon.xyz" ,"kode_scheduler": "sekolah_detail(cron)" ,"record_time": today ,"status":"sucess","error_message":"" }
        log_scheduler_sekolah =str(log_scheduler_sekolah).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler_sekolah)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "sekolah_detail(cron)" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)

with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        error=""
        futures_modul = [exec.submit(tambahData_modul,key)for key in datas_modul]
        log_scheduler_modul = {"domain":"dapurseon.xyz" ,"kode_scheduler": "modul" ,"record_time": today ,"status":"sucess","error_message":"" }
        log_scheduler_modul =str(log_scheduler_modul).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler_modul)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "modul" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)


### SET THESE VARIABLES ###
PROJECT_ID = "utopian-cistern-299708"
CLOUD_STORAGE_BUCKET = "sekolahan-online"
###########################

storage_client = storage.Client()
bucket = storage_client.bucket(CLOUD_STORAGE_BUCKET)
blobs = bucket.list_blobs()

total=0
for blob in blobs:
    total +=blob.size
size=str(total)

url = "https://api.seonindonesia.net/sekolah/update/3"

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "Basic YW5pc2E6YW5pc2E="
headers["Content-Type"] = "application/json"

datas ={"storage_usage": size}
datas=str(datas).replace("'", '"')
with ThreadPoolExecutor(max_workers=None) as exec:
    try:
        resp = requests.patch(url, headers=headers, data=datas)
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "update_size_storage_usage" ,"record_time": today ,"status":"sucess","error_message":""  }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)
    except Exception as e:
        error=str(e).replace("'", "")
        log_scheduler = {"domain":"dapurseon.xyz" ,"kode_scheduler": "update_size_storage_usage" ,"record_time": today ,"status":"sucess","error_message":error }
        log_scheduler =str(log_scheduler).replace("'", '"')
        resp = requests.post(urlLog_scheduler, headers=headersPython, data=log_scheduler)
