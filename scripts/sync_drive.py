#!/usr/bin/env python3
import os,io,json,hashlib,base64,logging,argparse,mimetypes,requests
from pathlib import Path
from datetime import datetime,timezone
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload,MediaIoBaseUpload

logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
log=logging.getLogger(__name__)

GITHUB_USER=os.environ["GITHUB_USER"]
GITHUB_TOKEN=os.environ["GITHUB_TOKEN"]
GDRIVE_CREDS_JSON=os.environ["GDRIVE_CREDENTIALS"]
GDRIVE_ROOT_ID=os.environ["GDRIVE_FOLDER_ID"]
GITHUB_API="https://api.github.com"
SCOPES=["https://www.googleapis.com/auth/drive"]
IGNORE={".git",".DS_Store","__pycache__","node_modules"}

def get_drive():
    c=service_account.Credentials.from_service_account_info(json.loads(GDRIVE_CREDS_JSON),scopes=SCOPES)
    return build("drive","v3",credentials=c,cache_discovery=False)

def gh_h():
    return {"Authorization":f"Bearer {GITHUB_TOKEN}","Accept":"application/vnd.github+json"}

def all_repos():
    repos,page=[],1
    while True:
        r=requests.get(f"{GITHUB_API}/users/{GITHUB_USER}/repos",headers=gh_h(),params={"per_page":100,"page":page,"type":"owner"},timeout=30)
        r.raise_for_status()
        b=r.json()
        if not b:break
        repos.extend(b);page+=1
    log.info(f"Found {len(repos)} repos")
    return repos

def repo_tree(name):
    for br in ["main","master","HEAD"]:
        r=requests.get(f"{GITHUB_API}/repos/{GITHUB_USER}/{name}/git/trees/{br}",headers=gh_h(),params={"recursive":"1"},timeout=30)
        if r.status_code==200:return[i for i in r.json().get("tree",[])if i["type"]=="blob"]
        if r.status_code==409:return[]
    return[]

def gh_file(repo,path):
    r=requests.get(f"{GITHUB_API}/repos/{GITHUB_USER}/{repo}/contents/{path}",headers=gh_h(),timeout=30)
    if r.status_code==404:return None
    r.raise_for_status()
    d=r.json()
    if d.get("encoding")=="base64":return base64.b64decode(d["content"])
    dl=d.get("download_url")
    return requests.get(dl,timeout=60).content if dl else None

def push_gh(repo,path,content,msg):
    url=f"{GITHUB_API}/repos/{GITHUB_USER}/{repo}/contents/{path}"
    r=requests.get(url,headers=gh_h(),timeout=15)
    body={"message":msg,"content":base64.b64encode(content).decode()}
    if r.status_code==200:body["sha"]=r.json()["sha"]
    requests.put(url,headers=gh_h(),json=body,timeout=30).raise_for_status()

def skip(p):
    parts=Path(p).parts
    return any(x in IGNORE or x.startswith(".") for x in parts)

def drive_folder(svc,name,parent):
    q=f"name='{name}' and mimeType='application/vnd.google-apps.folder' and '{parent}' in parents and trashed=false"
    r=svc.files().list(q=q,fields="files(id)",pageSize=1).execute().get("files",[])
    if r:return r[0]["id"]
    return svc.files().create(body={"name":name,"mimeType":"application/vnd.google-apps.folder","parents":[parent]},fields="id").execute()["id"]

def drive_files(svc,folder):
    out,pt={},None
    while True:
        r=svc.files().list(q=f"'{folder}' in parents and trashed=false",fields="nextPageToken,files(id,name,mimeType,md5Checksum,modifiedTime)",pageSize=1000,pageToken=pt).execute()
        for f in r.get("files",[]):
            if f["mimeType"]=="application/vnd.google-apps.folder":
                for p,m in drive_files(svc,f["id"]).items():out[f"{f['name']}/{p}"]=m
            else:out[f["name"]]=(f["id"],f.get("md5Checksum",""),f.get("modifiedTime",""))
        pt=r.get("nextPageToken")
        if not pt:break
    return out

def drive_up(svc,name,parent,data,fid=None):
    mime=mimetypes.guess_type(name)[0]or"application/octet-stream"
    media=MediaIoBaseUpload(io.BytesIO(data),mimetype=mime,resumable=False)
    if fid:svc.files().update(fileId=fid,media_body=media).execute()
    else:svc.files().create(body={"name":name,"parents":[parent]},media_body=media,fields="id").execute()

def drive_dl(svc,fid):
    buf=io.BytesIO()
    dl=MediaIoBaseDownload(buf,svc.files().get_media(fileId=fid))
    done=False
    while not done:_,done=dl.next_chunk()
    return buf.getvalue()

def md5(d):return hashlib.md5(d).hexdigest()

def to_drive(svc,repo,folder):
    log.info(f"-> Drive: {repo}")
    df=drive_files(svc,folder)
    for b in repo_tree(repo):
        p=b["path"]
        if skip(p):continue
        c=gh_file(repo,p)
        if not c:continue
        parts=p.split("/");par=folder
        for pt in parts[:-1]:par=drive_folder(svc,pt,par)
        fn=parts[-1];ex=df.get(p)
        if ex and ex[1]==md5(c):continue
        drive_up(svc,fn,par,c,ex[0] if ex else None)
        log.info(f"  Drive<- {p}")

def to_github(svc,repo,folder):
    log.info(f"<- GitHub: {repo}")
    df=drive_files(svc,folder)
    now=datetime.now(timezone.utc).isoformat()
    for rp,(fid,dmd5,_) in df.items():
        if skip(rp):continue
        c=gh_file(repo,rp)
        if c and md5(c)==dmd5:continue
        push_gh(repo,rp,drive_dl(svc,fid),f"[Drive->GitHub] {rp} {now}")

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--direction",choices=["to_drive","to_github","both"],default="both")
    args=p.parse_args()
    svc=get_drive();repos=all_repos()
    uf=drive_folder(svc,GITHUB_USER,GDRIVE_ROOT_ID)
    for r in repos:
        if r.get("archived"):continue
        rf=drive_folder(svc,r["name"],uf)
        if args.direction in("to_drive","both"):to_drive(svc,r["name"],rf)
        if args.direction in("to_github","both"):to_github(svc,r["name"],rf)
    log.info("Done")

if __name__=="__main__":main()
