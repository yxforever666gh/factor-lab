#!/usr/bin/env python3
from __future__ import annotations
import json, os, urllib.request, time
KEY=os.environ.get('DIEMENG_API_KEY')
BASE='https://mg.diemeng.chat/api'
paths=['/stock/balancesheet','/stock/cashflow','/stock/financial_indicator']
payloads=[
 {'stock_code':'000001.SZ','page':1,'page_size':5},
 {'stock_code':'600000.SH','page':1,'page_size':5},
 {'stock_code':'000001.SZ','start_date':'20230101','end_date':'20241231','page':1,'page_size':5},
 {'stock_code':'000001.SZ','ann_date':'20250315','page':1,'page_size':5},
 {'stock_code':'000001.SZ','end_date':'20241231','page':1,'page_size':5},
 {'stock_code':'000001.SZ','period':'20241231','page':1,'page_size':5},
 {'stock_code':'000001.SZ','report_period':'20241231','page':1,'page_size':5},
 {'end_date':'20241231','page':1,'page_size':5},
 {'ann_date':'20250315','page':1,'page_size':5},
]
def post(path,payload):
    req=urllib.request.Request(BASE+path,data=json.dumps(payload).encode(),method='POST',headers={'apiKey':KEY,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=25) as r:
            raw=r.read().decode('utf-8','replace')
            try: obj=json.loads(raw)
            except Exception: obj={'raw':raw[:300]}
            return r.status,obj
    except Exception as e:
        return None,{'error':type(e).__name__+': '+str(e)}
def rows(obj):
    for k in ['data','result']:
        v=obj.get(k) if isinstance(obj,dict) else None
        if isinstance(v,list): return [x for x in v if isinstance(x,dict)]
        if isinstance(v,dict):
            for sk in ['list','rows','items','records']:
                sv=v.get(sk)
                if isinstance(sv,list): return [x for x in sv if isinstance(x,dict)]
    return []
summary={}
for path in paths:
    print('\nPATH',path)
    summary[path]=[]
    for p in payloads:
        st,obj=post(path,p)
        rs=rows(obj)
        msg=(obj.get('msg') or obj.get('message') or obj.get('error') or obj.get('raw')) if isinstance(obj,dict) else None
        item={'payload':p,'status':st,'code':obj.get('code') if isinstance(obj,dict) else None,'msg':msg,'rows':len(rs),'cols':list(rs[0].keys())[:60] if rs else []}
        summary[path].append(item)
        print('payload',p,'status',item['status'],'code',item['code'],'msg',str(item['msg'])[:120],'rows',item['rows'],'cols',item['cols'][:15])
        if rs:
            print(' date fields', {k:v for k,v in rs[0].items() if 'date' in k.lower() or 'time' in k.lower()})
            break
        time.sleep(.1)
open('artifacts/diemeng_financial_param_probe_2026-05-06.json','w').write(json.dumps(summary,ensure_ascii=False,indent=2))
