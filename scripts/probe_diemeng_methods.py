#!/usr/bin/env python3
"""Supplemental Diemeng method probe. Redacts API key."""
from __future__ import annotations
import json, os, urllib.request, urllib.parse, urllib.error, time
from pathlib import Path

KEY=os.environ.get('DIEMENG_API_KEY')
BASE='https://mg.diemeng.chat/api'
OUT=Path('artifacts/diemeng_method_probe_2026-05-06.json')

def call(method,path,payload):
    url=BASE+path
    data=None
    if method=='GET':
        url += '?' + urllib.parse.urlencode(payload)
    else:
        data=json.dumps(payload,ensure_ascii=False).encode()
    req=urllib.request.Request(url, data=data, method=method, headers={'apiKey':KEY,'X-API-Key':KEY,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw=r.read().decode('utf-8','replace')
            try: obj=json.loads(raw)
            except Exception: obj=raw[:500]
            return {'http_ok':True,'status':r.status,'body':obj}
    except urllib.error.HTTPError as e:
        raw=e.read().decode('utf-8','replace')[:500]
        try: obj=json.loads(raw)
        except Exception: obj=raw
        return {'http_ok':False,'status':e.code,'body':obj}
    except Exception as e:
        return {'http_ok':False,'error':type(e).__name__+': '+str(e)}

def rows(body):
    if not isinstance(body,dict): return []
    if body.get('code') not in (None,0,200,'0','200'):
        return []
    for k in ['data','result']:
        v=body.get(k)
        if isinstance(v, list): return [x for x in v if isinstance(x,dict)]
        if isinstance(v, dict):
            for sk in ['list','rows','items','records']:
                sv=v.get(sk)
                if isinstance(sv,list): return [x for x in sv if isinstance(x,dict)]
    return []

payloads={
 'stock_daily':('/stock/daily', {'stock_code':'000001.SZ','start_time':'2024-01-02','end_time':'2024-01-10','page':1,'page_size':5}),
 'stock_daily_adj':('/stock/daily_adj', {'stock_code':'000001.SZ','start_time':'2024-01-02','end_time':'2024-01-10','page':1,'page_size':5}),
 'adj_factor':('/stock/adj_factor', {'stock_code':'000001.SZ','start_time':'2024-01-02','end_time':'2024-01-10','page':1,'page_size':5}),
 'income':('/stock/income', {'stock_code':'000001.SZ','start_date':'2023-01-01','end_date':'2024-12-31','page':1,'page_size':5}),
 'balancesheet':('/stock/balancesheet', {'stock_code':'000001.SZ','start_date':'2023-01-01','end_date':'2024-12-31','page':1,'page_size':5}),
 'cashflow':('/stock/cashflow', {'stock_code':'000001.SZ','start_date':'2023-01-01','end_date':'2024-12-31','page':1,'page_size':5}),
 'financial_indicator':('/stock/financial_indicator', {'stock_code':'000001.SZ','start_date':'2023-01-01','end_date':'2024-12-31','page':1,'page_size':5}),
 'main_fund_flow':('/stock/main_fund_flow_overview', {'stock_code':'000001.SZ','start_time':'2024-01-02','end_time':'2024-01-10','page':1,'page_size':5}),
 'dragon_tiger':('/stock/dragon_tiger', {'trade_date':'2024-01-02','page':1,'page_size':5}),
 'bond_daily':('/bond/daily', {'stock_code':'110059.SH','start_time':'2024-01-02','end_time':'2024-01-10','page':1,'page_size':5}),
}
res={}
for name,(path,payload) in payloads.items():
    res[name]={}
    for method in ['GET','POST']:
        c=call(method,path,payload)
        rs=rows(c.get('body'))
        cols=sorted({k for r in rs[:5] for k in r.keys()})
        res[name][method]={'status':c.get('status'),'http_ok':c.get('http_ok'),'error':c.get('error'),'code':c.get('body',{}).get('code') if isinstance(c.get('body'),dict) else None,'msg':(c.get('body',{}).get('msg') or c.get('body',{}).get('message')) if isinstance(c.get('body'),dict) else None,'rows':len(rs),'cols':cols[:80],'sample':rs[:1]}
        time.sleep(.1)
OUT.write_text(json.dumps(res,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps({k:{m:{'status':v[m].get('status'),'code':v[m].get('code'),'msg':v[m].get('msg'),'rows':v[m].get('rows'),'cols':v[m].get('cols')[:12]} for m in v} for k,v in res.items()},ensure_ascii=False,indent=2))
