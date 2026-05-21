#!/usr/bin/env python3
from __future__ import annotations
import argparse

def render_timer_units(*, allow_controlled_execution:bool=False, max_experiments:int=1, interval:str='6h')->dict[str,str]:
    flags=''
    if allow_controlled_execution:
        flags=f' --allow-controlled-execution --max-experiments {max_experiments}'
    service=(
        '[Unit]\nDescription=Factor Lab autonomous research loop once\n\n'
        '[Service]\nType=oneshot\nWorkingDirectory=/home/admin/factor-lab\n'
        'Environment=PYTHONPATH=src\n'
        f'ExecStart=/home/admin/factor-lab/.venv/bin/python /home/admin/factor-lab/scripts/run_autonomous_research_loop_once.py{flags}\n'
    )
    timer=(
        '[Unit]\nDescription=Run Factor Lab autonomous research loop periodically\n\n'
        f'[Timer]\nOnUnitActiveSec={interval}\nPersistent=true\nUnit=factor-lab-autonomous-research-loop.service\n\n'
        '[Install]\nWantedBy=timers.target\n'
    )
    return {'service':service,'timer':timer}

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--preview',action='store_true'); ap.add_argument('--allow-controlled-execution',action='store_true'); ap.add_argument('--max-experiments',type=int,default=1)
    a=ap.parse_args(); u=render_timer_units(allow_controlled_execution=a.allow_controlled_execution,max_experiments=a.max_experiments)
    print(u['service']); print('---'); print(u['timer'])
