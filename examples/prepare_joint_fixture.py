"""Freeze new synthetic before/after envelopes once. No network or credentials."""
import argparse
import hashlib
import json
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

from memstrata_mnemo_connector import freeze_record


def prepare(destination):
    destination=Path(destination)
    destination.mkdir(parents=True,exist_ok=False)
    namespace='synthetic-joint-'+uuid.uuid4().hex
    clock=time.time()
    outputs=[]
    for name,offset,text in [('before',120,'API keys'),('after',60,'signed short-lived tokens')]:
        primary={'id':namespace+'-'+name,'text':f'The fictional billing service uses {text}.',
            'key':namespace+'::auth-method','ts':clock-offset,'valid_from':clock-offset,
            'status':'active','mtype':'semantic','source':{'principal':'synthetic-billing-team',
                'doc':namespace+'-primary-'+name},'links':[namespace+'-runbook-'+name]}
        linked={'id':namespace+'-runbook-'+name,'source':{'principal':'synthetic-billing-team',
                    'doc':namespace+'-linked-runbook-'+name}}
        writer=SimpleNamespace(items=[primary,linked])
        payload=freeze_record(writer,primary)
        raw=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
        with (destination/(name+'.json')).open('xb') as f:f.write(raw)
        outputs.append({'file':name+'.json','source_id':primary['id'],'sha256':hashlib.sha256(raw).hexdigest(),
                        'bytes':len(raw),'corroboration_count':payload['fact_record']['corroboration_count']})
    manifest={'synthetic_only':True,'namespace':namespace,'created_at':clock,'files':outputs,
              'retry':'Send before.json, after.json, then the SAME before.json bytes. Do not regenerate.'}
    with (destination/'fixture.json').open('x',encoding='utf-8') as f:json.dump(manifest,f,indent=2)
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('destination');args=parser.parse_args()
    print(json.dumps(prepare(args.destination),indent=2))
