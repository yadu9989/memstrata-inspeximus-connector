"""Standalone synthetic checks; no installed connector, network or caller database."""
import hashlib
import json
import platform
import sqlite3
from pathlib import Path

from lifecycle_reference import Coordinator, KINDS, Request, SQLiteTarget, SyntheticWorkspace, Target
from provenance_projection import project_sources

KEY=b'synthetic-probe-key-not-a-service-secret-32'
MARKER='SYNTHETIC-COVERED-CONTENT-937de2'
ABSENT='NEVER-INSERTED-CONTROL-eca155'


def count_probe(delete, reported):
    class CountReportingTarget(SQLiteTarget):
        def apply(self, action, scope_key, record_key=None):
            if delete:
                super().apply(action, scope_key, record_key)
            return reported
    with SyntheticWorkspace() as ws:
        targets=tuple(Target(k,k) for k in sorted(KINDS))
        stores={t.name:CountReportingTarget(ws,t) for t in targets}
        c=Coordinator(ws,stores,hmac_key=KEY,authorize=lambda principal,request:principal=='operator')
        for store in stores.values():
            store.seed(c.scope('tenant','document'),c.record('record'),MARKER*100)
            assert store.byte_probe((MARKER,))['matched']>0
            assert store.byte_probe((ABSENT,))['matched']==0
        req=Request('operation','tenant','document','erased',targets)
        result=c.execute(req,'operator',probe_values=(MARKER,))
        if not delete:
            try:c.execute(req,'operator',probe_values=(ABSENT,))
            except ValueError:weaker_retry_rejected=True
            else:weaker_retry_rejected=False
        else:weaker_retry_rejected=None
        return {'reported_count':reported,'actually_deleted':delete,
                'complete':result['complete'],'global_erasure_proven':result['global_erasure_proven'],
                'weaker_retry_rejected':weaker_retry_rejected}


def main():
    false_claim=count_probe(False,2)
    zero_report=count_probe(True,0)
    assert not false_claim['complete'] and false_claim['weaker_retry_rejected']
    assert zero_report['complete'] and not zero_report['global_erasure_proven']
    f={'sources':[{'principal':p} for p in ('A','B','C')],
       'writer_metadata':{'source_provenance':{'associations':[
           {'source_index':i,'source':{'principal':p}} for i,p in enumerate(('A','B','C'))]}}}
    compacted=f['sources'][1:]
    bad=compacted[f['writer_metadata']['source_provenance']['associations'][1]['source_index']]['principal']
    projected=project_sources(f,[0])
    survivor=projected['associations'][1]
    good=projected['source_slots'][survivor['source_index']]['source']['principal']
    assert bad=='C' and good=='B'
    root=Path(__file__).parent
    result={'scope':'owned synthetic SQLite fixtures only; not live APIs or hardware erasure',
            'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,
            'false_erasure_claim':false_claim,'zero_count_real_erasure':zero_report,
            'source_slots':{'expected_survivor':'B','compaction_resolved':'C','tombstone_resolved':good},
            'code_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
                [Path(__file__),root/'lifecycle_reference.py',root/'provenance_projection.py']}}
    path=root/'run_lifecycle_probes.result.json'
    path.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__':main()
