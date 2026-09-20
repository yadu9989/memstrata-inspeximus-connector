import json
import pytest
from examples.prepare_joint_fixture import prepare
from memstrata_mnemo_connector import DurableOutbox, SourceConflict


def test_new_fixture_shared_principal_and_frozen_outbox(tmp_path):
    root=tmp_path/'fixture'
    m=prepare(root)
    before=json.loads((root/'before.json').read_bytes())
    after=json.loads((root/'after.json').read_bytes())
    assert m['synthetic_only']
    assert before['fact_record']['id']!=after['fact_record']['id']
    assert before['fact_record']['key']==after['fact_record']['key']
    assert before['fact_record']['valid_from']<after['fact_record']['valid_from']
    for value in (before,after):
        f=value['fact_record']
        assert f['corroboration_count']==1
        associations=f['writer_metadata']['source_provenance']['associations']
        assert len(associations)==2 and associations[0]['source_index']==associations[1]['source_index']==0
        assert associations[0]['source']['doc']!=associations[1]['source']['doc']
    box=DurableOutbox(tmp_path/'outbox.sqlite3','https://memory.example.com')
    box.enqueue(before);box.enqueue(after);box.enqueue(before)
    assert box.counts()=={'pending':2}
    before['fact_record']['text']='Changed under an existing immutable ID'
    with pytest.raises(SourceConflict):box.enqueue(before)
    with pytest.raises(FileExistsError):prepare(root)
