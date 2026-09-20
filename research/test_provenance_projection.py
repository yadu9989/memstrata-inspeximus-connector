import copy
import pytest
from research.provenance_projection import project_sources, fixture_identity_locations


def fixture():
    return {'sources': [{'principal': p, 'channel': 'unknown'} for p in ('A', 'B', 'C')],
            'writer_metadata': {'source': {'principal': 'A', 'doc': 'PRIMARY-DOC'},
              'source_provenance': {'associations': [
                {'source_index': i, 'writer_record_id': 'record-' + p,
                 'source': {'principal': p, 'doc': 'DOC-' + p}}
                for i, p in enumerate(('A', 'B', 'C'))]}}}


def test_compaction_failing_control_and_tombstone_fix():
    original = fixture()
    compacted = copy.deepcopy(original)
    del compacted['sources'][0]
    association = compacted['writer_metadata']['source_provenance']['associations'][1]
    assert compacted['sources'][association['source_index']]['principal'] == 'C'
    assert association['source']['principal'] == 'B'  # Demonstrates the bad association.
    projected = project_sources(original, [0])
    assert projected['source_slots'][0] == {'slot': 0, 'state': 'erased'}
    for index in (1, 2):
        a = projected['associations'][index]
        assert projected['source_slots'][a['source_index']]['source']['principal'] == a['source']['principal']
    assert original == fixture()  # Existing saved wire bytes are never changed.
    assert not projected['erasure_complete']


def test_erased_association_does_not_echo_source_or_writer_id():
    result = project_sources(fixture(), [0])
    assert result['associations'][0] == {'source_index': 0, 'state': 'erased'}
    assert not fixture_identity_locations(result, 'PRIMARY-DOC')
    assert not fixture_identity_locations(result, 'record-A')
    assert fixture_identity_locations(result, 'DOC-B')


def test_legacy_cleanup_misses_new_source_echo_and_unknown_copy():
    f = fixture()
    marker = 'LINKED-RUNBOOK'
    f['writer_metadata']['source_provenance']['associations'][1]['source']['doc'] = marker
    f['extra_copy'] = marker
    f['sources'] = []
    f['writer_metadata'].pop('source')
    paths = fixture_identity_locations(f, marker)
    assert '$.writer_metadata.source_provenance.associations[1].source.doc' in paths
    assert '$.extra_copy' in paths
    f['writer_metadata'].pop('source_provenance')
    assert fixture_identity_locations(f, marker) == ['$.extra_copy']
    f.pop('extra_copy')
    assert fixture_identity_locations(f, marker) == []
    assert fixture_identity_locations(f, 'NEVER-INSERTED') == []


@pytest.mark.parametrize('index', [-1, 3, True, '0'])
def test_bad_slots_fail_closed(index):
    with pytest.raises(ValueError):
        project_sources(fixture(), [index])


def test_unmapped_source_and_duplicate_principal_associations():
    f = fixture()
    a = copy.deepcopy(f['writer_metadata']['source_provenance']['associations'][0])
    a.update(writer_record_id='record-linked', source={'principal': 'A', 'doc': 'LINKED-RUNBOOK'})
    f['writer_metadata']['source_provenance']['associations'].append(a)
    result = project_sources(f, [0])
    assert result['associations'][0]['state'] == result['associations'][3]['state'] == 'erased'
    assert len(result['source_slots']) == 3
