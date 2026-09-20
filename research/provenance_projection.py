"""Synthetic positional-provenance projection; never edits a queued wire envelope."""
import copy


def project_sources(fact, removed_indexes=()):
    sources = fact['sources']
    removed = set(removed_indexes)
    if any(type(i) is not int or not 0 <= i < len(sources) for i in removed):
        raise ValueError('invalid_source_slot')
    slots = [{'slot': i, 'state': 'erased'} if i in removed else
             {'slot': i, 'state': 'present', 'source': copy.deepcopy(s)}
             for i, s in enumerate(sources)]
    associations = []
    for original in fact.get('writer_metadata', {}).get('source_provenance', {}).get('associations', []):
        index = original.get('source_index')
        if index is not None and (type(index) is not int or not 0 <= index < len(sources)):
            raise ValueError('dangling_source_association')
        associations.append({'source_index': index, 'state': 'erased'} if index in removed else
                            {**copy.deepcopy(original), 'state': 'present'})
    return {'capability': 'source-slots-research-v1', 'source_slots': slots,
            'associations': associations, 'wire_envelope_modified': False,
            'erasure_complete': False}


def fixture_identity_locations(value, marker, path='$'):
    """Find an exact synthetic marker in keys/values, including unknown metadata.

    This is an adversarial fixture helper, not data-subject discovery or an eraser.
    """
    found = []
    if isinstance(value, str) and marker in value:
        found.append(path)
    elif isinstance(value, dict):
        for k, v in value.items():
            if marker in k:
                found.append(path + '.<key>')
            found.extend(fixture_identity_locations(v, marker, path + '.' + k))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found.extend(fixture_identity_locations(v, marker, path + f'[{i}]'))
    return found
