import hashlib
import os

import pytest

from scripts.volume_inventory import inventory


def test_inventory_covers_bytes_empty_directories_and_ownership(tmp_path):
    (tmp_path/'empty').mkdir()
    (tmp_path/'data').write_bytes(b'recovery evidence')
    entries=inventory(tmp_path)['entries']
    assert entries['empty']['type']=='directory'
    assert entries['data']['sha256']==hashlib.sha256(b'recovery evidence').hexdigest()
    assert entries['data']['bytes']==17
    assert {'mode','uid','gid'} <= entries['data'].keys()
    assert entries['.']['type']=='directory'


def test_file_corruption_changes_inventory(tmp_path):
    file=tmp_path/'data'
    file.write_bytes(b'original')
    before=inventory(tmp_path)
    file.write_bytes(b'changed!')
    assert inventory(tmp_path)!=before


@pytest.mark.skipif(not hasattr(os,'mkfifo'),reason='FIFO requires POSIX')
def test_special_files_are_rejected(tmp_path):
    os.mkfifo(tmp_path/'pipe')
    with pytest.raises(ValueError,match='Unsupported special file'):
        inventory(tmp_path)
