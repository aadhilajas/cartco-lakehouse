"""Fingerprint a stopped Linux volume, including ownership/modes and symlink targets."""
import argparse
import hashlib
import json
from pathlib import Path
import stat


def inventory(root):
    root=Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError('Inventory root must be a real directory')
    entries={}
    for path in [root,*sorted(root.rglob('*'))]:
        info=path.lstat()
        item=dict(mode=stat.S_IMODE(info.st_mode),uid=info.st_uid,gid=info.st_gid)
        if stat.S_ISLNK(info.st_mode):
            item.update(type='symlink',target=str(path.readlink()))
        elif stat.S_ISDIR(info.st_mode):
            item['type']='directory'
        elif stat.S_ISREG(info.st_mode):
            digest=hashlib.sha256()
            with path.open('rb') as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):
                    digest.update(block)
            item.update(type='file',bytes=info.st_size,sha256=digest.hexdigest())
        else:
            raise ValueError(f'Unsupported special file in stopped volume: {path.relative_to(root)}')
        entries[path.relative_to(root).as_posix()]=item
    return dict(schema_version='cartco-volume-inventory.v1',entries=entries)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    report=inventory(args.root)
    with args.output.open('x',encoding='utf-8') as stream:
        json.dump(report,stream,sort_keys=True,indent=2)
    print(json.dumps(dict(entries=len(report['entries']))))


if __name__=='__main__':
    main()
