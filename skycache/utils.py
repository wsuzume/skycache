import hashlib

from datetime import datetime
from pathlib import Path
from io import StringIO

from typing import Iterable, List, Optional

def exclude_python_cache(xs: Iterable[Path]) -> List[Path]:
    python_cache_set = {'.ipynb_checkpoints', '__pycache__'}
    
    return [ x for x in xs if len(python_cache_set & set(x.parts)) == 0 ]

def get_latest_file(dir_path: Path, glob: str="*", return_None: bool=False) -> Optional[Path]:
    dir_path = Path(dir_path)
    
    paths = sorted(
        exclude_python_cache(dir_path.glob(glob)),
        key=lambda p: datetime.fromtimestamp(p.stat().st_mtime)
    )

    if len(paths) == 0:
        if return_None:
            return None
        raise FileNotFoundError(f"There were no files specified by glob '{glob}' in '{dir_path}'.")
    
    return paths[-1]

def hash_md5_recursive(path: Path, glob: str='*') -> str:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"No such file or directory: '{path}'.")

    if (not path.is_file()) and (not path.is_dir()):
        raise FileNotFoundError(f"path must be a file or a directory: '{path}'.")

    if path.is_file():
        with open(str(path), 'rb') as f:
            data = f.read()
        return hashlib.md5(data).hexdigest()

    with StringIO() as buf:
        for p in sorted(exclude_python_cache(path.rglob(glob))):
            with open(str(p), 'rb') as f:
                data = f.read()
            buf.write(hashlib.md5(data).hexdigest())
        all_hash = buf.getvalue()

    return hashlib.md5(all_hash.encode('utf-8')).hexdigest()