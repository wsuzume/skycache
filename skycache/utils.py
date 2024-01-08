import hashlib
import uuid

from datetime import datetime
from pathlib import Path
from io import StringIO

from typing import Callable, Iterable, List, Optional

from .randname import get_random_name

def ignore_path(ps: Iterable[Path], rule: set[str]) -> List[Path]:
    return [ p for p in ps if len(rule & set(p.parts)) == 0 ]

def ignore_python_cache(ps: Iterable[Path]) -> List[Path]:
    python_cache_set = {'.ipynb_checkpoints', '__pycache__'}
    
    return ignore_path(ps, python_cache_set)

def get_latest_file(dir_path: Path, glob: str="*", return_None: bool=False) -> Optional[Path]:
    dir_path = Path(dir_path)
    
    paths = sorted(
        ignore_python_cache(dir_path.glob(glob)),
        key=lambda p: datetime.fromtimestamp(p.stat().st_mtime)
    )

    if len(paths) == 0:
        if return_None:
            return None
        raise FileNotFoundError(f"There were no files specified by glob '{glob}' in '{dir_path}'.")
    
    return paths[-1]

def hash_md5(path: Path) -> str:
    """指定したファイルのハッシュ値を計算して返す。

    Parameters
    ----------
    path : Path
        ハッシュ値を計算したいファイルのパス。

    Returns
    -------
    str
        ハッシュ値。

    Raises
    ------
    FileNotFoundError
        指定されたパスにファイルが存在しないか、パスで指定された対象がファイルではない。
    """
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(f"No such file: '{path}'.")

    with open(str(path), 'rb') as f:
        data = f.read()
    return hashlib.md5(data).hexdigest()

def hash_md5_recursive(path: Path, glob: str='**/*') -> str:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"No such file or directory: '{path}'.")

    if (not path.is_file()) and (not path.is_dir()):
        raise FileNotFoundError(f"path must be a file or a directory: '{path}'.")

    if path.is_file():
        with open(str(path), 'rb') as f:
            data = f.read()
        return hashlib.md5(data).hexdigest()

    if glob is None:
        raise FileNotFoundError(f"path must be a file when glob is None: '{path}'.")

    with StringIO() as buf:
        for p in sorted([ p for p in ignore_python_cache(path.glob(glob)) if p.is_file() ]):
            with open(str(p), 'rb') as f:
                data = f.read()
            buf.write(hashlib.md5(data).hexdigest())
        all_hash = buf.getvalue()

    return hashlib.md5(all_hash.encode('utf-8')).hexdigest()

def get_unique_name(dateformat=None) -> str:
    now = datetime.now()

    if dateformat is None:
        date = (lambda x: x.strftime('%Y%m%d_%H%M%S%f'))(now)
    elif isinstance(dateformat, str):
        date = now.strftime(dateformat)
    elif isinstance(dateformat, Callable):
        date = dateformat(now)
    else:
        raise TypeError("dateformat must be instance of str or Callable[[datetime.datetime], str].")
        
    return '-'.join([ date, get_random_name(0), str(uuid.uuid4()).split('-')[0] ])

def timestamp_from_unique_name(tag):
    return datetime.strptime(tag.split('-')[0], '%Y%m%d_%H%M%S%f').timestamp()
