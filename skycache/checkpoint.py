import hashlib
import shutil
import numpy as np
import pandas as pd

from pathlib import Path
from io import StringIO
from typing import Any, Callable, Iterable, List, Optional, Union

from .utils import ignore_path, get_latest_file, hash_md5_recursive, timestamp_from_unique_name



        
# class Context:
#     def __init__(self, name: str, db: Optional[pd.DataFrame]=None, **kwargs):
#         """
#         Never copy db. We use db as a pointer to the db of Workflow.
#         """
        
#         self.name = name
#         self.db = db if db is not None else None
#         self.details = kwargs

#     def __repr__(self):
#         return f"{self.__class__.__name__}('{self.name}', {self.details})"

#     def checkpoint(self):
#         return CheckPoint(self)

# class Input:
#     def __init__(self, loader: Callable[[Path], Any], path: Path, glob: str='*', mode: str="latest"):
#         self.loader = loader
#         self.path = Path(path)
#         self._glob = glob
#         self.mode = mode

#     def __repr__(self):
#         return f"Input({self.loader.__name__}, path='{self.path}')"
    
#     def __call__(self):
#         return self.loader(self.path)

#     def glob(self) -> Union[Path, List[Path]]:
#         if not self.path.exists():
#             raise FileNotFoundError(f"input file does not exists: '{self.path}'.")
            
#         if not self.path.is_dir():
#             return self.path

#         if self.mode == "latest":
#             return get_latest_file(self.path)
#         elif self.mode == "all":
#             return sorted(ignore_python_cache(self.path.rglob(self._glob)))

#         raise RuntimeError(f"Unknown mode: '{self.mode}'.")
    
#     def hash(self):
#         if not self.path.exists():
#             raise FileNotFoundError(f"input file does not exists: '{self.path}'.")
            
#         if not self.path.is_dir():
#             # ファイルでなければ hash_md5_recursive の中でエラーになるので
#             # こちら側ではチェックしなくてよい
#             return hash_md5_recursive(self.path)

#         if self.mode == "latest":
#             return hash_md5_recursive(self.glob())
#         elif self.mode == "all":
#             return hash_md5_recursive(self.path, glob=self._glob)
            
#         raise RuntimeError(f"Unknown mode: '{self.mode}'.")

# class CheckPoint:
#     def __init__(self, context: Context):
#         self.context = context

#     def __repr__(self):
#         return f"{self.__class__.__name__}({self.context})"
    
#     def __enter__(self):
#         print("こんにちは")

#     def __exit__(self, *args):
#         print("さようなら")

#     def using(self):
#         print("今日はいい天気です")
#         return self