import hashlib
import pandas as pd
from pathlib import Path

import uuid

from typing import Iterable, Optional

from .checkpoint import Input
from .utils import get_unique_name

class Workflow:
    def __init__(self, root_dir: Path, db_path: Optional[Path]=None):
        self.root_dir = Path(root_dir)
        self.name = get_unique_name()

        self._db_path = None if db_path is None else Path(db_path)

        # Never substitude again to this attribute
        # We are using this as a pointer
        self._database = SQLite3(db_path=self.db_path)

        self._context = {}

    @property
    def database(self):
        # read only property
        return self._database
    
    def __repr__(self):
        return f"Workflow({self.name})"

    def __setitem__(self, key, val):
        self._context[key] = val

    def __getitem__(self, key):
        return self._context[key]
    
    def use(self, *args):
        kwargs = { k: self._context[k] for k in args }
        return Context(self.name, self.database, **kwargs)
    
    @property
    def db_path(self):
        if self._db_path is None:
            return self.root_dir / ".skycache" / "database.db"
        return self._db_path

    @property
    def inputs(self):
        return { k: v for k, v in self._context.items() if isinstance(v, Input) }
    
    def get_input_hash_record(self):
        record = { k: ipt.hash() for k, ipt in self.inputs.items()}
        return pd.Series(record, name=self.name)

    def compile(self, dest_dir: Path=None):
        dest_dir = dest_dir
        return