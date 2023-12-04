import hashlib
import pandas as pd
from pathlib import Path

import uuid

from typing import Iterable, Optional

from .checkpoint import Context, Input, CheckPoint

class Workflow:
    def __init__(self, root_dir: Path, db_path: Optional[Path]=None):
        self.root_dir = Path(root_dir)
        self.name = uuid.uuid4()

        self._db_path = None if db_path is None else Path(db_path)

        # Never substitude again to the database
        # We are using this as a pointer
        self.database = self.load_database()

        self._context = {}
    
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
            return self.root_dir / "database.csv"
        return self._db_path
    
    def load_database(self):
        if not self.db_path.exists():
            return pd.DataFrame()
        return pd.read_csv(self.db_path, index_col=0)

    def update_database(self):
        record = self.get_input_hash_record()
        for col in record.index:
            self.database.loc[self.name, col] = record[col]
        return self.database
    
    def save_database(self):
        self.database.to_csv(self.db_path)

    @property
    def inputs(self):
        return { k: v for k, v in self._context.items() if isinstance(v, Input) }
    
    def get_input_hash_record(self):
        record = { k: ipt.hash() for k, ipt in self.inputs.items()}
        return pd.Series(record, name=self.name)