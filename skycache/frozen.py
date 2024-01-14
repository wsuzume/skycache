import pandas as pd

from pathlib import Path

class FrozenFile:
    """
    どこにキャッシュされているかまで判明しているファイル。
    """
    def __init__(self, path: Path, cache_root_dir: Path, tag: str):
        self._origin = Path(path)
        self._cache_root_dir = Path(cache_root_dir)
        self._tag = tag
    
    def __repr__(self):
        return f"{self.__class__.__name__}('{self.origin}', '{self.cache_root_dir}', '{self.tag}')"
    
    def __str__(self):
        return str(self.path)

    @property
    def origin(self):
        return self._origin
    
    @property
    def cache_root_dir(self):
        return self._cache_root_dir
    
    @property
    def tag(self):
        return self._tag

    @property
    def path(self):
        return self.cache_root_dir / self.tag / self.origin

class FrozenContext:
    """
    どのファイルを使用するかまで判明しているコンテキスト。
    """
    def __init__(self, df: pd.DataFrame, cache_root_dir: Path, inc_idx: pd.Index=None):
        self.df = df
        self.idx = inc_idx
        self.cache_root_dir = Path(cache_root_dir)
    
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    
    @property
    def tag(self):
        xs = self.df['tag'].value_counts()
        if len(xs) != 1:
            raise RuntimeError(f"broken hash table: tag can't be specified.")
        return xs.index[0]

    @property
    def incriments(self):
        return self.df[self.idx]
    
    @property
    def is_update_detected(self):
        return len(self.incriments) != 0

    def _as_frozenfile_list(self, df: pd.DataFrame):
        ret = []
        for _, row in df.iterrows():
            ret.append(
                FrozenFile(
                    path=row['path'],
                    cache_root_dir=self.cache_root_dir,
                    tag=row['refer'],
                )
            )
        
        return ret

    def __getitem__(self, key):
        if isinstance(key, slice):
            if key.start is None and key.stop is None and key.step is None:
                df = self.df
            else:
                raise KeyError(f"specifying start, stop, and step is not allowed.")
        else:
            df = self.df[self.df['key'] == str(key)]
        
        return self._as_frozenfile_list(df)
    
    def group(self, group):
        return ...