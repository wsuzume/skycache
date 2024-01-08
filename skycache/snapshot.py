from __future__ import annotations

import numpy as np
import pandas as pd
import shutil

from pathlib import Path
from tqdm.auto import tqdm

from typing import Callable, List

from .managed import ManagedFile
from .utils import timestamp_from_unique_name
from .database import \
    is_table_exists, drop_table, create_table_recent, read_table_recent, read_table_recent_select, \
    insert_or_replace_into_recent, \
    create_table_history, read_table_history, read_table_history_select, \
    insert_or_replace_into_history, search_history

class SnapshotTableKey:
    def __init__(self, key=None, prefix=None, path=None):
        self.key = key if key is not None else ''
        self.prefix = prefix if prefix is not None else ''
        self.path = path if path is not None else ''

    def __repr__(self):
        return f"{self.__class__.__name__}(key='{self.key}', prefix='{self.prefix}', path='{self.path}')"

    def __str__(self):
        return f"{self.key}|{self.prefix}|{self.path}"
    
    def __hash__(self):
        return hash(str(self))
    
    def __eq__(self, other):
        return str(self) == str(other)

def get_updates(table_cur: dict, table_prev: dict, aware: str="mtime_aware") -> dict:
    """ManagedFile の比較機能を利用して更新の有無を把握する。
    各ファイルについて更新の必要性の有無の判定は以下。
    * "mtime_aware" ... mtime が一致する場合は True。一致しない場合は hash を比較し、一致すれば True。mtime も hash も一致しない場合は False。
    * "mtime_only" ... mtime が一致する場合は True。一致しない場合は False。
    * "hash_only" ... hash が一致する場合は True。一致しない場合は False。
    * "both" ... hash と mtime が両方とも一致する場合は True。一致しない場合は False。
    * "strict" ... filecmp を使用した比較。
    
    "strict" は実際のファイルを読み取って比較する他、hash, mtime についても ManagedFile に保持されていなければ
    実際のファイルを読み取りに行くことに注意。現在のファイルとキャッシュ先のファイルを比較する場合は、
    table_prev のパスがキャッシュ先のファイルを指していないとならない。

    Parameters
    ----------
    table_cur : dict
        _description_
    table_prev : dict
        _description_
    aware : str, optional
        _description_, by default "mtime_aware"

    Returns
    -------
    dict
        _description_

    Raises
    ------
    ValueError
        _description_
    """
    aware_set = { "mtime_only", "hash_only", "mtime_aware", "both", "strict" }

    if aware not in aware_set:
        raise ValueError(f"aware must be one of {aware_set}.")
    
    updates = {}
    for key, mf_cur in table_cur.items():
        if key not in table_prev:
            updates[key] = mf_cur.copy()
            continue

        if not mf_cur.is_same_file_with(table_cur[key], aware=aware):
            updates[key] = mf_cur.copy()
            continue
    
    return updates

def snapshot_table_from_dataframe(df: pd.DataFrame):
    def null_string_to_None(row, col):
        return row[col] if row[col] != '' else None
    
    def None_to_nan(row, col):
        return row[col] if row[col] is None else np.nan

    ret = {}
    for _, row in df.iterrows():
        key = null_string_to_None(row, 'key')
        prefix = null_string_to_None(row, 'prefix')
        group = null_string_to_None(row, 'group')
        mtime = None_to_nan(row, 'mtime')

        k = SnapshotTableKey(key=key, prefix=prefix, path=row['path'])
        ret[k] = ManagedFile(
            path=row['path'],
            key=key,
            prefix=prefix,
            group=group,
            hash=row['hash'],
            mtime=mtime,
        )

    return ret

class Snapshot:
    """管理対象としたいファイルとディレクトリに対する様々な操作を提供する。
    Snapshot は管理対象となるファイルが具体的に決定した状態である。
    そのため Snapshot 作成よりもあとに Resource に追加されたファイルに関しては
    Snapshot の管理対象としては反映されないことに注意する。
    Snapshot はアップデートを実行した時点で管理対象ファイルの hash や mtime などの情報を収集する。
    情報の収集時点とファイルのバックアップ時点で情報がズレる可能性があるため、
    Snapshot はキャッシュを作成する際に hash や mtime のアップデートが実施される。
    それでも Snapshot のバックアップ中にファイルを編集するとズレが起こる可能性があるので、
    Snapshot の管理対象となるファイルをバックアップ中に編集してはならない。

    sqlite3 に記録される情報には recent と history の２種類のテーブルがあり、
    最後のキャッシュ結果が recent に、過去のすべてのキャッシュ履歴が history に記録される。
    recent に相当するテーブル名は Snapshot の name に対応し、
    history に相当するテーブル名は history_[name] に対応する。
    """
    def __init__(self, name: str, files:List[ManagedFile], init_hash=False, init_mtime=False):
        self._name = name
        self.table = { SnapshotTableKey(mf.key, mf.prefix, mf.path):mf for mf in files }

        self.update_table(update_hash=init_hash, update_mtime=init_mtime)
    
    @property
    def name(self):
        return self._name

    @property
    def managed_files(self) -> List[ManagedFile]:
        """管理対象の ManagedFile のリストを返す。

        Returns
        -------
        List[ManagedFile]
            管理対象の ManagedFile のリスト。
        """
        return list(self.table.values())

    @property
    def path_list(self) -> List[Path]:
        """管理対象の Path のリストを返す

        Returns
        -------
        List[Path]
            管理対象の Path のリスト。
        """
        return sorted(list(set([ mf.path for mf in self.table.values() ])))
    
    def update_table(self, update_hash: str=True, update_mtime: str=True):
        """ManagedFile の hash と mtime を更新する。

        Parameters
        ----------
        update_hash : str, optional
            ハッシュを更新するかどうか。(by default True)
        update_mtime : str, optional
            mtime を更新するかどうか。(by default True)
        """
        if update_hash or update_mtime:
            for mf in self.table.values():
                mf.update(copy=False, update_hash=update_hash, update_mtime=update_mtime)
    
    def _copy(self, dest_dir: Path, path_list: List[Path], verbose: bool=False):
        """path_list に与えられたファイルのディレクトリ構造を保ったまま dest_dir にコピーする。

        Parameters
        ----------
        dest_dir : Path
            コピー先ディレクトリのパス。
        path_list : List[Path]
            コピーするファイルのパスのリスト。
        verbose : bool, optional
            コピーの進捗状況を表示する。(by default False)

        Raises
        ------
        ValueError
            dest_dir にカレントディレクトリが指定された。
        """
        dest_dir = Path(dest_dir)
        if len(dest_dir.parts) == 0:
            raise ValueError("current directory is not allowed for destination.")

        wrap = tqdm if verbose else (lambda x: x)
        
        for path in wrap(path_list):
            save_path = dest_dir / path
            save_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, save_path)
    
    def copy(self, dest_dir: Path, verbose: bool=False):
        """管理対象のファイルを、ディレクトリ構造を保ったまま dest_fir にコピーする。

        Parameters
        ----------
        dest_dir : Path
            コピー先ディレクトリのパス。
        verbose : bool, optional
            コピーの進捗状況を表示する。(by default False)
        """
        self._copy(dest_dir=dest_dir, path_list=self.path_list, verbose=verbose)

    def drop_recent_hash_table(self, db_path: Path):
        """直近のキャッシュ情報が記録されたテーブルを削除する。

        Parameters
        ----------
        db_path : Path
            テーブルを保存しているデータベースファイルのパス。
        """
        if is_table_exists(db_path, self.name):
            drop_table(db_path, table_name=self.name)
    
    def drop_history_hash_table(self, db_path: Path):
        """過去のすべてのキャッシュ情報が記録されたテーブルを削除する。

        Parameters
        ----------
        db_path : Path
            テーブルを保存しているデータベースファイルのパス。
        """
        if is_table_exists(db_path, self.name):
            drop_table(db_path, table_name=f"history_{self.name}")

    def read_recent_hash_table(self, db_path: Path, select: bool=True) -> pd.DataFrame:
        """直近のキャッシュ情報が記録されたテーブルを読み取る。

        Parameters
        ----------
        db_path : Path
            テーブルを保存しているデータベースファイルのパス。
        select : bool, optional
            管理対象のファイルに関連する情報のみを読み取る。(by default True)

        Returns
        -------
        pd.DataFrame
            直近のキャッシュ情報。
        """
        if not is_table_exists(db_path, self.name):
            create_table_recent(db_path, self.name)

        if not select:
            return read_table_recent(db_path, self.name)
        return read_table_recent_select(db_path, self.name, self.managed_files)

    def read_history_hash_table(self, db_path: Path, select: bool=True) -> pd.DataFrame:
        """過去のすべてのキャッシュ情報が記録されたテーブルを読み取る。

        Parameters
        ----------
        db_path : Path
            テーブルを保存しているデータベースファイルのパス。
        select : bool, optional
            管理対象のファイルに関連する情報のみを読み取る。(by default True)

        Returns
        -------
        pd.DataFrame
            過去のすべてのキャッシュ情報。
        """
        if not is_table_exists(db_path, f"history_{self.name}"):
            create_table_history(db_path, self.name)
        
        if not select:
            return read_table_history(db_path, self.name)
        return read_table_history_select(db_path, self.name, self.managed_files)
    
    @staticmethod
    def _parse_tag_and_timestamp(tag, timestamp):
        """内部ユーティリティ。ユーザからの使用は想定していない。
        """
        if tag is None:
            tag = ''
            if timestamp is None:
                t = np.nan
                return (tag, t)
            if isinstance(timestamp, Callable):
                t = timestamp()
                return (tag, t)
        else:
            if timestamp is None:
                t = timestamp_from_unique_name(tag)
                return (tag, t)

            if isinstance(timestamp, Callable):
                t = timestamp(tag)
                return (tag, t)    

        # 上記以外のすべてのケース
        return (tag, timestamp)

    def as_hash_table(self,
                      tag: str=None,
                      timestamp=None,
                      refer_self=False,
                      update_hash: bool=False,
                      update_mtime: bool=False) -> pd.DataFrame:
        tag, time = self._parse_tag_and_timestamp(tag, timestamp)
        
        table = []
        for mf in self.table.values():
            if update_hash or update_mtime:
                mf.update(update_hash=update_hash, update_mtime=update_mtime)

            path = str(mf.path)
            key = mf.key if mf.key is not None else ''
            refer = tag if refer_self else ''
            hash_ = mf.hash if mf.hash is not None else ''
            mtime = mf.mtime if mf.mtime is not None else np.nan
            group = mf.group if mf.group is not None else ''
            prefix = str(mf.prefix) if mf.prefix is not None else ''

            xs = pd.Series(
                [path, key, tag, refer, group, prefix, hash_, time, mtime],
                index=['path', 'key', 'tag', 'refer', 'group', 'prefix', 'hash', 'time', 'mtime']
            )
        
            table.append(xs)

        return pd.DataFrame(table)

    def _insert_or_replace_into_hash_table(self, db_path: Path, df: pd.DataFrame):
        data = [ row[1:] for row in df.itertuples() ]
        
        if not is_table_exists(db_path, f"history_{self.name}"):
            create_table_history(db_path, self.name)
            
        insert_or_replace_into_history(db_path, self.name, data)
    
    @staticmethod
    def _table_from_dataframe(df: pd.DataFrame):
        return snapshot_table_from_dataframe(df)
    
    def get_updates(self, db_path: Path=None, *,
                    df_prev: pd.DataFrame=None,
                    aware: str="mtime_aware") -> dict:
        """前回のキャッシュから更新されたファイルを取得する。
        .as_hash_table() で取得できるのと同形式のデータフレームを df_prev に直接与えてもよい。
        db_path, df_prev の両方を同時に指定することはできない。

        Parameters
        ----------
        db_path : Path, optional
            前回のキャッシュの情報が保存されているデータベースファイルのパス。(by default None）
        df_prev : pd.DataFrame, optional
            .as_hash_table() で取得できるのと同形式のデータフレーム。(by default None)
        aware : str, optional
            更新を検知するための比較方法。(by default "mtime_aware")

        Returns
        -------
        dict
            更新が検知されたファイルの辞書。

        Raises
        ------
        ValueError
            db_path と df_prev のうちどちらも指定されていないか、両方が指定された。
        """
        if (db_path is None) and (df_prev is None):
            raise ValueError(f"please specify db_path xor df_prev.")
        if (db_path is not None) and (df_prev is not None):
            raise ValueError(f"cannot be specified at the same time: db_path and df_prev.")

        if db_path is not None:
            df_prev = self.read_recent_hash_table(db_path)
        
        # TODO: キャッシュされている場合にキャッシュのパスで上書きする。
        table_prev = self._table_from_dataframe(df_prev)
        
        return get_updates(self.table, table_prev, aware=aware)

    def search_history(self,
                       db_path: Path,
                       mode="recent",
                       aware="both",
                       update_hash=True,
                       update_mtime=True) -> pd.DataFrame:
        """管理対象のファイルの現在のバージョンが履歴にあるかどうかを検索する。

        Parameters
        ----------
        db_path : Path
            履歴の情報が保存されているデータベースファイルのパス。
        mode : str, optional
            検索モード。"recent", "all" から選ぶ。(by default "recent")
            "recent" は直近の履歴のみ取得する。
            "all" は過去のすべての履歴を取得する。
        aware : str, optional
            バージョン比較のルールを規定する。(by default "both")
            "hash" ... ハッシュのみ比較する。
            "mtime" ... mtime のみ比較する。
            "both" ... ハッシュ、mtime の両方を比較する。
        update_hash : bool, optional
            検索前にハッシュを再取得する。(by default True)
        update_mtime : bool, optional
            検索前に mtime を再取得する。(by default True)

        Returns
        -------
        pd.DataFrame
            検索結果。
        """
        df = self.as_hash_table(update_hash=update_hash, update_mtime=update_mtime)
        
        return search_history(db_path, self.name, df, mode=mode, aware=aware)

    def calc_incremental_difference(self, db_path: Path, tag: str, timestamp=None, mode="recent", aware="both", update: bool=False, return_index=False):
        if update:
            self.update_table(update_hash=True, update_mtime=True)

        df_current = self.as_hash_table(tag=tag, timestamp=timestamp, refer_self=True, update_hash=False, update_mtime=False)
        df_history = self.search_history(db_path, mode=mode, aware=aware, update_hash=False, update_mtime=False)

        df_refer = df_history[["path", "key", "prefix", "refer"]]
        df_refer.columns = ["path", "key", "prefix", "refer_history"]
        
        df = pd.merge(df_current, df_refer, how="left", on=["path", "key", "prefix"])

        idx = df['refer_history'].isna()

        # 変更日時とハッシュが同じファイルが保存されていれば参照先を置き換え
        df.loc[~idx, 'refer'] = df.loc[~idx, 'refer_history']

        df = df[df_current.columns].copy()

        if return_index:
            return df, idx
        return df

    def enforce_cache(self, db_path: Path, cache_root_dir: Path, tag: str, timestamp=None):
        """現在の管理対象をすべて強制的にキャッシュする。
        定期的にバックアップとして実行しておくとよい。

        Parameters
        ----------
        db_path : Path
            キャッシュ情報を書き込むデータベースファイルのパス。
        cache_root_dir : Path
            キャッシュするファイルを保存しておくディレクトリのパス。
        tag : str
            キャッシュするバージョンにつける名前。
        timestamp : _type_, optional
            付与するタイムスタンプ。(by default None)
        """
        dest_dir = Path(cache_root_dir) / tag

        self.copy(dest_dir=dest_dir)
        df = self.as_hash_table(tag=tag, refer_self=True, update_hash=True, update_mtime=True)
        self._insert_or_replace_into_hash_table(db_path, df)

    def cache_increments(self,
                         db_path: Path,
                         cache_root_dir: Path,
                         tag: str,
                         timestamp=None,
                         mode="recent",
                         aware="both",
                         update: bool=False):
        dest_dir = Path(cache_root_dir) / tag
        
        df_inc, idx = self.calc_incremental_difference(
            db_path,
            tag,
            timestamp=timestamp,
            mode=mode,
            aware=aware,
            update=update,
            return_index=True
        )

        df = df_inc[idx]

        if len(df) == 0:
            return df

        self._copy(dest_dir=dest_dir, path_list=list(map(Path, df['path'])), verbose=False)
        self._insert_or_replace_into_hash_table(db_path=db_path, df=df_inc)
        
        return df
    
    def save(self, db_path: Path, cache_root_dir: Path, tag: str, timestamp=None):
        save_dir = Path(cache_root_dir) / tag
        if len(save_dir.parts) == 0:
            raise ValueError("current directory is not allowed for save_dir.")
        
        for path in self.path_list:
            save_path = save_dir / path
            save_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, save_path)

    def update(self, db_path: Path, cache_root_dir: Path, tag: str, timestamp=None):
        self.save(db_path, cache_root_dir, tag, timestamp)
        self.overwrite_recent_hash_table(db_path, tag=tag, timestamp=timestamp, update=True)