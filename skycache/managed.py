from __future__ import annotations

import copy
import filecmp

from pathlib import Path

from typing import Callable, Iterable, List

from .utils import ignore_path, hash_md5

class ManagedFile:
    """管理対象となっているファイルの情報を扱うクラス。
    対象のパスがディレクトリであってはならないので初期化時にパスが指しているのがファイルであることがチェックされる。
    path と mtime が一致するファイルは内容も同一であるとみなされる。

    Attributes
    ----------
    path : Path, immutable
        管理対象となるファイルのパス。immutable。
    prefix : Path, optional, immutable
        ManagedDirectory により生成された場合にそのディレクトリのパスが格納される。(by default None)
        path と前方一致していなければならない。
    group : str, optional, immutable
        管理グループが存在する場合、そのグループ名。(by default None)
    hash : str, optional
        管理対象となるファイルのハッシュ値。(by default None)
        ハッシュの計算方法はデフォルトで md5。
        一般に計算に時間がかかり、ファイルの変更に伴って変わるため、必要になるまで計算は保留される。
    mtime : float, optional
        管理対象となるファイルの last modified time。(by default None)
        取得方法はデフォルトで path.stat().st_mtime である。
        取得には一般にディスクアクセスを伴うため、必要になるまで取得は保留される。
    """
    def __init__(self, path: Path, *,
                 key: str=None,
                 prefix: Path=None,
                 group: str=None,
                 hash: str=None,
                 mtime: float=None,
                 hash_function: Callable[[Path], str]=None,
                 mtime_function: Callable[[Path], float]=None):
        """
        Parameters
        ----------
        path : Path
            管理対象となるファイルのパス。
        key : str, optional
            パスとは別に、管理対象を識別するためのキー。(by default None)
        prefix : Path, optional
            ManagedDirectory により生成された場合にそのディレクトリのパスが格納される。(by default None)
            path と前方一致していなければならない。
        hash : str, optional
            管理対象となるファイルのハッシュ値。(by default None)
        mtime : float, optional
            管理対象となるファイルの last modified time。 path.stat().st_mtime で取得した値。(by default None)
        group : str, optional
            管理グループが存在する場合、そのグループ名。(by default None)
        hash_function : Callable[[Path], str], optional
            管理対象となるファイルのハッシュ値の計算する関数。(by default None)
        mtime_function : Callable[[Path], float], optional
            管理対象となるファイルの last modified time を計算する関数。(by default None)

        Raises
        ------
        FileNotFoundError
            初期化時に path で指定されたファイルが存在しない、または path で指定された対象がファイルではない。
        ValueError
            与えられた prefix が path に前方一致していない。
        TypeError
            group に指定された引数が文字列ではない。（必要ではないが ManagedDirectory の挙動と合わせるため）
        """
        if (group is not None) and (not isinstance(group, str)):
            raise TypeError(f"group must be None or instance of str.")
        
        self._path = Path(path)
        self._key = key
        self._prefix = Path(prefix) if prefix is not None else None
        self._group = group

        self.hash = hash
        self.mtime = mtime

        self.hash_function = hash_function
        self.mtime_function = mtime_function

        if not self.path.is_file():
            raise FileNotFoundError(f"No such file: '{self.path}'.")

        if self.prefix is not None:
            parts = self.prefix.parts
            if parts != self.path.parts[:len(parts)]:
                raise ValueError(f"prefix not match.")
    
    @property
    def path(self):
        return self._path

    @property
    def key(self):
        return self._key

    @property
    def prefix(self):
        return self._prefix
    
    @property
    def group(self):
        return self._group

    def __repr__(self):
        params = [f"'{self.path}'"]
        if self.key is not None:
            params.append(f"key='{self.key}'")
        if self.prefix is not None:
            params.append(f"prefix='{self.prefix}'")
        if self.group is not None:
            params.append(f"group='{self.group}'")
        if self.hash is not None:
            params.append(f"hash='{self.hash}'")
        if self.mtime is not None:
            params.append(f"mtime={self.mtime}")
        return f"{self.__class__.__name__}({', '.join(params)})"

    def copy(self) -> ManagedFile:
        """自身のインスタンスのコピーを返す。

        Returns
        -------
        ManagedFile
            自身のコピー。
        """
        return ManagedFile(
            path=self.path,
            key=self.key,
            prefix=self.prefix,
            group=self.group,
            hash=self.hash,
            mtime=self.mtime,
            hash_function=self.hash_function,
            mtime_function=self.mtime_function,
        )

    def as_dict(self) -> dict:
        """ManagedFile の引数に与えたり、JSON に dump できる形式の辞書にする。
        ただし hash_function と mtime_function は名前しか保存できないので注意。

        Returns
        -------
        dict
            ManagedFile の情報を保持する辞書。
        """
        ret = { 'path': str(self.path) }
        if self.key is not None:
            ret['key'] = self.key
        if self.prefix is not None:
            ret['prefix'] = str(self.prefix)
        if self.group is not None:
            ret['group'] = self.group
        if self.hash is not None:
            ret['hash'] = self.hash
        if self.prefix is not None:
            ret['mtime'] = self.mtime
        if self.hash_function is not None:
            ret['hash_function'] = self.hash_function.__name__
        if self.mtime_function is not None:
            ret['mtime_function'] = self.mtime_function.__name__
        return ret
    
    def get_hash(self) -> str:
        """管理対象となるファイルのハッシュ値を取得する。
        デフォルトは md5。

        Returns
        -------
        str
            ハッシュ値。
        """
        if self.hash_function is not None:
            return self.hash_function(self.path)
        return hash_md5(self.path)

    def get_mtime(self) -> float:
        """管理対象となるファイルの last modified time を取得する。
        デフォルトは path.stat().st_mtime。

        Returns
        -------
        float
            last modified time.
        """
        return self.path.stat().st_mtime

    def update_hash(self, copy: bool=False) -> ManagedFile:
        """管理対象となるファイルのハッシュ値を更新したインスタンスを返す。

        Parameters
        ----------
        copy : bool, optional
            インスタンスをコピーするかどうか。(by default False)
            管理対象の量が膨大な場合、コピー作成はメモリを圧迫するのでデフォルトで False。
        
        Returns
        -------
        ManagedFile
            ハッシュ値が更新された ManagedFile のインスタンス。
        """
        mf = self if not copy else self.copy()
        mf.hash = mf.get_hash()
        return mf

    def update_mtime(self, copy: bool=False) -> ManagedFile:
        """管理対象となるファイルの mtime を更新した新しいインスタンスを返す。

        Parameters
        ----------
        copy : bool, optional
            インスタンスをコピーするかどうか。(by default False)
            管理対象の量が膨大な場合、コピー作成はメモリを圧迫するのでデフォルトで False。
        
        Returns
        -------
        ManagedFile
            mtime が更新された ManagedFile のインスタンス。
        """
        mf = self if not copy else self.copy()
        mf.mtime = mf.get_mtime()
        return mf
    
    def update(self, copy: bool=False, update_hash: bool=True, update_mtime: bool=True) -> ManagedFile:
        """管理対象となるファイルの hash, mtime を更新した新しいインスタンスを返す。

        Parameters
        ----------
        copy : bool, optional
            インスタンスをコピーするかどうか。(by default False)
            管理対象の量が膨大な場合、コピー作成はメモリを圧迫するのでデフォルトで False。
        update_hash : bool, optional
            ハッシュ値を更新するかどうか。(by default True)
        update_mtime : bool, optional
            mtime を更新するかどうか。(by default True)

        Returns
        -------
        ManagedFile
            更新された ManagedFile のインスタンス。
        """
        mf = self if not copy else self.copy()

        if update_hash:
            mf.hash = mf.get_hash()
        if update_mtime:
            mf.mtime = mf.get_mtime()

        return mf

    def defer_hash(self) -> ManagedFile:
        """hash が None であれば更新する。
        計算は一般に重たいので直前まで計算を遅延したいときに用いる。
        あくまで本来は初期化時に行うべき計算を遅延させただけなのでインスタンスのコピーは行わない。
        インスタンスをコピーしたいときは update で copy=True を指定する。

        Returns
        -------
        ManagedFile
            hash が更新されたインスタンス。
        """
        if self.hash is None:
            return self.update_hash(copy=False)
        return self

    def defer_mtime(self) -> ManagedFile:
        """mtime が None であれば更新する。
        取得は一般にディスクアクセスを伴うので直前まで取得を遅延したいときに用いる。
        あくまで本来は初期化時に行うべき計算を遅延させただけなのでインスタンスのコピーは行わない。
        インスタンスをコピーしたいときは update で copy=True を指定する。

        Returns
        -------
        ManagedFile
            mtime が更新されたインスタンス。
        """
        if self.mtime is None:
            return self.update_mtime(copy=False)
        return self
    
    def defer_update(self) -> ManagedFile:
        """hash と mtime のうち None であるものを更新する。
        取得を遅延したい場合に用いる。
        あくまで本来は初期化時に行うべき計算を遅延させただけなのでインスタンスのコピーは行わない。
        インスタンスをコピーしたいときは update で copy=True を指定する。

        Returns
        -------
        ManagedFile
            hash と mtime が更新されたインスタンス。
        """
        return self.defer_hash().defer_mtime()
    
    def is_same_file_with(self, other: ManagedFile, aware: str="mtime_aware") -> bool:
        """ファイルの同一性を比較する。

        Parameters
        ----------
        other : ManagedFile
            比較対象のファイル。
        aware : str, optional
            比較方法。(by default "mtime_aware")
            指定可能なモードは "mtime_only", "hash_only", "mtime_aware", "both" の４つ。
            hash と mtime の計算が遅延されている場合、"mtime_aware" が高速性と確実性に優れる。
            ハッシュは一定確率で衝突する可能性があることに注意する。
            ハッシュの衝突を憂慮するのであれば "mtime_only" を使用する。

        Returns
        -------
        bool
            * "mtime_aware" ... mtime が一致する場合は True。一致しない場合は hash を比較し、一致すれば True。mtime も hash も一致しない場合は False。
            * "mtime_only" ... mtime が一致する場合は True。一致しない場合は False。
            * "hash_only" ... hash が一致する場合は True。一致しない場合は False。
            * "both" ... hash と mtime が両方とも一致する場合は True。一致しない場合は False。
            * "strict" ... filecmp を使用した比較。

        Raises
        ------
        ValueError
            aware に "mtime_only", "hash_only", "mtime_aware", "both", "strict" 以外のモードが指定されている。
        """
        aware_set = { "mtime_only", "hash_only", "mtime_aware", "both", "strict" }

        if aware not in aware_set:
            raise ValueError(f"aware must be one of {aware_set}.")

        if aware == "mtime_only":
            # mtime が未取得なら更新
            self.defer_mtime()
            other.defer_mtime()
            if self.mtime == other.mtime:
                return True
            return False
        elif aware == "hash_only":
            # hash が未取得なら更新
            self.defer_hash()
            other.defer_hash()
            if self.hash == other.hash:
                return True
            return False
        elif aware == "mtime_aware":
            # mtime が未取得なら更新
            self.defer_mtime()
            other.defer_mtime()
            if self.mtime == other.mtime:
                return True
            # hash が未取得なら更新
            self.defer_hash()
            other.defer_hash()
            if self.hash == other.hash:
                return True
            return False
        elif aware == "both":
            # hash, mtime が未取得なら更新
            self.defer_update()
            other.defer_update()
            if (self.mtime == other.mtime) and (self.hash == other.hash):
                return True
            return False
        elif aware == "strict":
            return filecmp.cmp(str(self.path), str(other.path), shallow=True)

class ManagedDirectory:
    """管理対象となるファイルをディレクトリ単位で扱うためのクラス。
    デフォルトで `__pycache__` と `.ipynb_checkpoints` を含むファイル名は無視し、
    これらのディレクトリにあるファイルを管理対象に含めることはできない。

    Attributes
    ----------
    path : Path, immutable
        管理対象となるディレクトリのパス。
    glob : str, optional, immutable
        管理対象となるファイルを列挙するときの glob pattern。(by default '**/*')
        path.glob(pattern) に指定する pattern。
    group : str, optional, immutable
        管理グループが存在する場合、そのグループ名。(by default None)
    ignore : Iterable[str], optional, immutable
        列挙されたファイルの parts にこの引数の中にある文字列が含まれるとき列挙から除外する。(by default None)
        None が指定された場合は "__pycache__" と ".ipynb_checkpoints" を無視する。
    """
    def __init__(self, path: Path, glob: str='**/*', key: str=None, group: str=None, ignore: Iterable[str]=None):
        """_summary_

        Parameters
        ----------
        path : Path
            管理対象となるディレクトリのパス。
        glob : str, optional
            管理対象となるファイルを列挙するときの glob pattern。(by default '**/*')
            path.glob(pattern) に指定する pattern。
        key : str, optional
            パスとは別に、管理対象を識別するためのキー。
        group : str, optional
            管理グループが存在する場合、そのグループ名。(by default None)
        ignore : Iterable[str], optional
            列挙されたファイルの parts にこの引数の中にある文字列が含まれるとき列挙から除外する。(by default None)
            None が指定された場合は "__pycache__" と ".ipynb_checkpoints" を無視する。

        Raises
        ------
        FileNotFoundError
            指定されたパスにディレクトリが存在しないか、指定されたパスがディレクトリではない。
        TypeError
            group に指定された引数が文字列ではない。利便性のため位置引数での指定を可能にしているので
            ignore との指定ミスによるバグを未然に防ぐため型チェックを行う。
        TypeError
            ignore に文字列の集合ではなく文字列が指定された。group との誤指定がありうるのでエラーとなる。
        """
        if (group is not None) and (not isinstance(group, str)):
            raise TypeError(f"group must be None or instance of str.")
        if isinstance(ignore, str):
            raise TypeError(f"Assigning a string to 'ignore' is prohibited due to the potential for misinterpreting the 'group' specification.")

        self._path = Path(path)
        self._glob = glob
        self._key = key
        self._group = group

        self._ignore = { '__pycache__', '.ipynb_checkpoints' }
        if ignore is not None:
            self._ignore |= set(ignore)

        if not self.path.is_dir():
            raise FileNotFoundError(f"No such directory: '{self.path}'.")
    
    @property
    def path(self):
        return self._path
    
    @property
    def glob(self):
        return self._glob
    
    @property
    def key(self):
        return self._key

    @property
    def group(self):
        return self._group
    
    @property
    def ignore(self):
        return self._ignore

    def __repr__(self):
        if self.group is None:
            return f"{self.__class__.__name__}('{self.path}', glob='{self.glob}', ignore={self.ignore})"
        return f"{self.__class__.__name__}('{self.path}', glob='{self.glob}', group='{self.group}', ignore={self.ignore})"

    def copy(self) -> ManagedDirectory:
        return ManagedDirectory(
            path=copy.copy(self.path),
            glob=copy.copy(self.glob),
            key=copy.copy(self.key),
            group=copy.copy(self.group),
            ignore=copy.copy(self.ignore)
        )

    def as_dict(self) -> dict:
        """ManagedDirectory の引数に与えたり、JSON に dump できる形式の辞書にする。

        Returns
        -------
        dict
            ManagedDirectory の情報を必要十分に保持する辞書。
        """
        ret = {
            'path': str(self.path),
            'glob': self.glob,
        }
        if self.key is not None:
            ret['key'] = self.key
        if self.group is not None:
            ret['group'] = self.group
        ret['ignore'] = list(self.ignore)
        return ret
    
    def files(self) -> List[ManagedFile]:
        """管理対象となるファイルを列挙する。

        Returns
        -------
        List[ManagedFile]
            管理対象となるファイルのリスト。
        """
        ps = [ p for p in sorted(self.path.glob(self.glob)) if p.is_file() ]
        return [ ManagedFile(path=p, key=self.key, prefix=self.path) for p in ignore_path(ps, self.ignore) ]
