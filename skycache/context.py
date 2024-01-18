from __future__ import annotations

import json
import shutil

from io import StringIO
from pathlib import Path
from tqdm.auto import tqdm

from typing import List, Iterable, Mapping, Union

from .frozen import FrozenContext
from .managed import ManagedFile, ManagedDirectory
from .snapshot import Snapshot
from .utils import get_unique_name

class Resource:
    """管理対象としたいファイルとディレクトリに対する様々な操作を提供する。
    Resource は管理対象となるファイルとディレクトリを選ぶためのルールで定義されるため、
    具体的にどのファイルを管理するかまでは確定しない。
    .snapshot() で Snapshot に変換することで管理対象となるファイルが確定する。

    Resource は自身を他のリソースと区別するための識別名(name)と、管理ルールのテーブル(table)を内部に持つ。
    テーブルは辞書であり、キーはその管理対象の管理名、値は ManagedFile または ManagedDirectory である。
    キーが重複しない限りは同じパスの対象でも管理することができる。キーを指定しない場合は管理対象のパス名がキーとなる。
    管理対象のパス名とキーが一致している場合、その管理対象のキーは空文字列であるものとしてデータベースに記録される。
    既に登録されているキーを重複してテーブルに追加する操作はエラーになる。
    
    データベースの整合性を保つため、一度定義した Resource をコピーしたり書き換えたりしてはならない。
    そのためコピーに類する機能は提供されない。
    """
    def __init__(self, name: str, obj=None):
        """リソースを他のリソースと識別する名前(name)を与える。
        obj にはリソースに追加する管理対象を与えることができ、obj が None でない場合、add 関数に渡される。

        Parameters
        ----------
        name : str
            _description_
        obj : _type_, optional
            _description_, by default None
        """
        self._name = name
        self._table = {}

        if obj is not None:
            self.add(obj)

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}', {self.table})"

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    def _add(self, key: str, path: Path, *, glob: str='**/*', group: str=None, ignore: Iterable[str]=None):
        """add 関数のバックエンド。キーをテーブルに追加する。

        Parameters
        ----------
        key : str
            管理対象を識別し、テーブルに登録されるキー。
        path : Path
            管理対象のパス。
        glob : str, optional
            管理対象がディレクトリである場合のファイルの列挙パターン。(by default '**/*')
            管理対象がファイルの場合は無視される。
        group : str, optional
            管理対象に付与可能なグループラベル。(by default None)
        ignore : Iterable[str], optional
            管理対象がディレクトリである場合、管理から除外するサブディレクトリ名。(by default None)
            デフォルトで '__pycache__' と '.ipynb_checkpoints' が追加される。

        Raises
        ------
        KeyError
            既に存在するキーを追加しようとした。
        FileNotFoundError
            指定したパスに対象が存在しない。または対象がファイルでもディレクトリでもない。
        """
        if key in self.table:
            raise KeyError(f"key '{key}' is already exists in table.")
        
        path = Path(path)
        
        if (not path.is_file()) and (not path.is_dir()):
            raise FileNotFoundError(f"path is not file nor directory: '{path}'.")

        if path.is_file():
            self.table[key] = ManagedFile(path=path, key=key, group=group)
        elif path.is_dir():
            self.table[key] = ManagedDirectory(path=path, glob=glob, key=key, group=group, ignore=ignore)

    def add(self, path: Path, glob: str='**/*', *, key: str=None, group: str=None, ignore: Iterable[str]=None):
        """リソースの追加を行う関数。ファイル、ディレクトリのどちらでも与えることができ、
        それぞれ ManagedFile, ManagedDirectory に対応するように引数を与える。
        位置引数で与えることができるのは path, glob のみである。
        それ以上は位置引数で何を指定するつもりなのかが曖昧であり、
        バグの原因となりうるので許可しない。
        
        複数の管理対象をリストや辞書などで path に渡すことが可能。
        * 単に Path のリストの場合は、glob, group, ignore を引き継いで各 Path の add が実行される。
        * [Path, str] のリストの場合はそれぞれを第一引数、第二引数として、group, ignore を引き継いで add が実行される。
        * 辞書のリストの場合、各辞書をキーワード引数として展開して add が実行される。
        * 辞書が与えられた場合、その辞書のキーをテーブルのキーとする。値についてはリストで与える場合と同様の場合分けが行われる。


        Parameters
        ----------
        path : Path, Iterable, Mapping
            管理対象のパス。Iterable, Mapping の場合は展開して追加を行う。
        glob : str, optional
            管理対象がディレクトリの場合のファイル列挙パターン。(by default '**/*')
            path が指す対象がファイルなら無視される。
        group : str, optional
            管理対象に付与するグループラベル。(by default None)
        ignore : Iterable[str], optional
            管理対象がディレクトリの場合、無視するサブディレクトリ・ファイル名。(by default None)
            デフォルトで '__pycache__' と '.ipynb_checkpoints' が追加される。

        Raises
        ------
        KeyError
            既に存在するキーを追加しようとした。
        FileNotFoundError
            指定したパスに対象が存在しない。または対象がファイルでもディレクトリでもない。
        """
        if isinstance(path, (str, Path)):
            # キーは指定しなければパスを文字列化したものになる
            key = key if key is not None else str(path)
            self._add(key=key, path=path, glob=glob, group=group, ignore=ignore)
            return self

        # 以下は Iterable, Mapping に対する処理。

        def inherit_args(args, **kwargs):
            ret = { **kwargs }
            if isinstance(args, (str, Path)):
                ret['path'] = args
            elif isinstance(args, Mapping):
                for k, v in args.items():
                    ret[k] = v
            elif isinstance(args, Iterable):
                if len(args) == 1:
                    ret['path'] = args[0]
                elif len(args) == 2:
                    ret['path'] = args[0]
                    ret['glob'] = args[1]
                else:
                    raise TypeError(f"too many or less positional arguments.")
            else:
                TypeError(f"Unrecognized arguments: '{type(args)}'")
            
            if 'path' not in ret:
                raise TypeError(f"path must be specified.")

            if 'key' not in ret:
                ret['key'] = str(ret['path'])

            return ret

        if isinstance(path, Mapping):
            for k, args in path.items():
                kwargs = inherit_args(args, key=str(k), glob=glob, group=group, ignore=ignore)
                self._add(**kwargs)
        elif isinstance(path, Iterable):
            for args in path:
                kwargs = inherit_args(args, glob=glob, group=group, ignore=ignore)
                self._add(**kwargs)
        
        return self

    def as_dict(self) -> dict:
        """Resource のテーブルを JSON 形式で保存可能な辞書に変換する。

        Returns
        -------
        dict
            JSON 形式で保存可能な Resource のテーブル情報。
        """
        return { str(v.path): v.as_dict() for v in self.table.values() }
    
    def dump(self, fp, indent: int=None):
        """JSON 形式で Resource のテーブルをダンプする。

        Parameters
        ----------
        f : file-like object
            .write() がサポートされているファイルライクオブジェクト。
        indent : int, optional
            JSON のインデント。(by default None)
        """
        json.dump(self.as_dict(), fp, indent=indent)

    def dumps(self, indent: int=2) -> str:
        """Resource のテーブルを JSON 形式の文字列でダンプする。

        Parameters
        ----------
        indent : int, optional
            JSON のインデント。(by default 2)

        Returns
        -------
        str
            JSON 形式の文字列。
        """
        with StringIO() as f:
            self.dump(f, indent=indent)
            xs = f.getvalue()
        return xs
    
    def save(self, dest_dir: Path) -> Path:
        """現在のリソースのテーブル情報を JSON 形式で保存する。
        保存先のファイル名は [name].json であり、Resource に付けられた名前に一致する。
        指定ミスを防ぐためこの関数で保存名を指定することはできない。
        .dump() を用いれば同じ内容を別名で保存することは可能。

        Parameters
        ----------
        dest_dir : Path
            保存先となるディレクトリのパス。

        Returns
        -------
        Path
            テーブル情報を保存したパス。
        """
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)

        save_path = dest_dir / f"{self.name}.json"

        with open(str(save_path), 'w') as f:
            self.dump(f, indent=2)
        
        return save_path
    
    def list(self, sort_by_name: bool=False) -> List[Union[ManagedFile, ManagedDirectory]]:
        """管理対象を指定するルールの一覧を返す。

        Parameters
        ----------
        sort_by_name : bool, optional
            管理ルールをテーブルのキーでソートする。(by default False)

        Returns
        -------
        List[Union[ManagedFile, ManagedDirectory]]
            管理ルールのリスト。
        """
        if not sort_by_name:
            table = self.table
        else:
            table = dict(sorted(self.table.items(), key=lambda x: x[0]))

        return list(table.values())
    
    def files(self, sort_by_name: bool=False) -> List[ManagedFile]:
        """管理されているファイルの一覧を返す。

        Parameters
        ----------
        sort_by_name : bool, optional
            管理ルールを列挙する際にテーブルのキーでソートする。(by default False)

        Returns
        -------
        List[ManagedFile]
            管理対象となっているファイルのリスト。
        """
        ret = []

        for v in self.list(sort_by_name=sort_by_name):
            if isinstance(v, ManagedFile):
                ret.append(v.copy())
            elif isinstance(v, ManagedDirectory):
                ret.extend(v.files())
        return ret

    def snapshot(self) -> Snapshot:
        """管理対象の Snapshot を返す。

        Returns
        -------
        Snapshot
            管理対象の Snapshot。
        """
        return Snapshot(name=self.name, files=self.files(sort_by_name=True))

    def copy(self, dest_dir: Path, verbose=False):
        """管理対象となっているファイルを、ディレクトリ構造を保ったまま別のディレクトリにコピーする。

        Parameters
        ----------
        dest_dir : Path
            コピー先のディレクトリ。
        verbose : bool, optional
            コピーの進捗状況を表示するかどうか。(by default False)
        """
        return self.snapshot().copy(dest_dir=dest_dir, verbose=verbose)

    def cache_increments(self, db_path: Path, cache_root_dir: Path, verbose=False):
        tag = get_unique_name()

        snap = self.snapshot()
        df, idx = snap.cache_increments(
            db_path=db_path,
            cache_root_dir=cache_root_dir,
            tag=tag,
            update=True,
            verbose=verbose,
            return_index=True
        )

        return df, idx
    
    def freeze(self, db_path: Path, cache_root_dir: Path, verbose=False):
        df, idx = self.cache_increments(
            db_path=db_path,
            cache_root_dir=cache_root_dir,
            verbose=verbose
        )

        return FrozenContext(df, cache_root_dir=cache_root_dir, inc_idx=idx)