import sqlite3
import pandas as pd
from pathlib import Path

from typing import List

from .managed import ManagedFile

def is_table_exists(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        query = f"SELECT COUNT(*) FROM sqlite_master WHERE TYPE='table' AND name='{table_name}'"
        
        cur.execute(query)
    
        res = cur.fetchone()
        # 戻り値にはヒットしたテーブルの個数が入っている
        if res[0] == 0:
            exists = False
        else:
            exists = True
    finally:
        conn.close()
    
    return exists

def drop_table(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        query = f"DROP TABLE IF EXISTS '{table_name}'"
        
        cur.execute(query)
    finally:
        conn.close()

def get_column_info(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        query = f"PRAGMA table_info('{table_name}')"
        res = cur.execute(query)
        
        column_info = res.fetchall()
    finally:
        conn.close()
    
    column_info = [ column for column in column_info ]
    
    return column_info

### recent
def create_table_recent(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # group が sqlite3 の予約語なので `` で囲んでエスケープしている。
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
            `path` TEXT,
            `key` TEXT,
            `tag` TEXT,
            `refer` TEXT,
            `group` TEXT,
            `prefix` TEXT,
            `hash` TEXT,
            `time` REAL,
            `mtime` REAL,
            UNIQUE(path, key)
        )
        """
        
        cur.execute(query)
    finally:
        conn.close()

def insert_or_replace_into_recent(db_path: Path, table_name: str, data):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        query = f"INSERT OR REPLACE INTO `{table_name}` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        cur.executemany(query, data)
        conn.commit()
    finally:
        conn.close()

def read_table_recent(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        
        query = f"SELECT * FROM `{table_name}`"
        
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df

def read_table_recent_select(db_path: Path, table_name: str, managed_files: List[ManagedFile]):
    def to_query(managed_files):
        return ", ".join([f"('{mf.path}', '{mf.key}')" for mf in managed_files ])
        
    try:
        conn = sqlite3.connect(str(db_path))
        
        query = f"SELECT * FROM `{table_name}` WHERE (`path`, `key`) IN ({to_query(managed_files)})"
        
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df

### history
def create_table_history(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        # group が sqlite3 の予約語なので `` で囲んでエスケープしている。
        query = f"""CREATE TABLE IF NOT EXISTS history_{table_name} (
            `path` TEXT,
            `key` TEXT,
            `tag` TEXT,
            `refer` TEXT,
            `group` TEXT,
            `prefix` TEXT,
            `hash` TEXT,
            `time` REAL,
            `mtime` REAL,
            UNIQUE(path, key, tag)
        )
        """
        
        cur.execute(query)
    finally:
        conn.close()

def insert_or_replace_into_history(db_path: Path, table_name: str, data):
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        
        query = f"INSERT OR REPLACE INTO `history_{table_name}` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        cur.executemany(query, data)
        conn.commit()
    finally:
        conn.close()

def read_table_history(db_path: Path, table_name: str):
    try:
        conn = sqlite3.connect(str(db_path))
        
        query = f"SELECT * FROM `history_{table_name}`"
        
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df

def read_table_history_select(db_path: Path, table_name: str, managed_files: List[ManagedFile]):
    def to_query(managed_files):
        return ", ".join([f"('{mf.path}', '{mf.key}')" for mf in managed_files ])
        
    try:
        conn = sqlite3.connect(str(db_path))
        
        query = f"SELECT * FROM `history_{table_name}` WHERE (`path`, `key`) IN ({to_query(managed_files)})"
        
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df

def search_history(db_path: Path, table_name: str, df: pd.DataFrame, mode: str="recent", aware: str="both"):
    """
    data = List[[path, key, hash, mtime]]
    """
    data = [ (row['path'], row['key'], row['hash'], row['mtime']) for _, row in df.iterrows() ]

    if aware == "hash":
        cond = "H.path = C.path AND H.key = C.key AND H.hash = C.hash"
    elif aware == "mtime":
        cond = "H.path = C.path AND H.key = C.key AND H.mtime = C.mtime"
    elif aware == "both":
        cond = "H.path = C.path AND H.key = C.key AND H.hash = C.hash AND H.mtime = C.mtime"

    if mode == "all":
        select_query = f"""
            SELECT H.*
            FROM (
                SELECT A.* FROM `history_{table_name}` A INNER JOIN current B ON A.path = B.path AND A.key = B.key
            ) H JOIN current C ON {cond}
        """
    elif mode == "recent":
        select_query = f"""
            WITH T AS (
                SELECT H.*,
                    ROW_NUMBER() OVER (PARTITION BY H.path ORDER BY H.time DESC) rn
                FROM (
                    SELECT A.* FROM `history_{table_name}` A INNER JOIN current B ON A.path = B.path AND A.key = B.key
                ) H JOIN current C ON {cond}
            )
            SELECT `path`, `key`, `tag`, `refer`, `group`, `prefix`, `hash`, `time`, `mtime` FROM T WHERE rn = 1
        """
    
    try:
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
    
        query = f"CREATE TEMPORARY TABLE current (path TEXT, key TEXT, hash TEXT, mtime REAL, UNIQUE(path, key))"
        cur.execute(query)
        
        query = f"INSERT OR REPLACE INTO current VALUES(?, ?, ?, ?)"
        cur.executemany(query, data)
        
        df = pd.read_sql_query(select_query, conn)
    finally:
        conn.close()

    return df