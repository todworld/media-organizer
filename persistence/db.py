import sqlite3
from pathlib import Path

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.db_path, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA temp_store=MEMORY;")
        return con

    def init(self) -> None:
        from pathlib import Path
        schema_path = Path(__file__).parent / "schema.sql"
        sql = schema_path.read_text(encoding="utf-8")
        con = self.connect()
        try:
            con.executescript(sql)
            con.commit()
        finally:
            con.close()
