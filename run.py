import argparse
import datetime as _dt
import re
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import yaml


# Modello di configurazione runtime (caricato da YAML)
@dataclass(frozen=True)
class DbConfig:
    dt_type: str
    host: str
    port: int
    database: str
    table_view_in: Any
    table_view_out: Any
    user: str
    password: str
    driver: str | None = None


def _require(value: Any, field: str) -> Any:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"Parametro mancante: {field}")
    return value


# Normalizza dt_type (numero o stringa) e calcola la porta di default per l'engine scelto
def _resolve_dt_type_and_default_port(dt_type: str) -> tuple[str, int]:
    v = str(dt_type).strip()
    mapping: dict[str, tuple[str, int]] = {
        "1": ("MSSQL", 1433),
        "2": ("MariaDB", 3306),
        "3": ("MySQL", 3306),
        "4": ("PostgreSQL", 5432),
    }
    if v in mapping:
        return mapping[v]

    v_lower = v.lower()
    if v_lower in {"mssql", "sqlserver", "ms sql server", "ms_sql_server"}:
        return "MSSQL", 1433
    if v_lower in {"mariadb"}:
        return "MariaDB", 3306
    if v_lower in {"mysql"}:
        return "MySQL", 3306
    if v_lower in {"postgres", "postgresql"}:
        return "PostgreSQL", 5432
    raise ValueError(f"dt_type non supportato: {dt_type}")


# Carica config YAML e valida i campi obbligatori per source/destination
def load_config(path: str) -> tuple[DbConfig, DbConfig]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    src = raw.get("source_database") or {}
    dst = raw.get("destination_database") or {}

    def to_cfg(section: dict[str, Any], prefix: str) -> DbConfig:
        dt_raw = str(_require(section.get("dt_type"), f"{prefix}.dt_type")).strip()
        _dt_name, default_port = _resolve_dt_type_and_default_port(dt_raw)
        port_raw = section.get("port")
        port = default_port if port_raw is None or (isinstance(port_raw, str) and not port_raw.strip()) else int(port_raw)
        return DbConfig(
            dt_type=dt_raw,
            host=str(_require(section.get("host"), f"{prefix}.host")).strip(),
            port=port,
            database=str(_require(section.get("database"), f"{prefix}.database")).strip(),
            table_view_in=section.get("table_view_in", "all"),
            table_view_out=section.get("table_view_out"),
            user=str(_require(section.get("user"), f"{prefix}.user")).strip(),
            password=str(_require(section.get("password"), f"{prefix}.password")),
            driver=str(section.get("driver")).strip() if section.get("driver") is not None else None,
        )

    return to_cfg(src, "source_database"), to_cfg(dst, "destination_database")


# Interpreta table_view_in: "all" (tutte), singola tabella, oppure lista separata da ';'
def _parse_table_view_in(value: Any) -> tuple[str, list[str]]:
    if value is None:
        return "all", []
    if isinstance(value, str):
        v = value.strip()
        if not v or v.lower() == "all":
            return "all", []
        v_lower = v.lower()
        for prefix in ("view:", "v:"):
            if v_lower.startswith(prefix):
                name = v[len(prefix) :].strip()
                if not name:
                    raise ValueError("table_view_in non valido: specifica il nome dopo 'view:'")
                return "single_view", [name]
        parts = [p.strip() for p in v.split(";") if p.strip()]
        if len(parts) <= 1:
            return "single", [v]
        return "list", parts
    raise ValueError("table_view_in deve essere 'all', un nome tabella, 'view:<nome_view>', o più nomi separati da ';'")


# Ordina le tabelle in base alle dipendenze FK (prima i parent, poi i child) per ridurre errori in creazione FK
def _order_tables_by_fk_dependencies(source: "DbAdapter", conn, tables: list[str]) -> list[str]:
    nodes = {t.lower(): t for t in tables}
    node_keys = set(nodes.keys())
    parents: dict[str, set[str]] = {k: set() for k in node_keys}
    for t in tables:
        child_key = t.lower()
        for fk in source.list_foreign_keys(conn, t):
            parent = str(fk.get("parent_table") or "").strip()
            if not parent:
                continue
            parent_key = parent.lower()
            if parent_key in node_keys:
                parents[child_key].add(parent_key)

    indeg: dict[str, int] = {k: 0 for k in node_keys}
    children: dict[str, set[str]] = {k: set() for k in node_keys}
    for child, ps in parents.items():
        indeg[child] = len(ps)
        for p in ps:
            children[p].add(child)

    order_hint = {k: i for i, k in enumerate([t.lower() for t in tables])}
    ready = sorted([k for k, d in indeg.items() if d == 0], key=lambda k: order_hint.get(k, 10**9))
    out: list[str] = []
    remaining = set(node_keys)
    while ready:
        k = ready.pop(0)
        if k not in remaining:
            continue
        remaining.remove(k)
        out.append(nodes[k])
        for ch in sorted(children.get(k, set()), key=lambda x: order_hint.get(x, 10**9)):
            indeg[ch] -= 1
            if indeg[ch] == 0:
                ready.append(ch)

    if remaining:
        for k in sorted(remaining, key=lambda x: order_hint.get(x, 10**9)):
            out.append(nodes[k])
    return out


# Interfaccia DB: operazioni minime per leggere schema/dati dal source e ricrearli sul destination
class DbAdapter:
    kind: str

    def create_database_if_missing(self, database: str) -> None:
        raise NotImplementedError

    def connect(self, database: str):
        raise NotImplementedError

    def list_tables(self, conn) -> list[str]:
        raise NotImplementedError

    def list_views(self, conn) -> list[str]:
        raise NotImplementedError

    def list_procedures(self, conn) -> list[str]:
        raise NotImplementedError

    def get_table_columns(self, conn, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_table_primary_key(self, conn, table: str) -> list[str]:
        raise NotImplementedError

    def get_view_definition(self, conn, view: str) -> str:
        raise NotImplementedError

    def get_procedure_definition(self, conn, proc: str) -> str:
        raise NotImplementedError

    def render_create_table(self, table: str, columns: list[dict[str, Any]], pk: list[str], include_defaults: bool = True) -> str:
        raise NotImplementedError

    def insert_rows(self, conn, table: str, columns: list[str], rows: Iterable[Sequence[Any]]) -> int:
        raise NotImplementedError

    def table_exists(self, conn, table: str) -> bool:
        raise NotImplementedError

    def truncate_table(self, conn, table: str) -> None:
        raise NotImplementedError

    def delete_all_rows(self, conn, table: str) -> None:
        raise NotImplementedError

    def list_indexes(self, conn, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def list_foreign_keys(self, conn, table: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    def render_create_index(self, index: dict[str, Any]) -> str:
        raise NotImplementedError

    def render_add_foreign_key(self, fk: dict[str, Any]) -> str:
        raise NotImplementedError

    def supports_cross_engine_routines(self) -> bool:
        return True


# Adapter MariaDB/MySQL (usa PyMySQL) per schema + dati + metadati (indici/FK/view/proc)
class MariaDbAdapter(DbAdapter):
    kind = "MariaDB"

    def __init__(self, cfg: DbConfig):
        self.cfg = cfg

    def create_database_if_missing(self, database: str) -> None:
        conn = self.connect(database=None)
        try:
            cur = conn.cursor()
            cur.execute("CREATE DATABASE IF NOT EXISTS `{}`".format(database.replace("`", "``")))
            conn.commit()
        finally:
            conn.close()

    def connect(self, database: str | None):
        import pymysql

        return pymysql.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            user=self.cfg.user,
            password=self.cfg.password,
            database=database,
            charset="utf8mb4",
            autocommit=False,
        )

    def list_tables(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
        )
        return [r[0] for r in cur.fetchall()]

    def list_views(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
            """
        )
        return [r[0] for r in cur.fetchall()]

    def list_procedures(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ROUTINE_NAME
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_NAME
            """
        )
        return [r[0] for r in cur.fetchall()]

    def get_table_columns(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (table,),
        )
        out: list[dict[str, Any]] = []
        for (
            name,
            data_type,
            char_len,
            num_prec,
            num_scale,
            dt_prec,
            is_nullable,
            col_default,
            extra,
        ) in cur.fetchall():
            out.append(
                {
                    "name": name,
                    "data_type": str(data_type).lower() if data_type is not None else "",
                    "char_len": char_len,
                    "num_prec": num_prec,
                    "num_scale": num_scale,
                    "dt_prec": dt_prec,
                    "nullable": str(is_nullable).upper() == "YES",
                    "default": col_default,
                    "auto_increment": (extra or "").lower().find("auto_increment") >= 0,
                }
            )
        return out

    def get_table_primary_key(self, conn, table: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT k.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
              ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
             AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA
             AND tc.TABLE_NAME = k.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = DATABASE()
              AND tc.TABLE_NAME = %s
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY k.ORDINAL_POSITION
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]

    def get_view_definition(self, conn, view: str) -> str:
        cur = conn.cursor()
        cur.execute("SHOW CREATE VIEW `{}`".format(view.replace("`", "``")))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Impossibile leggere definizione view: {view}")
        return row[1]

    def get_procedure_definition(self, conn, proc: str) -> str:
        cur = conn.cursor()
        cur.execute("SHOW CREATE PROCEDURE `{}`".format(proc.replace("`", "``")))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"Impossibile leggere definizione procedura: {proc}")
        return row[2]

    def render_create_table(self, table: str, columns: list[dict[str, Any]], pk: list[str], include_defaults: bool = True) -> str:
        pk_set = set(x.lower() for x in pk)
        for c in columns:
            if c["name"].lower() in pk_set:
                c["is_pk"] = True

        parts: list[str] = []
        for c in columns:
            rendered_type = self._render_col_type(c)
            col = f"`{c['name'].replace('`', '``')}` {rendered_type}"
            if c.get("auto_increment"):
                col += " AUTO_INCREMENT"
            col += " NULL" if c.get("nullable", True) else " NOT NULL"
            default = c.get("default")
            if include_defaults and self._default_is_allowed(default, rendered_type):
                col += f" DEFAULT {self._render_default(default)}"
            parts.append(col)
        if pk:
            cols = ", ".join(f"`{x.replace('`', '``')}`" for x in pk)
            parts.append(f"PRIMARY KEY ({cols})")
        body = ",\n  ".join(parts)
        return f"CREATE TABLE `{table.replace('`', '``')}` (\n  {body}\n) ENGINE=InnoDB"

    def _default_is_allowed(self, default: Any, rendered_type: str) -> bool:
        if default is None:
            return False
        rt = (rendered_type or "").upper()
        if rt in {"LONGTEXT", "LONGBLOB"}:
            return False
        s = str(default).strip()
        if re.match(r"^0{4}-0{2}-0{2}", s):
            return False
        if s.upper() == "NULL":
            return True
        return True

    def _render_default(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        s = str(value).strip()
        if s.upper() in {"CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()", "NOW()", "NOW"}:
            return "CURRENT_TIMESTAMP"
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            return s
        if s.startswith("'") and s.endswith("'"):
            return s
        return "'" + s.replace("'", "''") + "'"

    def _render_col_type(self, c: dict[str, Any]) -> str:
        t = (c.get("data_type") or "").lower()
        char_len = c.get("char_len")
        num_prec = c.get("num_prec")
        num_scale = c.get("num_scale")
        if t in {"varchar", "char"}:
            n = int(char_len or 255)
            return f"{t.upper()}({n})"
        if t in {"nvarchar", "nchar"}:
            n = int(char_len or 255)
            return f"{'VARCHAR' if t == 'nvarchar' else 'CHAR'}({n})"
        if t in {"text", "mediumtext", "longtext"}:
            return "LONGTEXT"
        if t in {"blob", "mediumblob", "longblob"}:
            return "LONGBLOB"
        if t in {"tinyint", "smallint", "int", "integer", "bigint"}:
            return "INT" if t in {"int", "integer"} else t.upper()
        if t in {"bit", "boolean", "bool"}:
            return "TINYINT(1)"
        if t in {"decimal", "numeric"}:
            p = int(num_prec) if num_prec is not None else 18
            s = int(num_scale) if num_scale is not None else 0
            return f"DECIMAL({p},{s})"
        if t in {"float", "double", "real"}:
            return "DOUBLE"
        if t in {"datetime", "timestamp"}:
            return "DATETIME"
        if t == "date":
            return "DATE"
        if t == "time":
            return "TIME"
        if t == "json":
            return "LONGTEXT"
        return "LONGTEXT"

    def insert_rows(self, conn, table: str, columns: list[str], rows: Iterable[Sequence[Any]]) -> int:
        cur = conn.cursor()
        cols = ", ".join(f"`{c.replace('`', '``')}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO `{table.replace('`', '``')}` ({cols}) VALUES ({placeholders})"
        count = 0
        batch: list[Sequence[Any]] = []
        for r in rows:
            batch.append(r)
            if len(batch) >= 1000:
                cur.executemany(sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            count += len(batch)
        return count

    def table_exists(self, conn, table: str) -> bool:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND TABLE_TYPE='BASE TABLE'
            LIMIT 1
            """,
            (table,),
        )
        return cur.fetchone() is not None

    def truncate_table(self, conn, table: str) -> None:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE `{table.replace('`', '``')}`")

    def delete_all_rows(self, conn, table: str) -> None:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM `{table.replace('`', '``')}`")

    def list_indexes(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND INDEX_NAME <> 'PRIMARY'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX
            """,
            (table,),
        )
        grouped: dict[str, dict[str, Any]] = {}
        for idx_name, non_unique, seq, col in cur.fetchall():
            name = str(idx_name)
            g = grouped.get(name)
            if g is None:
                g = {"name": name, "unique": int(non_unique) == 0, "table": table, "columns": []}
                grouped[name] = g
            g["columns"].append(str(col))
        return list(grouped.values())

    def list_foreign_keys(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                kcu.CONSTRAINT_NAME,
                kcu.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME,
                kcu.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
              ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
             AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE kcu.TABLE_SCHEMA = DATABASE()
              AND kcu.TABLE_NAME = %s
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
              AND rc.CONSTRAINT_SCHEMA = DATABASE()
            ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """,
            (table,),
        )
        grouped: dict[str, dict[str, Any]] = {}
        for name, child_table, child_col, parent_table, parent_col, _pos in cur.fetchall():
            fk_name = str(name)
            g = grouped.get(fk_name)
            if g is None:
                g = {
                    "name": fk_name,
                    "child_table": str(child_table),
                    "parent_table": str(parent_table),
                    "child_columns": [],
                    "parent_columns": [],
                }
                grouped[fk_name] = g
            g["child_columns"].append(str(child_col))
            g["parent_columns"].append(str(parent_col))
        return list(grouped.values())

    def render_create_index(self, index: dict[str, Any]) -> str:
        name = str(index["name"]).replace("`", "``")
        table = str(index["table"]).replace("`", "``")
        unique = "UNIQUE " if index.get("unique") else ""
        cols = ", ".join(f"`{c.replace('`', '``')}`" for c in index.get("columns") or [])
        return f"CREATE {unique}INDEX `{name}` ON `{table}` ({cols})"

    def render_add_foreign_key(self, fk: dict[str, Any]) -> str:
        name = str(fk["name"]).replace("`", "``")
        child = str(fk["child_table"]).replace("`", "``")
        parent = str(fk["parent_table"]).replace("`", "``")
        child_cols = ", ".join(f"`{c.replace('`', '``')}`" for c in fk.get("child_columns") or [])
        parent_cols = ", ".join(f"`{c.replace('`', '``')}`" for c in fk.get("parent_columns") or [])
        return f"ALTER TABLE `{child}` ADD CONSTRAINT `{name}` FOREIGN KEY ({child_cols}) REFERENCES `{parent}` ({parent_cols})"


class MsSqlAdapter(DbAdapter):
    kind = "MSSQL"

    def __init__(self, cfg: DbConfig):
        self.cfg = cfg

    def _odbc_escape(self, value: str) -> str:
        s = str(value)
        if any(x in s for x in [";", "{", "}"]) or (s[:1].isspace() or s[-1:].isspace()):
            s = s.replace("}", "}}")
            return "{" + s + "}"
        return s

    def _is_login_failed(self, e: Exception) -> bool:
        msg = str(e)
        return ("Login failed for user" in msg) or ("(18456)" in msg) or ("'28000'" in msg) or ("[28000]" in msg)

    def create_database_if_missing(self, database: str) -> None:
        conn = self.connect(database="master")
        try:
            cur = conn.cursor()
            cur.execute(
                """
                IF DB_ID(?) IS NULL
                BEGIN
                    DECLARE @sql nvarchar(max) = N'CREATE DATABASE [' + REPLACE(?, ']', ']]') + N']'
                    EXEC sp_executesql @sql
                END
                """,
                (database, database),
            )
            conn.commit()
        finally:
            conn.close()

    def connect(self, database: str | None):
        import pyodbc

        if database is None:
            database = "master"

        installed = [str(d) for d in (pyodbc.drivers() or [])]
        candidates: list[str] = []
        if self.cfg.driver:
            candidates.append(self.cfg.driver)
        else:
            def ver(d: str) -> int:
                m = re.search(r"ODBC Driver\s+(\d+)\s+for\s+SQL\s+Server", d, flags=re.IGNORECASE)
                return int(m.group(1)) if m else -1

            modern = sorted(
                [d for d in installed if re.search(r"ODBC Driver\s+\d+\s+for\s+SQL\s+Server", d, flags=re.IGNORECASE)],
                key=ver,
                reverse=True,
            )
            candidates.extend(modern or ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"])

        tried: list[str] = []
        last_error: Exception | None = None
        server = f"tcp:{self.cfg.host}"
        if self.cfg.port:
            server = f"{server},{self.cfg.port}"

        for driver in candidates:
            if not driver or driver in tried:
                continue
            tried.append(driver)
            variants = [
                "Encrypt=no;TrustServerCertificate=yes;",
                "Encrypt=yes;TrustServerCertificate=yes;",
                "",
            ]
            for extra in variants:
                try:
                    conn_str = (
                        f"DRIVER={{{driver}}};"
                        f"SERVER={server};"
                        f"DATABASE={self._odbc_escape(database)};"
                        f"UID={self._odbc_escape(self.cfg.user)};"
                        f"PWD={self._odbc_escape(self.cfg.password)};"
                        "Connection Timeout=10;"
                        + extra
                    )
                    return pyodbc.connect(
                        conn_str,
                        autocommit=False,
                    )
                except Exception as e:
                    last_error = e
                    continue

        installed_msg = ", ".join(installed) if installed else "(nessun driver ODBC trovato da pyodbc)"
        has_modern = any(re.search(r"ODBC Driver\s+\d+\s+for\s+SQL\s+Server", d, flags=re.IGNORECASE) for d in installed)
        if not has_modern:
            raise RuntimeError(
                "Connessione a SQL Server fallita: driver ODBC per SQL Server non disponibile. "
                "Installa 'ODBC Driver 17 for SQL Server' o 'ODBC Driver 18 for SQL Server' (Microsoft). "
                f"Driver rilevati: {installed_msg}. "
                f"Server: {server}. "
                f"Ultimo errore: {type(last_error).__name__}: {last_error}"
            )

        if last_error is not None and self._is_login_failed(last_error):
            raise RuntimeError(
                "Connessione a SQL Server fallita per credenziali non valide o autenticazione non consentita. "
                f"Utente: {self.cfg.user}. "
                "Verifica user/password, che SQL Server sia in modalità Mixed Mode (SQL Authentication), "
                "che l'utente non sia disabilitato/bloccato e che abbia accesso al database richiesto. "
                f"Driver rilevati: {installed_msg}. "
                f"Server: {server}. "
                f"Ultimo errore: {type(last_error).__name__}: {last_error}"
            )

        raise RuntimeError(
            "Connessione a SQL Server fallita. "
            "Verifica host/porta, firewall, TCP/IP abilitato su SQL Server e che l'istanza ascolti sulla porta indicata. "
            f"Driver rilevati: {installed_msg}. "
            f"Server: {server}. "
            f"Ultimo errore: {type(last_error).__name__}: {last_error}"
        )

    def list_tables(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = 'dbo'
            ORDER BY t.name
            """
        )
        return [r[0] for r in cur.fetchall()]

    def list_views(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT v.name
            FROM sys.views v
            JOIN sys.schemas s ON s.schema_id = v.schema_id
            WHERE s.name = 'dbo'
            ORDER BY v.name
            """
        )
        return [r[0] for r in cur.fetchall()]

    def list_procedures(self, conn) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.name
            FROM sys.procedures p
            JOIN sys.schemas s ON s.schema_id = p.schema_id
            WHERE s.name = 'dbo'
            ORDER BY p.name
            """
        )
        return [r[0] for r in cur.fetchall()]

    def get_table_columns(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                c.name AS column_name,
                t.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                dc.definition AS default_definition,
                c.is_identity
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            JOIN sys.tables tb ON tb.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = tb.schema_id
            LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
            WHERE s.name = 'dbo' AND tb.name = ?
            ORDER BY c.column_id
            """,
            (table,),
        )
        out: list[dict[str, Any]] = []
        for (name, data_type, max_length, precision, scale, is_nullable, default_def, is_identity) in cur.fetchall():
            out.append(
                {
                    "name": name,
                    "data_type": str(data_type).lower(),
                    "max_length": max_length,
                    "precision": precision,
                    "scale": scale,
                    "nullable": bool(is_nullable),
                    "default": default_def,
                    "identity": bool(is_identity),
                }
            )
        return out

    def get_table_primary_key(self, conn, table: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name='dbo' AND t.name = ? AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]

    def get_view_definition(self, conn, view: str) -> str:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME('dbo') + '.' + QUOTENAME(?)))
            """,
            (view,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            raise RuntimeError(f"Impossibile leggere definizione view: {view}")
        return str(row[0])

    def get_procedure_definition(self, conn, proc: str) -> str:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT OBJECT_DEFINITION(OBJECT_ID(QUOTENAME('dbo') + '.' + QUOTENAME(?)))
            """,
            (proc,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            raise RuntimeError(f"Impossibile leggere definizione procedura: {proc}")
        return str(row[0])

    def render_create_table(self, table: str, columns: list[dict[str, Any]], pk: list[str], include_defaults: bool = True) -> str:
        pk_set = set(x.lower() for x in pk)
        for c in columns:
            if c["name"].lower() in pk_set:
                c["is_pk"] = True

        parts: list[str] = []
        for c in columns:
            col = f"[{c['name'].replace(']', ']]')}] {self._render_col_type(c)}"
            if c.get("identity"):
                col += " IDENTITY(1,1)"
            col += " NULL" if c.get("nullable", True) else " NOT NULL"
            default = c.get("default")
            if include_defaults and default is not None and str(default).strip():
                default_sql = self._normalize_default(str(default))
                if default_sql.upper() != "NULL":
                    col += f" DEFAULT {default_sql}"
            parts.append(col)
        if pk:
            safe_table_name = table.replace(']', ']]').replace('[', '')
            cols = ", ".join(f"[{x.replace(']', ']]')}]" for x in pk)
            parts.append(f"CONSTRAINT [PK_{safe_table_name}] PRIMARY KEY ({cols})")
        body = ",\n  ".join(parts)
        return f"CREATE TABLE [dbo].[{table.replace(']', ']]')}] (\n  {body}\n)"

    def _normalize_default(self, definition: str) -> str:
        d = definition.strip()
        if d.upper() == "NULL":
            return "NULL"
        while d.startswith("(") and d.endswith(")"):
            d = d[1:-1].strip()
        if d.upper() in {"GETDATE()", "SYSDATETIME()", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP()"}:
            return "GETDATE()"
        
        # Gestione apici singoli
        if d.startswith("'") and d.endswith("'"):
            return d
        
        # Se è un numero, lo teniamo così com'è
        if re.fullmatch(r"-?\d+(\.\d+)?", d):
            return d
            
        # Se è una stringa ma non ha apici (es. proveniente da MariaDB default), aggiungiamoli
        if d and not d.startswith("'") and not d.upper() in {"GETDATE()", "NULL"}:
            return "'" + d.replace("'", "''") + "'"
            
        return d

    def _render_col_type(self, c: dict[str, Any]) -> str:
        t = (c.get("data_type") or "").lower()
        max_length = c.get("max_length")
        precision = c.get("precision")
        scale = c.get("scale")
        if t in {"nvarchar", "varchar", "nchar", "char"}:
            if max_length in (-1, 0, None):
                return f"{t.upper()}(450)"
            n = int(max_length)
            if t in {"nvarchar", "nchar"}:
                n = max(1, n // 2)
            return f"{t.upper()}({n})"
        if t in {"text", "ntext"}:
            return "NVARCHAR(MAX)"
        if t in {"binary", "varbinary"}:
            if max_length in (-1, 0, None):
                return f"{t.upper()}(MAX)"
            return f"{t.upper()}({int(max_length)})"
        if t == "image":
            return "VARBINARY(MAX)"
        if t in {"tinyint", "smallint", "int", "bigint"}:
            return t.upper()
        if t == "bit":
            return "BIT"
        if t in {"decimal", "numeric"}:
            p = int(precision) if precision is not None else 18
            s = int(scale) if scale is not None else 0
            return f"DECIMAL({p},{s})"
        if t in {"float", "real"}:
            return t.upper()
        if t in {"datetime2", "datetime", "smalldatetime"}:
            return "DATETIME2" if t == "datetime2" else "DATETIME"
        if t == "date":
            return "DATE"
        if t == "time":
            return "TIME"
        if t == "uniqueidentifier":
            return "UNIQUEIDENTIFIER"
        if t == "money":
            return "DECIMAL(19,4)"
        if t == "smallmoney":
            return "DECIMAL(10,4)"
        return "NVARCHAR(MAX)"

    def insert_rows(self, conn, table: str, columns: list[str], rows: Iterable[Sequence[Any]]) -> int:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass

        cols = ", ".join(f"[{c.replace(']', ']]')}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO [dbo].[{table.replace(']', ']]')}] ({cols}) VALUES ({placeholders})"
        count = 0
        batch: list[Sequence[Any]] = []
        for r in rows:
            batch.append(r)
            if len(batch) >= 1000:
                cur.executemany(sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            cur.executemany(sql, batch)
            count += len(batch)
        return count

    def supports_cross_engine_routines(self) -> bool:
        return False

    def table_exists(self, conn, table: str) -> bool:
        safe = table.replace("]", "]]").replace("'", "''")
        cur = conn.cursor()
        cur.execute(f"SELECT 1 WHERE OBJECT_ID(N'[dbo].[{safe}]', 'U') IS NOT NULL")
        return cur.fetchone() is not None

    def truncate_table(self, conn, table: str) -> None:
        safe = table.replace("]", "]]")
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE [dbo].[{safe}]")

    def delete_all_rows(self, conn, table: str) -> None:
        safe = table.replace("]", "]]")
        cur = conn.cursor()
        cur.execute(f"DELETE FROM [dbo].[{safe}]")

    def list_indexes(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT i.name, i.is_unique, ic.key_ordinal, c.name
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            JOIN sys.tables t ON t.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name='dbo'
              AND t.name = ?
              AND i.is_primary_key = 0
              AND i.is_unique_constraint = 0
              AND i.type_desc <> 'HEAP'
              AND ic.key_ordinal > 0
            ORDER BY i.name, ic.key_ordinal
            """,
            (table,),
        )
        grouped: dict[str, dict[str, Any]] = {}
        for name, is_unique, _ord, col in cur.fetchall():
            idx_name = str(name)
            g = grouped.get(idx_name)
            if g is None:
                g = {"name": idx_name, "unique": bool(is_unique), "table": table, "columns": []}
                grouped[idx_name] = g
            g["columns"].append(str(col))
        return list(grouped.values())

    def list_foreign_keys(self, conn, table: str) -> list[dict[str, Any]]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                fk.name,
                pt.name AS child_table,
                pc.name AS child_column,
                rt.name AS parent_table,
                rc.name AS parent_column,
                fkc.constraint_column_id
            FROM sys.foreign_keys fk
            JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
            JOIN sys.tables pt ON pt.object_id = fkc.parent_object_id
            JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
            JOIN sys.columns pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
            JOIN sys.tables rt ON rt.object_id = fkc.referenced_object_id
            JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
            JOIN sys.columns rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
            WHERE ps.name='dbo' AND rs.name='dbo' AND pt.name = ?
            ORDER BY fk.name, fkc.constraint_column_id
            """,
            (table,),
        )
        grouped: dict[str, dict[str, Any]] = {}
        for name, child_table, child_col, parent_table, parent_col, _pos in cur.fetchall():
            fk_name = str(name)
            g = grouped.get(fk_name)
            if g is None:
                g = {
                    "name": fk_name,
                    "child_table": str(child_table),
                    "parent_table": str(parent_table),
                    "child_columns": [],
                    "parent_columns": [],
                }
                grouped[fk_name] = g
            g["child_columns"].append(str(child_col))
            g["parent_columns"].append(str(parent_col))
        return list(grouped.values())

    def render_create_index(self, index: dict[str, Any]) -> str:
        name = str(index["name"]).replace("]", "]]")
        table = str(index["table"]).replace("]", "]]")
        unique = "UNIQUE " if index.get("unique") else ""
        cols = ", ".join(f"[{c.replace(']', ']]')}]" for c in index.get("columns") or [])
        return f"CREATE {unique}INDEX [{name}] ON [dbo].[{table}] ({cols})"

    def render_add_foreign_key(self, fk: dict[str, Any]) -> str:
        name = str(fk["name"]).replace("]", "]]")
        child = str(fk["child_table"]).replace("]", "]]")
        parent = str(fk["parent_table"]).replace("]", "]]")
        child_cols = ", ".join(f"[{c.replace(']', ']]')}]" for c in fk.get("child_columns") or [])
        parent_cols = ", ".join(f"[{c.replace(']', ']]')}]" for c in fk.get("parent_columns") or [])
        return f"ALTER TABLE [dbo].[{child}] ADD CONSTRAINT [{name}] FOREIGN KEY ({child_cols}) REFERENCES [dbo].[{parent}] ({parent_cols})"


# Factory: costruisce l'adapter corretto in base al dt_type (numero o nome)
def build_adapter(cfg: DbConfig) -> DbAdapter:
    dt_name, _default_port = _resolve_dt_type_and_default_port(cfg.dt_type)
    t = dt_name.lower()
    if t in {"mariadb", "mysql"}:
        return MariaDbAdapter(cfg)
    if t in {"mssql", "sqlserver", "ms sql server", "ms_sql_server"}:
        return MsSqlAdapter(cfg)
    if t in {"postgresql", "postgres"}:
        raise ValueError("dt_type PostgreSQL non ancora supportato")
    raise ValueError(f"dt_type non supportato: {cfg.dt_type}")


# Iteratore streaming delle righe dal source (evita di caricare tutto in RAM)
def _iter_source_rows(source: DbAdapter, conn, table: str, columns: list[str]) -> Iterable[Sequence[Any]]:
    cur = conn.cursor()
    if source.kind == "MariaDB":
        sel_cols = ", ".join(f"`{c.replace('`', '``')}`" for c in columns)
        cur.execute(f"SELECT {sel_cols} FROM `{table.replace('`', '``')}`")
    else:
        sel_cols = ", ".join(f"[{c.replace(']', ']]')}]" for c in columns)
        cur.execute(f"SELECT {sel_cols} FROM [dbo].[{table.replace(']', ']]')}]")
    for row in cur:
        yield tuple(row)


# Traduzione "best-effort" della SELECT delle view tra MSSQL e MariaDB (conversioni semplici di quoting/funzioni)
def _translate_view_sql(sql: str, src_kind: str, dst_kind: str) -> str:
    s = sql.strip()
    if src_kind == dst_kind:
        return s
    if src_kind == "MSSQL" and dst_kind == "MariaDB":
        m = re.search(r"\bCREATE\s+VIEW\b[\s\S]*?\bAS\b\s*([\s\S]*)\Z", s, flags=re.IGNORECASE)
        select_sql = m.group(1).strip() if m else s
        select_sql = select_sql.replace("[", "`").replace("]", "`")
        select_sql = select_sql.replace("GETDATE()", "NOW()")
        return select_sql
    if src_kind == "MariaDB" and dst_kind == "MSSQL":
        m = re.search(r"\bVIEW\b[\s\S]*?\bAS\b\s*([\s\S]*)\Z", s, flags=re.IGNORECASE)
        select_sql = m.group(1).strip() if m else s
        select_sql = select_sql.replace("`", "")
        select_sql = select_sql.replace("NOW()", "GETDATE()")
        return select_sql
    return s


# Traduzione "best-effort" delle stored procedure tra MSSQL e MariaDB (conversioni semplici + mapping tipi parametri)
def _translate_procedure_sql(sql: str, src_kind: str, dst_kind: str, proc_name: str) -> str:
    s = sql.strip()
    if src_kind == dst_kind:
        return s
    if src_kind == "MSSQL" and dst_kind == "MariaDB":
        s = re.sub(r"^\s*SET\s+ANSI_NULLS\s+ON\s*;?\s*", "", s, flags=re.IGNORECASE | re.MULTILINE)
        s = re.sub(r"^\s*SET\s+QUOTED_IDENTIFIER\s+ON\s*;?\s*", "", s, flags=re.IGNORECASE | re.MULTILINE)
        s = s.replace("GETDATE()", "NOW()")
        s = s.replace("ISNULL(", "IFNULL(")
        m = re.search(
            r"\bCREATE\s+(?:PROC|PROCEDURE)\b\s+([\s\S]*?)\bAS\b\s*([\s\S]*)\Z",
            s,
            flags=re.IGNORECASE,
        )
        if not m:
            body = s
            params = ""
        else:
            header = m.group(1).strip()
            body = m.group(2).strip()
            header = re.sub(r"^\s*(?:\[[^\]]+\]\.)?\[[^\]]+\]\s*", "", header)
            params = header.strip()
        params_sql = ""
        if params:
            raw = params
            raw = raw.replace("\n", " ").replace("\r", " ")
            parts = [p.strip() for p in raw.split(",") if p.strip()]
            out_params: list[str] = []
            for p in parts:
                p = re.sub(r"\bOUTPUT\b", "", p, flags=re.IGNORECASE).strip()
                p = re.sub(r"=\s*[^ ]+", "", p).strip()
                mm = re.match(r"@(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s+(?P<type>[A-Za-z0-9_]+)(?P<rest>[\s\S]*)\Z", p)
                if not mm:
                    continue
                name = mm.group("name")
                typ = mm.group("type").lower()
                if typ in {"nvarchar", "varchar"}:
                    len_m = re.search(r"\(\s*(max|\d+)\s*\)", p, flags=re.IGNORECASE)
                    if len_m and str(len_m.group(1)).lower() == "max":
                        typ_sql = "LONGTEXT"
                    elif len_m:
                        typ_sql = f"VARCHAR({len_m.group(1)})"
                    else:
                        typ_sql = "VARCHAR(255)"
                elif typ in {"int", "bigint", "smallint", "tinyint"}:
                    typ_sql = typ.upper() if typ != "int" else "INT"
                elif typ in {"bit"}:
                    typ_sql = "TINYINT(1)"
                elif typ in {"datetime", "datetime2", "smalldatetime"}:
                    typ_sql = "DATETIME"
                elif typ in {"decimal", "numeric"}:
                    ps = re.search(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)", p)
                    typ_sql = f"DECIMAL({ps.group(1)},{ps.group(2)})" if ps else "DECIMAL(18,0)"
                elif typ in {"uniqueidentifier"}:
                    typ_sql = "CHAR(36)"
                else:
                    typ_sql = "LONGTEXT"
                out_params.append(f"IN `{name}` {typ_sql}")
            params_sql = ", ".join(out_params)

        body = body.strip()
        body = re.sub(r"^\s*BEGIN\s*", "", body, flags=re.IGNORECASE)
        body = re.sub(r"\s*END\s*;?\s*\Z", "", body, flags=re.IGNORECASE)
        body = body.replace("[", "`").replace("]", "`")
        return f"CREATE PROCEDURE `{proc_name.replace('`', '``')}`({params_sql})\nBEGIN\n{body}\nEND"

    if src_kind == "MariaDB" and dst_kind == "MSSQL":
        s = s.replace("NOW()", "GETDATE()")
        s = s.replace("IFNULL(", "ISNULL(")
        s = re.sub(r"\bDEFINER\s*=\s*`[^`]+`\s*@\s*`[^`]+`\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\bSQL\s+SECURITY\s+DEFINER\b", "", s, flags=re.IGNORECASE)

        m = re.search(r"\bPROCEDURE\b\s*`?([A-Za-z0-9_]+)`?\s*\(([\s\S]*?)\)\s*(BEGIN[\s\S]*)\Z", s, flags=re.IGNORECASE)
        if not m:
            params_sql = ""
            body = s.replace("`", "")
        else:
            raw_params = m.group(2).strip()
            body = m.group(3).strip()
            out_params: list[str] = []
            if raw_params:
                parts = [p.strip() for p in raw_params.split(",") if p.strip()]
                for p in parts:
                    p = re.sub(r"^\s*(INOUT|IN|OUT)\s+", "", p, flags=re.IGNORECASE)
                    pm = re.match(r"`?(?P<name>[A-Za-z_][A-Za-z0-9_]*)`?\s+(?P<type>[A-Za-z0-9_]+)(?P<rest>[\s\S]*)\Z", p)
                    if not pm:
                        continue
                    name = pm.group("name")
                    typ = pm.group("type").lower()
                    if typ in {"varchar", "char"}:
                        lm = re.search(r"\(\s*(\d+)\s*\)", p)
                        typ_sql = f"{typ.upper()}({lm.group(1)})" if lm else "VARCHAR(255)"
                    elif typ in {"text", "mediumtext", "longtext"}:
                        typ_sql = "NVARCHAR(MAX)"
                    elif typ in {"int", "integer"}:
                        typ_sql = "INT"
                    elif typ in {"bigint", "smallint", "tinyint"}:
                        typ_sql = typ.upper()
                    elif typ in {"decimal", "numeric"}:
                        ps = re.search(r"\(\s*(\d+)\s*,\s*(\d+)\s*\)", p)
                        typ_sql = f"DECIMAL({ps.group(1)},{ps.group(2)})" if ps else "DECIMAL(18,0)"
                    elif typ in {"datetime", "timestamp"}:
                        typ_sql = "DATETIME2"
                    else:
                        typ_sql = "NVARCHAR(MAX)"
                    out_params.append(f"@{name} {typ_sql}")
            params_sql = (", " + ", ".join(out_params)) if out_params else ""
            body = body.replace("`", "")

        body = re.sub(r"^\s*BEGIN\s*", "", body, flags=re.IGNORECASE)
        body = re.sub(r"\s*END\s*;?\s*\Z", "", body, flags=re.IGNORECASE)
        return f"CREATE PROCEDURE [dbo].[{proc_name.replace(']', ']]')}] {params_sql}\nAS\nBEGIN\n{body}\nEND"

    return s


# Rimuove DEFINER e altre clausole non portabili nelle definizioni MariaDB (utile per migrazioni cross-environment)
def _strip_mariadb_definer(sql: str) -> str:
    s = sql
    s = re.sub(r"\bDEFINER\s*=\s*`[^`]+`\s*@\s*`[^`]+`\s*", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bSQL\s+SECURITY\s+DEFINER\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).replace(" (", "(").strip()
    return s


# Scrive sia su stdout che su file log (se inizializzato)
def _print(msg: str) -> None:
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    sys.stdout.write(line)
    sys.stdout.flush()
    fh = getattr(_print, "_log_fh", None)
    if fh is not None:
        try:
            fh.write(line)
            fh.flush()
        except Exception:
            pass


# Inizializza il file di log in append (crea la cartella logs se manca)
def _init_log_file() -> None:
    if getattr(_print, "_log_fh", None) is not None:
        return
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "tools_DB_2_DB.log"
    fh = open(log_path, "a", encoding="utf-8", buffering=1)
    setattr(_print, "_log_fh", fh)
    _print(f"Log file: {log_path}")


# Chiude il file di log (se aperto)
def _close_log_file() -> None:
    fh = getattr(_print, "_log_fh", None)
    if fh is not None:
        try:
            fh.close()
        finally:
            setattr(_print, "_log_fh", None)


def _is_mariadb_invalid_default_error(e: Exception) -> bool:
    try:
        if getattr(e, "args", None):
            code = e.args[0]
            if code == 1067:
                return True
    except Exception:
        pass
    return "Invalid default value" in str(e)


def main() -> int:
    # Bootstrap logging e parsing argomenti CLI
    _init_log_file()
    parser = argparse.ArgumentParser(description="Copia database/tabelle tra DBMS (MSSQL <-> MariaDB).")
    parser.add_argument("--config", default="config/config.yaml", help="Percorso config YAML")
    parser.add_argument("--skip-table", action="append", default=[], help="Salta una tabella (ripetibile). Esempio: --skip-table AGK_Variables")
    args = parser.parse_args()

    started_at = _dt.datetime.now()
    # Contatori finali (riepilogo)
    stats: dict[str, int] = {
        "tables": 0,
        "tables_ok": 0,
        "tables_skip": 0,
        "rows": 0,
        "indexes_ok": 0,
        "indexes_skip": 0,
        "fk_ok": 0,
        "fk_skip": 0,
        "views_ok": 0,
        "views_skip": 0,
        "procs_ok": 0,
        "procs_skip": 0,
    }

    # Costruzione adapter source/destination in base a dt_type
    src_cfg, dst_cfg = load_config(args.config)
    src = build_adapter(src_cfg)
    dst = build_adapter(dst_cfg)

    # Selezione delle tabelle da copiare (all/list/single) e gestione mapping input->output in modalità singola tabella
    mode, table_view_names = _parse_table_view_in(src_cfg.table_view_in)
    all_mode = mode == "all"
    list_mode = mode == "list"
    multi_mode = all_mode or list_mode
    single_mode = mode == "single"
    single_view_mode = mode == "single_view"

    single_table_in: str | None = None
    single_table_out: str | None = None
    single_table_out_configured: str | None = None
    if single_mode:
        single_table_in = table_view_names[0].split(".")[-1].strip()
        if not single_table_in:
            raise RuntimeError("table_view_in non valido")

        out_raw = dst_cfg.table_view_out
        if out_raw is not None and str(out_raw).strip():
            single_table_out_configured = str(out_raw).strip().split(".")[-1].strip() or None
    skip_tables_raw: list[str] = []
    for x in args.skip_table or []:
        if x is None:
            continue
        s = str(x).strip()
        if not s:
            continue
        s = s.split(".")[-1].strip()
        if s:
            skip_tables_raw.append(s)
    skip_tables = {t.lower() for t in skip_tables_raw}

    # Logging iniziale delle connessioni (senza password)
    _print(f"Sorgente: {src.kind} {src_cfg.host}:{src_cfg.port}/{src_cfg.database}")
    _print(f"Destinazione: {dst.kind} {dst_cfg.host}:{dst_cfg.port}/{dst_cfg.database}")

    # Assicura che il database di destinazione esista prima di connettersi
    _print("Verifica/creazione database di destinazione...")
    dst.create_database_if_missing(dst_cfg.database)

    # Connessioni DB (una per source e una per destination)
    src_conn = src.connect(src_cfg.database)
    dst_conn = dst.connect(dst_cfg.database)
    try:
        if single_view_mode:
            wanted = (table_view_names[0] if table_view_names else "").split(".")[-1].strip()
            if not wanted:
                raise RuntimeError("table_view_in non valido")

            available_views = src.list_views(src_conn)
            views_lower = {v.lower(): v for v in available_views}
            match_view = views_lower.get(wanted.lower())
            if match_view is None:
                raise RuntimeError(f"View non trovata nel source_database: {wanted}")

            view_def = src.get_view_definition(src_conn, match_view)
            if src.kind == dst.kind:
                view_sql = _strip_mariadb_definer(view_def) if dst.kind == "MariaDB" else view_def
            else:
                select_sql = _translate_view_sql(view_def, src.kind, dst.kind)
                if dst.kind == "MariaDB":
                    view_sql = f"CREATE VIEW `{match_view.replace('`', '``')}` AS {select_sql}"
                else:
                    view_sql = f"CREATE VIEW [dbo].[{match_view.replace(']', ']]')}] AS {select_sql}"

            dcur = dst_conn.cursor()
            try:
                if dst.kind == "MariaDB":
                    dcur.execute(f"DROP VIEW IF EXISTS `{match_view.replace('`', '``')}`")
                else:
                    safe = match_view.replace("]", "]]").replace("'", "''")
                    dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'V') IS NOT NULL DROP VIEW [dbo].[{safe}]")
                dst_conn.commit()

                dcur.execute(view_sql)
                dst_conn.commit()
                _print(f"View OK: {match_view}")
                stats["views_ok"] += 1
            finally:
                dcur.close()

            finished_at = _dt.datetime.now()
            _print(
                "Riepilogo: "
                f"tabelle={stats['tables']} ok={stats['tables_ok']} skip={stats['tables_skip']} righe={stats['rows']}; "
                f"indici ok={stats['indexes_ok']} skip={stats['indexes_skip']}; "
                f"fk ok={stats['fk_ok']} skip={stats['fk_skip']}; "
                f"view ok={stats['views_ok']} skip={stats['views_skip']}; "
                f"proc ok={stats['procs_ok']} skip={stats['procs_skip']}; "
                f"durata={(finished_at - started_at)}"
            )
            _print("Completato.")
            return 0

        # Determina elenco tabelle da copiare, con filtri e normalizzazione nome (schema.table -> table)
        available_src_tables = src.list_tables(src_conn)
        available_lower = {t.lower(): t for t in available_src_tables}
        if all_mode:
            src_tables = list(available_src_tables)
        elif list_mode:
            src_tables = []
            for wanted_raw in table_view_names:
                wanted = wanted_raw.split(".")[-1].strip()
                if not wanted:
                    continue
                match = available_lower.get(wanted.lower())
                if match is None:
                    available_views = src.list_views(src_conn)
                    if any(v.lower() == wanted.lower() for v in available_views):
                        raise RuntimeError(
                            f"Nome '{wanted}' trovato tra le view, non tra le tabelle. "
                            f"Per copiare solo la view usa table_view_in: view:{wanted}"
                        )
                    raise RuntimeError(f"Tabella non trovata nel source_database: {wanted}")
                if match not in src_tables:
                    src_tables.append(match)
        else:
            wanted_lower = (single_table_in or "").lower()
            match = available_lower.get(wanted_lower)
            if match is None:
                available_views = src.list_views(src_conn)
                if any(v.lower() == wanted_lower for v in available_views):
                    raise RuntimeError(
                        f"Nome '{single_table_in}' trovato tra le view, non tra le tabelle. "
                        f"Per copiare solo la view usa table_view_in: view:{single_table_in}"
                    )
                raise RuntimeError(f"Tabella non trovata nel source_database: {single_table_in}")
            src_tables = [match]
            single_table_out = single_table_out_configured or match

        if skip_tables:
            src_tables = [t for t in src_tables if t.lower() not in skip_tables]

        if multi_mode:
            src_tables = _order_tables_by_fk_dependencies(src, src_conn, src_tables)

        _print(f"Tabelle da copiare: {len(src_tables)}")
        stats["tables"] = len(src_tables)

        if dst.kind == "MariaDB":
            # Guardrail: se il source ha tabelle con maiuscole, la destination MariaDB deve preservare il case (lower_case_table_names != 1)
            cur = dst_conn.cursor()
            try:
                cur.execute("SELECT @@lower_case_table_names")
                row = cur.fetchone()
                lctn = int(row[0]) if row and row[0] is not None else 0
            finally:
                cur.close()

            if any(t != t.lower() for t in src_tables):
                if lctn == 1:
                    raise RuntimeError(
                        "Requisito NON soddisfatto: devi preservare il case dei nomi tabella, "
                        "ma la destinazione MariaDB ha lower_case_table_names=1 (Windows default), "
                        "che forza i nomi in minuscolo. "
                        "Configura MariaDB con lower_case_table_names=2 (preserva il case nei metadati) "
                        "oppure usa un server con lower_case_table_names=0, poi ricrea il datadir e ripeti la copia."
                    )
                if lctn == 2:
                    _print(
                        "OK: MariaDB destination usa lower_case_table_names=2 (preserva il case nei metadati, confronti case-insensitive)."
                    )

        fk_checks_disabled = False
        if dst.kind == "MariaDB" and multi_mode:
            # Migliora le performance e riduce errori durante la creazione bulk: FK checks off (ripristinati a fine fase tabelle)
            cur = dst_conn.cursor()
            try:
                cur.execute("SET FOREIGN_KEY_CHECKS=0")
                dst_conn.commit()
                fk_checks_disabled = True
                _print("Destinazione MariaDB: FOREIGN_KEY_CHECKS=0 (temporaneo)")
            finally:
                cur.close()

        mssql_pre_dropped = False
        if dst.kind == "MSSQL" and multi_mode:
            # In MSSQL conviene droppare in ordine inverso prima del create per minimizzare conflitti/dipendenze
            _print("Destinazione MSSQL: DROP tabelle esistenti (ordine dipendenze inverso)")
            dcur = dst_conn.cursor()
            try:
                for t in reversed(src_tables):
                    safe = t.replace("]", "]]").replace("'", "''")
                    dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'U') IS NOT NULL DROP TABLE [dbo].[{safe}]")
                dst_conn.commit()
            finally:
                dcur.close()
            mssql_pre_dropped = True

        try:
            # Fase 1: tabelle (schema + dati in multi_mode, solo dati/align in single_mode)
            for table in src_tables:
                if multi_mode:
                    _print(f"Tabella: {table} (schema + dati)")
                else:
                    _print(f"Tabella: {table} (allineamento dati)")

                cols = src.get_table_columns(src_conn, table)
                pk = src.get_table_primary_key(src_conn, table)
                column_names = [c["name"] for c in cols]
                dst_table = table

                if multi_mode:
                    # Modalità completa: ricrea la tabella in destinazione (DROP + CREATE), poi carica i dati
                    create_sql = dst.render_create_table(dst_table, cols, pk, include_defaults=True)

                    dcur = dst_conn.cursor()
                    try:
                        if dst.kind == "MariaDB":
                            dcur.execute(f"DROP TABLE IF EXISTS `{dst_table.replace('`', '``')}`")
                        elif not mssql_pre_dropped:
                            safe = dst_table.replace("]", "]]").replace("'", "''")
                            dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'U') IS NOT NULL DROP TABLE [dbo].[{safe}]")
                        dst_conn.commit()

                        try:
                            dcur.execute(create_sql)
                        except Exception as e:
                            if dst.kind == "MariaDB" and _is_mariadb_invalid_default_error(e):
                                _print("  Default non valido in CREATE TABLE, riprovo senza DEFAULT...")
                                create_sql_no_defaults = dst.render_create_table(dst_table, cols, pk, include_defaults=False)
                                dcur.execute(create_sql_no_defaults)
                            else:
                                raise
                        dst_conn.commit()
                    finally:
                        dcur.close()
                else:
                    # Modalità allineamento dati: se tabella esiste verifica struttura, decide TRUNCATE/DELETE o ricreazione
                    dst_table = single_table_out or dst_table
                    if dst.table_exists(dst_conn, dst_table):
                        should_recreate = False
                        try:
                            existing_cols = dst.get_table_columns(dst_conn, dst_table)
                            existing_names = [str(c.get("name") or "").lower() for c in existing_cols]
                            expected_names = [n.lower() for n in column_names]
                            if existing_names != expected_names:
                                should_recreate = True
                        except Exception as e:
                            _print(f"  Verifica struttura fallita ({type(e).__name__}: {e}), ricreo tabella...")
                            should_recreate = True

                        if should_recreate:
                            _print("  Struttura diversa: DROP + CREATE TABLE")
                            dcur = dst_conn.cursor()
                            try:
                                if dst.kind == "MariaDB":
                                    dcur.execute(f"DROP TABLE IF EXISTS `{dst_table.replace('`', '``')}`")
                                else:
                                    safe = dst_table.replace("]", "]]").replace("'", "''")
                                    dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'U') IS NOT NULL DROP TABLE [dbo].[{safe}]")
                                dst_conn.commit()

                                create_sql = dst.render_create_table(dst_table, cols, pk, include_defaults=True)
                                try:
                                    dcur.execute(create_sql)
                                except Exception as e:
                                    if dst.kind == "MariaDB" and _is_mariadb_invalid_default_error(e):
                                        _print("  Default non valido in CREATE TABLE, riprovo senza DEFAULT...")
                                        create_sql_no_defaults = dst.render_create_table(dst_table, cols, pk, include_defaults=False)
                                        dcur.execute(create_sql_no_defaults)
                                    else:
                                        raise
                                dst_conn.commit()
                            finally:
                                dcur.close()
                        else:
                            try:
                                _print("  Destinazione: TRUNCATE")
                                dst.truncate_table(dst_conn, dst_table)
                                dst_conn.commit()
                            except Exception as e:
                                _print(f"  TRUNCATE fallito ({type(e).__name__}: {e}), provo DELETE...")
                                dst.delete_all_rows(dst_conn, dst_table)
                                dst_conn.commit()
                    else:
                        _print("  Destinazione: CREATE TABLE")
                        create_sql = dst.render_create_table(dst_table, cols, pk, include_defaults=True)
                        dcur = dst_conn.cursor()
                        try:
                            try:
                                dcur.execute(create_sql)
                            except Exception as e:
                                if dst.kind == "MariaDB" and _is_mariadb_invalid_default_error(e):
                                    _print("  Default non valido in CREATE TABLE, riprovo senza DEFAULT...")
                                    create_sql_no_defaults = dst.render_create_table(dst_table, cols, pk, include_defaults=False)
                                    dcur.execute(create_sql_no_defaults)
                                else:
                                    raise
                            dst_conn.commit()
                        finally:
                            dcur.close()

                try:
                    inserted = dst.insert_rows(dst_conn, dst_table, column_names, _iter_source_rows(src, src_conn, table, column_names))
                    dst_conn.commit()
                    _print(f"  Righe caricate: {inserted}")
                    stats["tables_ok"] += 1
                    stats["rows"] += int(inserted)
                except Exception as e:
                    stats["tables_skip"] += 1
                    _print(f"  Tabella SKIP: {table} ({type(e).__name__}: {e})")
                    dst_conn.rollback()
        finally:
            if fk_checks_disabled:
                # Ripristina FK checks (MariaDB)
                cur = dst_conn.cursor()
                try:
                    cur.execute("SET FOREIGN_KEY_CHECKS=1")
                    dst_conn.commit()
                    _print("Destinazione MariaDB: FOREIGN_KEY_CHECKS=1 (ripristinato)")
                finally:
                    cur.close()

        if multi_mode:
            # Fase 2: ricrea indici e foreign key dopo il caricamento dati (riduce tempi e problemi di dipendenze)
            _print("Indici: copia definizioni")
            for table in src_tables:
                for idx in src.list_indexes(src_conn, table):
                    try:
                        dcur = dst_conn.cursor()
                        try:
                            dcur.execute(dst.render_create_index(idx))
                            dst_conn.commit()
                            _print(f"  Index OK: {table}.{idx.get('name')}")
                            stats["indexes_ok"] += 1
                        finally:
                            dcur.close()
                    except Exception as e:
                        _print(f"  Index SKIP: {table}.{idx.get('name')} ({type(e).__name__}: {e})")
                        stats["indexes_skip"] += 1

            _print("Foreign key: copia definizioni")
            used_fk_names: set[str] = set()
            for table in src_tables:
                for fk in src.list_foreign_keys(src_conn, table):
                    try:
                        parent = str(fk.get("parent_table") or "")
                        if parent and not dst.table_exists(dst_conn, parent):
                            raise RuntimeError(f"Tabella referenziata non presente in destinazione: {parent}")

                        name = str(fk.get("name") or "")
                        if dst.kind == "MSSQL":
                            norm = name.lower()
                            if norm in used_fk_names:
                                child = str(fk.get("child_table") or "T")
                                name = f"FK_{child}_{name}"
                                if len(name) > 120:
                                    name = name[:120]
                                fk = dict(fk)
                                fk["name"] = name
                            used_fk_names.add(name.lower())

                        dcur = dst_conn.cursor()
                        try:
                            dcur.execute(dst.render_add_foreign_key(fk))
                            dst_conn.commit()
                            _print(f"  FK OK: {fk.get('child_table')}.{fk.get('name')}")
                            stats["fk_ok"] += 1
                        finally:
                            dcur.close()
                    except Exception as e:
                        _print(f"  FK SKIP: {fk.get('child_table')}.{fk.get('name')} ({type(e).__name__}: {e})")
                        stats["fk_skip"] += 1

        if all_mode:
            # Fase 3: replica view e stored procedure (solo quando si sta clonando l'intero database)
            _print("View: copia definizioni")
            for view in src.list_views(src_conn):
                try:
                    view_def = src.get_view_definition(src_conn, view)
                    if src.kind == dst.kind:
                        view_sql = _strip_mariadb_definer(view_def) if dst.kind == "MariaDB" else view_def
                    else:
                        select_sql = _translate_view_sql(view_def, src.kind, dst.kind)
                        if dst.kind == "MariaDB":
                            view_sql = f"CREATE VIEW `{view.replace('`', '``')}` AS {select_sql}"
                        else:
                            view_sql = f"CREATE VIEW [dbo].[{view.replace(']', ']]')}] AS {select_sql}"

                    dcur = dst_conn.cursor()
                    try:
                        if dst.kind == "MariaDB":
                            dcur.execute(f"DROP VIEW IF EXISTS `{view.replace('`', '``')}`")
                        else:
                            safe = view.replace("]", "]]").replace("'", "''")
                            dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'V') IS NOT NULL DROP VIEW [dbo].[{safe}]")
                        dst_conn.commit()

                        dcur.execute(view_sql)
                        dst_conn.commit()
                        _print(f"  View OK: {view}")
                        stats["views_ok"] += 1
                    finally:
                        dcur.close()
                except Exception as e:
                    _print(f"  View SKIP: {view} ({type(e).__name__}: {e})")
                    stats["views_skip"] += 1

            _print("Stored procedure: copia definizioni")
            for proc in src.list_procedures(src_conn):
                try:
                    proc_def = src.get_procedure_definition(src_conn, proc)
                    proc_sql = _translate_procedure_sql(proc_def, src.kind, dst.kind, proc)

                    dcur = dst_conn.cursor()
                    try:
                        if dst.kind == "MariaDB":
                            dcur.execute(f"DROP PROCEDURE IF EXISTS `{proc.replace('`', '``')}`")
                            dst_conn.commit()
                            dcur.execute(proc_sql)
                        else:
                            safe = proc.replace("]", "]]").replace("'", "''")
                            dcur.execute(f"IF OBJECT_ID(N'[dbo].[{safe}]', 'P') IS NOT NULL DROP PROCEDURE [dbo].[{safe}]")
                            dst_conn.commit()
                            dcur.execute(proc_sql)
                        dst_conn.commit()
                        _print(f"  Proc OK: {proc}")
                        stats["procs_ok"] += 1
                    finally:
                        dcur.close()
                except Exception as e:
                    _print(f"  Proc SKIP: {proc} ({type(e).__name__}: {e})")
                    stats["procs_skip"] += 1

        finished_at = _dt.datetime.now()
        # Riepilogo finale
        _print(
            "Riepilogo: "
            f"tabelle={stats['tables']} ok={stats['tables_ok']} skip={stats['tables_skip']} righe={stats['rows']}; "
            f"indici ok={stats['indexes_ok']} skip={stats['indexes_skip']}; "
            f"fk ok={stats['fk_ok']} skip={stats['fk_skip']}; "
            f"view ok={stats['views_ok']} skip={stats['views_skip']}; "
            f"proc ok={stats['procs_ok']} skip={stats['procs_skip']}; "
            f"durata={(finished_at - started_at)}"
        )
        _print("Completato.")
        return 0
    finally:
        # Cleanup connessioni e log
        try:
            src_conn.close()
        finally:
            dst_conn.close()
        _close_log_file()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        # In caso di errore non gestito, logga traceback sia su console che su file
        _init_log_file()
        _print("ERRORE NON GESTITO:")
        _print(traceback.format_exc().rstrip())
        _close_log_file()
        raise
