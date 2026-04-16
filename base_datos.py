"""Migración a SQLite (Fase 3–4): to_sql, tabla ventas con PK/FK y operaciones CRUD."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


def _reemplazar_tabla_con_llave_primaria(
    conn: sqlite3.Connection, nombre: str, ddl_columnas: str
) -> None:
    """Sustituye la tabla creada por `to_sql` por una versión con PRIMARY KEY (requerido para FK)."""
    tmp = f"{nombre}__pk"
    conn.execute(f"DROP TABLE IF EXISTS {tmp}")
    conn.execute(f"CREATE TABLE {tmp} ({ddl_columnas})")
    conn.execute(f"INSERT INTO {tmp} SELECT * FROM {nombre}")
    conn.execute(f"DROP TABLE {nombre}")
    conn.execute(f"ALTER TABLE {tmp} RENAME TO {nombre}")


class MigracionCafeteria:
    """Carga DataFrames limpios con `to_sql`, asegura PKs y crea la tabla de ventas."""

    DDL_PRODUCTOS = """
        id_producto INTEGER PRIMARY KEY,
        nombre_producto TEXT NOT NULL,
        categoria TEXT NOT NULL,
        precio REAL NOT NULL,
        stock INTEGER NOT NULL,
        fecha_vencimiento TEXT NOT NULL
    """

    DDL_CLIENTES = """
        id_cliente INTEGER PRIMARY KEY,
        nombre TEXT NOT NULL,
        tipo TEXT NOT NULL,
        email TEXT NOT NULL,
        telefono TEXT NOT NULL,
        fecha_nacimiento TEXT NOT NULL
    """

    DDL_PROVEEDORES = """
        id_proveedor INTEGER PRIMARY KEY,
        empresa TEXT NOT NULL,
        ciudad TEXT NOT NULL,
        contacto TEXT NOT NULL,
        telefono TEXT NOT NULL,
        email TEXT NOT NULL
    """

    DDL_VENTAS = """
        id_venta INTEGER PRIMARY KEY AUTOINCREMENT,
        id_cliente INTEGER NOT NULL,
        id_producto INTEGER NOT NULL,
        id_proveedor INTEGER NOT NULL,
        cantidad INTEGER NOT NULL,
        total_venta REAL NOT NULL,
        fecha_venta TEXT NOT NULL,
        FOREIGN KEY (id_cliente) REFERENCES clientes(id_cliente),
        FOREIGN KEY (id_producto) REFERENCES productos(id_producto),
        FOREIGN KEY (id_proveedor) REFERENCES proveedores(id_proveedor)
    """

    def __init__(self, ruta_db: Path | str) -> None:
        self.ruta_db = Path(ruta_db)

    def conectar(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.ruta_db)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.commit()
        return conn

    def migrar_dataframes(
        self,
        df_productos: pd.DataFrame,
        df_clientes: pd.DataFrame,
        df_proveedores: pd.DataFrame,
    ) -> None:
        """Fase 3: persiste los tres DataFrames con `to_sql` y normaliza PKs."""
        conn = self.conectar()
        try:
            conn.execute("DROP TABLE IF EXISTS ventas;")
            for t in ("productos", "clientes", "proveedores"):
                conn.execute(f"DROP TABLE IF EXISTS {t};")
            conn.commit()

            df_productos.to_sql("productos", conn, if_exists="replace", index=False)
            df_clientes.to_sql("clientes", conn, if_exists="replace", index=False)
            df_proveedores.to_sql("proveedores", conn, if_exists="replace", index=False)

            _reemplazar_tabla_con_llave_primaria(
                conn, "productos", self.DDL_PRODUCTOS.strip()
            )
            _reemplazar_tabla_con_llave_primaria(
                conn, "clientes", self.DDL_CLIENTES.strip()
            )
            _reemplazar_tabla_con_llave_primaria(
                conn, "proveedores", self.DDL_PROVEEDORES.strip()
            )
            conn.commit()
        finally:
            conn.close()

    def crear_tabla_ventas(self) -> None:
        conn = self.conectar()
        try:
            conn.execute("DROP TABLE IF EXISTS ventas;")
            conn.execute(f"CREATE TABLE ventas ({self.DDL_VENTAS.strip()});")
            conn.commit()
        finally:
            conn.close()


class OperacionesVentas:
    """CRUD sobre `ventas` con SQL puro (sqlite3)."""

    def __init__(self, ruta_db: Path | str) -> None:
        self.ruta_db = Path(ruta_db)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.ruta_db)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def insertar_cinco_ejemplos(self) -> None:
        """Create: cinco ventas con FK válidas."""
        filas = [
            (103, 4, 600444, 3, 7500.0, "2026-04-10"),
            (106, 1, 900111, 2, 10000.0, "2026-04-11"),
            (110, 7, 500555, 1, 7500.0, "2026-04-12"),
            (114, 5, 100999, 4, 6000.0, "2026-04-13"),
            (117, 9, 200888, 2, 6000.0, "2026-04-14"),
        ]
        sql = """
            INSERT INTO ventas (id_cliente, id_producto, id_proveedor, cantidad, total_venta, fecha_venta)
            VALUES (?, ?, ?, ?, ?, ?);
        """
        conn = self._conn()
        try:
            conn.executemany(sql, filas)
            conn.commit()
        finally:
            conn.close()

    def consulta_join(self) -> list[tuple[Any, ...]]:
        """Read: clientes y productos unidos a ventas."""
        sql = """
            SELECT
                c.nombre AS nombre_cliente,
                p.nombre_producto AS nombre_producto,
                v.total_venta
            FROM ventas v
            INNER JOIN clientes c ON v.id_cliente = c.id_cliente
            INNER JOIN productos p ON v.id_producto = p.id_producto;
        """
        conn = self._conn()
        try:
            cur = conn.execute(sql)
            return cur.fetchall()
        finally:
            conn.close()

    def actualizar_venta_uno(self, cantidad: int, total_venta: float) -> None:
        """Update: venta con id_venta = 1."""
        sql = """
            UPDATE ventas
            SET cantidad = ?, total_venta = ?
            WHERE id_venta = 1;
        """
        conn = self._conn()
        try:
            conn.execute(sql, (cantidad, total_venta))
            conn.commit()
        finally:
            conn.close()

    def eliminar_venta_tres(self) -> None:
        """Delete: venta con id_venta = 3."""
        conn = self._conn()
        try:
            conn.execute("DELETE FROM ventas WHERE id_venta = 3;")
            conn.commit()
        finally:
            conn.close()
