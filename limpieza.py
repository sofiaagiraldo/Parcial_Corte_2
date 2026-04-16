"""Limpieza y normalización de DataFrames (Fase 2): 1NF–3NF con Pandas."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd


def _split_doble_guion(valor: str) -> tuple[str, str]:
    """Separa 'Nombre - Categoría' en dos campos (1NF)."""
    if pd.isna(valor) or not str(valor).strip():
        return "Sin Nombre", "Sin Categoria"
    texto = str(valor).strip()
    if " - " in texto:
        izq, der = texto.split(" - ", 1)
        return izq.strip(), der.strip()
    return texto, "Sin Categoria"


def _limpiar_precio(serie: pd.Series) -> pd.Series:
    """Quita $, convierte a float."""
    def uno(x):
        if pd.isna(x):
            return 0.0
        s = str(x).replace("$", "").replace(",", "").strip()
        try:
            return float(s)
        except ValueError:
            return 0.0

    return serie.map(uno)


def _texto_titulo(serie: pd.Series) -> pd.Series:
    return serie.astype(str).str.strip().str.title()


class LimpiezaProductos:
    """Normaliza productos: nombre/categoría separados, precios numéricos, textos en título."""

    COLUMNAS_SALIDA = [
        "id_producto",
        "nombre_producto",
        "categoria",
        "precio",
        "stock",
        "fecha_vencimiento",
    ]

    @classmethod
    def desde_csv(cls, ruta: Path | str) -> pd.DataFrame:
        df = pd.read_csv(ruta)
        return cls.limpiar(df)

    @classmethod
    def limpiar(cls, df: pd.DataFrame) -> pd.DataFrame:
        partes = df["producto_categoria"].apply(lambda x: _split_doble_guion(x))
        df = df.assign(
            nombre_producto=partes.apply(lambda t: t[0]),
            categoria=partes.apply(lambda t: t[1]),
        )
        df["nombre_producto"] = _texto_titulo(df["nombre_producto"])
        df["categoria"] = _texto_titulo(df["categoria"])
        df["precio"] = _limpiar_precio(df["precio"])
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
        hoy = date.today().isoformat()
        fv = pd.to_datetime(df["fecha_vencimiento"], errors="coerce")
        df["fecha_vencimiento"] = fv.dt.strftime("%Y-%m-%d")
        df["fecha_vencimiento"] = df["fecha_vencimiento"].fillna(hoy)
        df = df.drop(columns=["producto_categoria"])
        return df[cls.COLUMNAS_SALIDA]


class LimpiezaClientes:
    """Separa nombre y tipo; elimina edad (redundante respecto a fecha_nacimiento)."""

    COLUMNAS_SALIDA = [
        "id_cliente",
        "nombre",
        "tipo",
        "email",
        "telefono",
        "fecha_nacimiento",
    ]

    @classmethod
    def desde_csv(cls, ruta: Path | str) -> pd.DataFrame:
        df = pd.read_csv(ruta)
        return cls.limpiar(df)

    @classmethod
    def limpiar(cls, df: pd.DataFrame) -> pd.DataFrame:
        partes = df["cliente_tipo"].apply(lambda x: _split_doble_guion(x))
        df = df.assign(
            nombre=partes.apply(lambda t: t[0]),
            tipo=partes.apply(lambda t: t[1]),
        )
        df["nombre"] = _texto_titulo(df["nombre"])
        df["tipo"] = _texto_titulo(df["tipo"])
        df["email"] = df["email"].fillna("").astype(str).str.strip()
        df.loc[df["email"] == "", "email"] = "No Registra"
        df["telefono"] = df["telefono"].fillna("No Registra").astype(str).str.strip()
        df.loc[df["telefono"] == "", "telefono"] = "No Registra"
        df.loc[df["telefono"].str.lower() == "nan", "telefono"] = "No Registra"
        hoy = date.today().isoformat()
        df["fecha_nacimiento"] = pd.to_datetime(
            df["fecha_nacimiento"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df["fecha_nacimiento"] = df["fecha_nacimiento"].fillna(hoy)
        df = df.drop(columns=["cliente_tipo", "edad"])
        return df[cls.COLUMNAS_SALIDA]


class LimpiezaProveedores:
    """Separa empresa y ciudad; id_proveedor = NIT para FK en ventas."""

    COLUMNAS_SALIDA = [
        "id_proveedor",
        "empresa",
        "ciudad",
        "contacto",
        "telefono",
        "email",
    ]

    @classmethod
    def desde_csv(cls, ruta: Path | str) -> pd.DataFrame:
        df = pd.read_csv(ruta)
        return cls.limpiar(df)

    @classmethod
    def limpiar(cls, df: pd.DataFrame) -> pd.DataFrame:
        df = df.rename(columns={"nit_proveedor": "id_proveedor"})
        partes = df["empresa_ciudad"].apply(lambda x: _split_doble_guion(x))
        df = df.assign(
            empresa=partes.apply(lambda t: t[0]),
            ciudad=partes.apply(lambda t: t[1]),
        )
        df["empresa"] = _texto_titulo(df["empresa"])
        df["ciudad"] = _texto_titulo(df["ciudad"])
        df["contacto"] = df["contacto"].fillna("No Registra").astype(str).str.strip()
        df.loc[df["contacto"] == "", "contacto"] = "No Registra"
        df.loc[df["contacto"].str.lower() == "nan", "contacto"] = "No Registra"
        df["telefono"] = df["telefono"].fillna("No Registra").astype(str).str.strip()
        df.loc[df["telefono"] == "", "telefono"] = "No Registra"
        df.loc[df["telefono"].str.lower() == "nan", "telefono"] = "No Registra"
        df["email"] = df["email"].fillna("").astype(str).str.strip()
        df.loc[df["email"] == "", "email"] = "No Registra"
        df = df.drop(columns=["empresa_ciudad"])
        df["id_proveedor"] = df["id_proveedor"].astype(int)
        return df[cls.COLUMNAS_SALIDA]


class OrquestadorLimpieza:
    """Coordina carga, limpieza y exportación de los tres CSV limpios."""

    def __init__(self, directorio: Path | str) -> None:
        self.directorio = Path(directorio)

    def ejecutar(self) -> dict[str, pd.DataFrame]:
        p_productos = self.directorio / "parcial_2_productos_sucios.csv"
        p_clientes = self.directorio / "parcial_2_clientes_sucios.csv"
        p_proveedores = self.directorio / "parcial_2_proveedores_sucios.csv"

        dfs = {
            "productos": LimpiezaProductos.desde_csv(p_productos),
            "clientes": LimpiezaClientes.desde_csv(p_clientes),
            "proveedores": LimpiezaProveedores.desde_csv(p_proveedores),
        }
        dfs["productos"].to_csv(
            self.directorio / "parcial_2_productos_limpios.csv", index=False
        )
        dfs["clientes"].to_csv(
            self.directorio / "parcial_2_clientes_limpios.csv", index=False
        )
        dfs["proveedores"].to_csv(
            self.directorio / "parcial_2_proveedores_limpios.csv", index=False
        )
        return dfs
