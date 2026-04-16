"""Generación de los tres archivos CSV sucios del parcial (Fase 1)."""

from __future__ import annotations

from io import StringIO
from pathlib import Path

import pandas as pd

# Datos fuente según enunciado del parcial
CAFETERIA_PRODUCTOS_SUCIOS = """id_producto,producto_categoria,precio,stock,fecha_vencimiento
1,Café Tostao - Bebida,$5000,50,2026-12-01
2,chocolatina jet - snack,1200,NaN,2027-01-15
3,Empanada - Snack,$3000,20,2026-05-10
4,Jugo Hit - Bebida,2500,NaN,2026-08-22
5,Galletas Festival - Snack,$1500,100,2026-11-30
6,agua cristal - bebida,2000,30,2026-10-10
7,Sandwich de Pavo - ALMUERZO,$7500,15,2026-04-20
8,brownie - Postre,3500,NaN,2026-04-25
9,Té Helado - BEBIDA,$3000,40,2026-07-11
10,Papas Margarita - snack,2200,60,2026-09-05
11,Croissant - Panaderia,$2800,NaN,2026-05-15
12,Avena Alpina - Bebida,2600,25,2026-06-20
13,Yogurt Finesse - BEBIDA,$3200,35,2026-07-30
14,Muffin de Arandanos - postre,4000,NaN,2026-08-10
15,Dedito de Queso - Snack,$2500,45,2026-05-05
16,Ensalada de Frutas - Postre,5500,10,2026-04-18
17,Wrap de Pollo - Almuerzo,$8000,NaN,2026-04-19
18,Gomitas Trululu - SNACK,1000,80,2026-12-20
19,Tinto - Bebida,$1500,100,2026-04-30
20,Aromatica - bebida,2000,NaN,2026-05-01"""

CAFETERIA_CLIENTES_SUCIOS = """id_cliente,cliente_tipo,email,telefono,edad,fecha_nacimiento
101,Diego Zuluaga - Profesor,diego@correo.com,3001112233,40,1985-05-10
102,ana gomez - estudiante,,NaN,22,2004-01-15
103,CARLOS RUIZ - PROFESOR,carlos@correo.com,3109998877,35,1989-08-20
104,maria lopez - estudiante,,3204445566,28,1995-11-30
105,Luis Perez - Externo,luis@correo.com,NaN,50,1976-02-14
106,sofia ramirez - ESTUDIANTE,sofia@correo.com,3112223344,20,2006-03-22
107,JUAN CAMILO - estudiante,juan@correo.com,NaN,24,2002-07-11
108,andres felipe - Externo,,3156667788,30,1996-09-05
109,VALERIA MORALES - profesor,valeria@correo.com,3009991122,45,1981-12-01
110,isabella carranza - ESTUDIANTE,isa@correo.com,NaN,21,2005-04-18
111,matias mondragon - estudiante,,3105556677,23,2003-02-28
112,ARIANNE AMOROCHO - externo,arianne@correo.com,3201112233,27,1999-06-15
113,mariana peña - Estudiante,mariana@correo.com,NaN,22,2004-08-08
114,nicolas torres - PROFESOR,nico@correo.com,3114445566,38,1988-10-10
115,juan leyton - estudiante,,3007778899,25,2001-11-25
116,CRISTIAN BERMUDEZ - externo,cristian@correo.com,NaN,32,1994-01-05
117,paula arciniegas - ESTUDIANTE,paula@correo.com,3152223344,20,2006-09-14
118,samuel lozano - estudiante,,3206667788,24,2002-05-30
119,GABRIELA RUIZ - profesor,gabi@correo.com,NaN,42,1984-07-07
120,lucas blanco - Externo,lucas@correo.com,3103334455,29,1995-03-12"""

CAFETERIA_PROVEEDORES_SUCIOS = """nit_proveedor,empresa_ciudad,contacto,telefono,email
900111,CoopCafe - Bogota,Carlos Perez,3005551122,carlos@coop.com
800222,Insumos Panaderos - Medellin,NaN,3114445566,
700333,Lacteos El Prado - Cali,Ana Rojas,NaN,ana@prado.com
600444,Distribuidora Sabana - Chia,Luis Gomez,3208889900,luis@sabana.com
500555,Salsas y Especias - Bogota,NaN,3157778899,
400666,Empaques de Carton - Barranquilla,Marta Diaz,3001112233,marta@empaques.com
300777,Granos y Cereales - Bucaramanga,NaN,NaN,ventas@granos.com
200888,Bebidas Refrescantes - Bogota,Jorge Silva,3109998877,
100999,Dulces y Postres - Medellin,Elena Castro,3204445566,elena@dulces.com
999000,Frutas Frescas - Cali,NaN,3112223344,frutas@cali.com"""


class GeneradorDatosSucios:
    """Crea los tres CSV sucios en la ruta base del proyecto."""

    def __init__(self, directorio: Path | str) -> None:
        self.directorio = Path(directorio)

    def generar_todos(self) -> dict[str, Path]:
        """Escribe los tres archivos y devuelve rutas por nombre lógico."""
        self.directorio.mkdir(parents=True, exist_ok=True)
        rutas = {
            "productos": self.directorio / "parcial_2_productos_sucios.csv",
            "clientes": self.directorio / "parcial_2_clientes_sucios.csv",
            "proveedores": self.directorio / "parcial_2_proveedores_sucios.csv",
        }
        pd.read_csv(StringIO(CAFETERIA_PRODUCTOS_SUCIOS)).to_csv(
            rutas["productos"], index=False
        )
        pd.read_csv(StringIO(CAFETERIA_CLIENTES_SUCIOS)).to_csv(
            rutas["clientes"], index=False
        )
        pd.read_csv(StringIO(CAFETERIA_PROVEEDORES_SUCIOS)).to_csv(
            rutas["proveedores"], index=False
        )
        return rutas
