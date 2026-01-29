# ============================================
# LAB Connecting Python to SQL - Sakila
# ============================================

import pandas as pd
from sqlalchemy import create_engine, text

# -------------------------------------------------
# 1. CONEXIÓN A MySQL (AJUSTA SOLO LA CONTRASEÑA)
# -------------------------------------------------

USER = "root"
PASSWORD = "loreS231223%"   # <--- CAMBIA ESTO
HOST = "localhost"
PORT = 3307                     # según tu captura de Workbench
DB = "sakila"

connection_url = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"

# Creamos el engine global
engine = create_engine(connection_url)


# -------------------------------------------------
# 2. rentals_month(engine, month, year)
#    Devuelve un DataFrame con TODAS las filas de
#    rental de ese mes y año.
# -------------------------------------------------

def rentals_month(engine, month: int, year: int) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los rentals del mes/año indicados.

    Parámetros
    ----------
    engine : sqlalchemy.Engine
        Conexión a la base de datos Sakila.
    month : int
        Mes (1–12).
    year : int
        Año (por ejemplo 2005).

    Returns
    -------
    pandas.DataFrame
        Todas las columnas de la tabla rental para ese mes/año.
    """

    query = text("""
        SELECT *
        FROM rental
        WHERE MONTH(rental_date) = :month
          AND YEAR(rental_date)  = :year;
    """)

    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, params={"month": month, "year": year})

    return df


# -------------------------------------------------
# 3. rental_count_month(df_rentals, month, year)
#    Cuenta cuántos rentals tiene cada customer_id
#    para ese mes y año.
# -------------------------------------------------

def rental_count_month(df_rentals: pd.DataFrame, month: int, year: int) -> pd.DataFrame:
    """
    A partir del DataFrame de rentals de un mes/año,
    devuelve un DataFrame con el número de alquileres
    por customer_id para ese mes.

    La columna con el conteo se llama:
        rentals_MM_YYYY  (ej: rentals_05_2005)
    """

    col_name = f"rentals_{month:02d}_{year}"

    counts = (
        df_rentals
        .groupby("customer_id")
        .size()
        .reset_index(name=col_name)
    )

    return counts


# -------------------------------------------------
# 4. compare_rentals(df_a, df_b)
#    Junta las dos tablas de conteos y añade la
#    columna "difference" = mes2 - mes1
# -------------------------------------------------

def compare_rentals(df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe dos DataFrames de conteos por cliente
    (salida de rental_count_month) y devuelve uno combinado
    con una columna 'difference' = rentals_mesB - rentals_mesA.
    """

    # Identificamos los nombres de las columnas de conteo
    col_a = [c for c in df_a.columns if c != "customer_id"][0]
    col_b = [c for c in df_b.columns if c != "customer_id"][0]

    merged = df_a.merge(df_b, on="customer_id", how="outer").fillna(0)

    merged["difference"] = merged[col_b] - merged[col_a]

    return merged


# -------------------------------------------------
# 5. EJEMPLO DE USO (puedes dejarlo para probar
#    o comentarlo en el lab final)
# -------------------------------------------------

if __name__ == "__main__":
    # Rentals de mayo 2005
    rentals_may = rentals_month(engine, month=5, year=2005)
    print("Rentals mayo 2005:", rentals_may.shape)

    # Rentals de junio 2005
    rentals_jun = rentals_month(engine, month=6, year=2005)
    print("Rentals junio 2005:", rentals_jun.shape)

    # Conteos por cliente
    may_counts = rental_count_month(rentals_may, 5, 2005)
    jun_counts = rental_count_month(rentals_jun, 6, 2005)

    # Comparación
    comparison = compare_rentals(may_counts, jun_counts)
    print("\nComparación (primeras filas):")
    print(comparison.head())
