"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    #Nombre de las columnas
    column_names = ['cluster', 'cantidad de palabras clave', 'porcentaje de palabras clave', 'principales palabras clave']
    #Anchura de columnas
    column_widths = [5, 7, 19, 100]

    df = pd.read_fwf('clusters_report.txt', widths=column_widths, names=column_names, skiprows=4)
    
    df_2 = df.copy()
    #Creamos una columna con el número de grupo, con ffll y bfill nos movemos para completar los valores faltantes y con astype(int) convertimos a entero
    df_2["grupo"] = df_2["cluster"].ffill().bfill().astype(int)
    #Agrupamos por grupo y nos quedamos con la primera fila de cada grupo, en la columna de claves hacemos join
    df_2 = df_2.groupby("grupo").agg({
        "cluster": "first",
        "cantidad de palabras clave": "first",
        "porcentaje de palabras clave": "first",
        "principales palabras clave": lambda x: " ".join(x)
    }).reset_index(drop=True)
    #Eliminamos espacios en blanco adicionales
    df_2["principales palabras clave"] = df_2["principales palabras clave"].str.split().apply(lambda x: " ".join(x))
    #Eliminamos el caracter "."
    df_2["principales palabras clave"] = df_2["principales palabras clave"].str.replace(".", "", regex=False)
    #Eliminamos el caracter "%" de la columna porcentaje de palabras clave
    df_2["porcentaje de palabras clave"] = df_2["porcentaje de palabras clave"].str.replace("%", "", regex=False)
    #Reemplazamos las , de la columna porcentaje de palabras clave por .
    df_2["porcentaje de palabras clave"] = df_2["porcentaje de palabras clave"].str.replace(",", ".", regex=False)
    #Convertimos la columna porcentaje de palabras clave a float
    df_2["porcentaje de palabras clave"] = df_2["porcentaje de palabras clave"].astype(float)
    #Convertimos la columna cantidad de palabras y cluster a int
    df_2["cantidad de palabras clave"] = df_2["cantidad de palabras clave"].astype(int)
    df_2["cluster"] = df_2["cluster"].astype(int)
    #Reemplamos los espacios en blanco del nombre de las columnas por guiones bajos
    df_2.columns = df_2.columns.str.replace(' ', '_', regex=False)

    return df_2

