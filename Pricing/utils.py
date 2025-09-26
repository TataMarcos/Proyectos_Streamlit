import os
import snowflake.connector
import json
import pandas as pd
import streamlit as st
from snowflake.connector.pandas_tools import write_pandas

#Función para loguearse
def snowflake_login():
    counter = 0
    while True:
        if counter + 1 < 4:
            print(f"Intento {counter + 1}")

            try:
                user = st.text_input("INGRESAR USUARIO: ")
                psw = st.text_input("INGRESAR CONTRASEÑA: ")
                pass_ = st.text_input("INGRESAR PASSCODE: ")
                # user = input("INGRESAR USUARIO: ")
                # psw = input("INGRESAR CONTRASEÑA: ")
                # pass_ = input("INGRESAR PASSCODE: ")

                # Establish Snowflake connection
                snowflake_connection = snowflake.connector.connect(
                    user=user,
                    password=psw,
                    account="XZ23267-dp32414",
                    passcode=pass_,
                    database='SANDBOX_PLUS',
                    schema='DWH'
                )
                cursor = snowflake_connection.cursor()

                print('Correct Password - connected to SNOWFLAKE')
                break

            except FileNotFoundError:
                print("Error: 'credentials.json' file not found.")
                break
            except json.JSONDecodeError:
                print("Error: 'credentials.json' file is not valid JSON.")
                break
            except Exception as e:
                counter += 1
                print(f'Error: {e}')
                print('Incorrect Password - provide again')
        else:
            print('3 Intentos fallidos')
            break

    return user, cursor, snowflake_connection

def descargar_segmento(cursor: snowflake.connector.cursor.SnowflakeCursor,
                       query: str, conds=[]) -> pd.DataFrame:

    # Obtengo json con directorio de queries y su orden
    query_path = 'Queries/' + query + '.sql'

    # Obtengo el texto de mi query
    with open(query_path, 'r', encoding="utf8") as file: command = file.read()

    # Ejecuto query para obtener clientes que superaron la promo
    if len(conds) == 0:
        cursor.execute(command)
    elif len(conds) == 1:
        cond = conds[0]
        cursor.execute(command.format(cond=cond))
    else:
        cond = conds[0]
        cond2 = conds[1]
        cursor.execute(command.format(cond=cond, cond2=cond2))

    # Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cursor.fetch_pandas_all()

    return df

def carga_snow_generic(df, ctx, table, database, schema):

    # Ingresamos la base de datos en Snow
    success = write_pandas(df = df, conn = ctx, table_name= table,
                           database= database, schema = schema)

    if success:
        print('SUCCESS')
        return success

def clean_table(cursor, table, cond=None):

    # SQL para obtener datos
    if cond is None:
        sql = f"DELETE FROM SANDBOX_PLUS.DWH.{table};"
    else:
        sql = f"DELETE FROM SANDBOX_PLUS.DWH.{table} WHERE {cond};"

    # Ejecuto select de Snow
    cursor.execute(sql)

    return True