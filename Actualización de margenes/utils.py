import json
import snowflake.connector
import pandas as pd
import numpy as np
import os
from snowflake.connector.pandas_tools import write_pandas
import streamlit as st

###########################################################################################################
########################################### AUXILIARY FUNCTIONS ###########################################
###########################################################################################################

def get_credentials(type: str) -> dict:
    print(os.getcwd())
    f = open('credentials.json',)
    credentials = json.load(f)[type]

    return credentials

def get_credentials_correo(type: str) -> dict:
    print(os.getcwd())
    f = open("C:\\Users\\arturo.botata12\\Documents\GitHub\\credentials_correo.json",)
    credentials = json.load(f)[type]

    return credentials


def snowflake_login():

    if os.getcwd().upper() == 'C:\\USERS\\ARTURO.BOTATA12\\DOCUMENTS\\GITHUB\\PROYECTOS_STREAMLIT\\CABECERAS':

        user = "PLUS_VM1_NEW"

        snowflake_connection = snowflake.connector.connect(
            user=user,
            password="aK09fWyh4i5oVcI9A31Ea4vXMcquhMMlIE9sXRoil3oSw9faD9",
            account="XZ23267-dp32414",
            database="SANDBOX_PLUS",
            schema="DWH"
        )
        cursor = snowflake_connection.cursor()
    else:
        counter = 0
        while True:
            if counter + 1 < 4:
                print(f"Intento {counter + 1}")

                try:
                    user = st.text_input("INGRESAR USUARIO: ")
                    psw = st.text_input("INGRESAR CONTRASEÑA: ")
                    pass_ = st.text_input("INGRESAR PASSCODE: ")

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


def registrar_log(status_code: int, mensaje: str, archivo: str):
    """
        Esta funcion auxiliar que creamos, es a modo de tener un log ante posibles errores
        del código de envio, vamos a recibir como parametros un codigo y un mensaje para guardar
        en un diccionario y devolverlo para ir grabando una lista.
    """
    with open(archivo, 'a') as file:
        error = f'{status_code} - {mensaje}\n'
        file.write(error)


def carga_snow_generic(df, ctx, table, database, schema):

    # Ingresamos la base de datos en Snow
    success = write_pandas(df = df,
                            conn = ctx,
                            table_name= table,
                            database= database,
                            schema = schema)

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


def descargar_segmento(cursor: snowflake.connector.cursor.SnowflakeCursor,
                       query: str, cond = None) -> pd.DataFrame:

    # Obtengo json con directorio de queries y su orden
    query_path = query + '.sql'

    # Obtengo el texto de mi query
    with open(query_path, 'r', encoding="utf8") as file: command = file.read()

    # Ejecuto query para obtener clientes que superaron la promo
    if not(cond):
        cursor.execute(command)
        #command = command
    else:
        cursor.execute(command.replace(';', cond))
        #command = command.replace(';', cond)

    # Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cursor.fetch_pandas_all()

    return df