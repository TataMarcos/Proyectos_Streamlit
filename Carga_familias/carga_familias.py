import pandas as pd
import tkinter as tk
from tkinter import filedialog
import getpass
import time
import pandas as pd
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import streamlit as st
import keyboard
import json
import psutil

#Función para loguearse
def snowflake_login():
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

        st.write('Correct Password - connected to SNOWFLAKE')
    except:
        st.write('No se ingresaron las credenciales correctas aún')

    return user, cursor, snowflake_connection

keyboard.press_and_release('ctrl+w')        #Close the window

try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login()
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        user = st.session_state.user
        cursor = st.session_state.cursor
        snow = st.session_state.snow  # Reuse the existing Snowflake session
except:
    st.write('Aún no se ingresaron credenciales')

f=datetime.now().strftime('%Y%m%d_%H%M%S')

if 'familias' not in st.session_state:
    familias = st.button("Descargar familias actuales.")

    if familias:
        q = """ select group_name grupo, dept_name depto, class_name clase, sub_name subclase,
                    a.item, b.item_Desc descripcion, familia
                from sandbox_plus.dwh.RELACIONES_ITEM_PARENT_ACTUALIZADOS a
                left join mstrdb.dwh.item_master b on a.item::text=b.item::text and item_number_type='ITEM'
                left join mstrdb.dwh.deps c on c.dept=b.dept
                left join mstrdb.dwh.groups d on d.group_no=c.group_no
                left join mstrdb.dwh.class e on e.clase=b.clase
                left join mstrdb.dwh.subclass f on f.subclase=b.subclase
                where a.item is not null"""
        cursor.execute(q)
        familias_previas = cursor.fetch_pandas_all()

        # Convertir ITEM a entero
        familias_previas["ITEM"] = familias_previas["ITEM"].astype('int64')
        
        st.write('Familias actuales:')
        st.write('')
        st.dataframe(familias_previas)

        st.session_state.familias = familias_previas
else:
    st.write('Familias actuales:')
    st.write('')
    st.dataframe(st.session_state.familias)

if 'upd' not in st.session_state:
    continuar = st.button("Actualizar familias")
    if continuar:
        st.session_state.upd = True
try:
    if st.session_state.upd:
        st.write('')
        st.write("Ahora importá un archivo excel con las siguientes columnas [ITEM, FAMILIA]. Aguarda unos segundos..")
        
        #Cargamos el archivo
        uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

        if uploaded_file is not None:
            df = pd.read_excel(uploaded_file)
            df.columns = df.columns.str.upper()

            try:
                df = df[[ 'ITEM', 'FAMILIA']]
            except KeyError:
                st.write("\n❌ Hay un error con las columnas. Verifica que todas existan en el archivo.")
                st.write("\nPrograma finalizado. Cerrando...")
                # Give a bit of delay for user experience
                time.sleep(5)
                # Terminate streamlit python process
                pid = os.getpid()
                p = psutil.Process(pid)
                p.terminate()
        
            st.dataframe(df.astype('str'))

            df = df.dropna().drop_duplicates()

            st.write("\n✅ Archivo validado correctamente. Puedes continuar.")
            st.write('')
            st.write('El archivo tiene ', len(df), ' combinaciones')

            try:
                df["ITEM"] = df["ITEM"].astype('int64')
            except:
                st.write('La columna item tiene valores no numéricos. Revisar.')
                st.write("Programa finalizado. Cerrando...")
                # Give a bit of delay for user experience
                time.sleep(5)
                # Terminate streamlit python process
                pid = os.getpid()
                p = psutil.Process(pid)
                p.terminate()

            df['TABLA']=f
            df['USUARIO']=user

            df = df[['ITEM','FAMILIA','TABLA','USUARIO']]
            st.write('El archivo a cargar queda asi:')
            st.dataframe(df.astype('str'))

            time.sleep(3)
            try:
                success, nchunks, nrows, _ = write_pandas(snow, df, database='SANDBOX_PLUS', schema='DWH',
                                                          table_name='INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS')
                st.write(f"Éxito: {success}, Chunks: {nchunks}, Filas insertadas: {nrows}")
            except:
                st.write('Falló la carga')
                st.write("Programa finalizado. Cerrando...")
                # Give a bit of delay for user experience
                time.sleep(10)
                # Terminate streamlit python process
                pid = os.getpid()
                p = psutil.Process(pid)
                p.terminate()


            st.write('Ahora voy a actualizar la info en la base de datos')

            qa=f"""MERGE INTO SANDBOX_PLUS.DWH.RELACIONES_ITEM_PARENT_ACTUALIzADOS AS target
            USING (select distinct item,familia from sandbox_plus.dwh.INPUT_RELACIONES_ITEM_PARENT_ACTUALIZADOS where tabla='{f}') AS source
            ON target.item = source.item
            WHEN MATCHED THEN 
                UPDATE SET target.familia = source.familia
            WHEN NOT MATCHED THEN 
                INSERT (item, familia) VALUES (source.item, source.familia)"""

            result = cursor.execute(qa)
            st.write("Se actualizaron ", result.rowcount, " lineas")
except:
    pass

#Armamos bloque para cerrar el programa
exit_app = st.button("Cerrar el programa.")
if exit_app:
    st.write('Cerrando el programa...')
    # Give a bit of delay for user experience
    time.sleep(1)
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()