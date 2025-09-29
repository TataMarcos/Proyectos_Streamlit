import pandas as pd
import os
import snowflake.connector
import json
import streamlit as st
import time
import psutil
import keyboard
from datetime import datetime, timedelta

#Función para loguearse
def snowflake_login():
    if os.getcwd().upper() == 'C:\\USERS\\ARTURO.BOTATA12\\DOCUMENTS\\GITHUB\\PROYECTOS_STREAMLIT\\PRECIOS_DIARIOS':

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

def descargar_segmento(cursor: snowflake.connector.cursor.SnowflakeCursor,
                       query: str, conds:list) -> pd.DataFrame:

    # Obtengo json con directorio de queries y su orden
    query_path = query + '.sql'

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

keyboard.press_and_release('ctrl+w')        #Close the window

if 'snow' not in st.session_state:
    user, cursor, snow = snowflake_login()
    st.session_state.user = user
    st.session_state.cursor = cursor
    st.session_state.snow = snow
else:
    snow = st.session_state.snow  # Reuse the existing Snowflake session
    user = st.session_state.user
    cursor = st.session_state.cursor

if 'snow' not in st.session_state:
    st.stop()

#Aramamos listado de fechas
fechas = []
for i in range((datetime.today().date() - datetime.today().replace(year=datetime.today().date().year - 1).date()).days):
    fechas.append("'" + (datetime.today().replace(year=datetime.today().date().year - 1).date()
                         + timedelta(days=i)).strftime('%Y-%m-%d') + "'")
fechas.reverse()

#Seleccionamos la fecha
fecha = st.selectbox('Seleccione fecha:', options=fechas)
st.write(fecha)

prog = st.selectbox('Seleccione el programa: ', ['ACTIVOS', 'I+D'])

if prog == 'ACTIVOS':
    #Cargamos el archivo
    st.write('Arrastrá el archivo excel con las siguientes columnas [LOCAL,ITEM]')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    #Lo leemos
    if uploaded_file is None:
        st.stop()
    if uploaded_file is not None:
        price = pd.read_excel(uploaded_file)
        price.columns = price.columns.str.upper()

    #Hacemos listado de locales y orines
    try:
        loc = '('
        for l in price['LOCAL'].unique():
            loc += str(l) + ','
        loc = loc.strip(',')
        loc += ')'

        items = "('"
        for i in price['ITEM'].unique():
            items += str(i) + "','"
        items = items.strip(",'")
        items += "')"

        #Descargamos de snow
        c = ' AND LGL.GEOG_LOCL_COD IN ' + loc + " AND LAA.ORIN IN " + items + ';'
        conds = []
        conds.append(c)
        conds.append(fecha)
        df = descargar_segmento(cursor=cursor, query='PRECIOS', conds=conds).astype({'ORIN':str, 'LOCAL':str})

        price['ORIN'] = price['ITEM'].apply(str)
        price['LOCAL'] = price['LOCAL'].apply(str)

        df_final = price[['ORIN', 'LOCAL']].merge(df, how='left')

        #Mostramos el dataframe final
        st.dataframe(df_final)
    except:
        st.write('Se cargó un archivo con un formato erróneo')

if prog == 'I+D':
    #Cargamos el archivo
    st.write('Arrastrá el archivo excel con las siguientes columnas [ITEM]')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    #Lo leemos
    if uploaded_file is None:
        st.stop()
    if uploaded_file is not None:
        price = pd.read_excel(uploaded_file)
        price.columns = price.columns.str.upper()

    #Hacemos listado de locales y orines
    try:
        items = "('"
        for i in price['ITEM'].unique():
            items += str(i) + "','"
        items = items.strip(",'")
        items += "')"

        #Descargamos de snow
        c = "AND LAA.ORIN IN " + items
        conds = []
        conds.append(c)
        conds.append(fecha)
        df = descargar_segmento(cursor=cursor, query='PRECIOS - I+D', conds=conds).astype({'ORIN':str})

        #Mostramos el dataframe final
        st.dataframe(df)
    except:
        st.write('Se cargó un archivo con un formato erróneo')

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