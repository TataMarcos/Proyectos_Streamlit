import sys
import pandas as pd
import snowflake.connector
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import re
from dateutil.relativedelta import relativedelta
import streamlit as st
import time
pd.options.mode.chained_assignment = None

def descargar_promos_empiezan_dos_dias(cursor):
    urls = []
    st.write('')
    st.write('Inicia descargar_promos_empiezan_dos_dias')
    # Cargo las promos que empezaron hoy
    # Si es Lunes, cargo las promos que empezaron el Sabado, Domingo o Lunes
    query = '''
    SELECT
        DISTINCT
        EVENTO_ID,
        CASE
            WHEN EVENTO_ID IN
                (2284, 2299, 2300, 2301, 2302, 2303, 2304, 2305)
                THEN 'OFERTA INTERNA'
                
            WHEN EVENTO_ID BETWEEN 2451 AND 2462
                THEN 'OFERTA INTERNA'
                
            WHEN EVENTO_ID IN
                (2168, 2217, 2236, 2252, 2271, 2292, 2293, 2294, 2295, 2296, 2297, 2298)
                THEN 'INDUCIDA'
                
            WHEN EVENTO_ID BETWEEN 2463 AND 2474
                THEN 'INDUCIDA'
                
            WHEN EVENTO_ID BETWEEN 2319 AND 2325
                THEN 'DISCONTINUADO'
                
            WHEN 
                EVENTO_ID BETWEEN 2394 AND 2408
                THEN 'LIQUIDACIONES DICIEMBRE 2024'
                
            WHEN 
                EVENTO_ID IN (2371, 2372, 2373, 2375, 2376, 2379, 2380, 2381, 2382, 2383, 2384, 2385, 2386, 2387, 2388, 2392, 2393)
                THEN 'LIQUIDACIONES DICIEMBRE 2024'
                
            WHEN 
                EVENTO_ID IN (2377, 2378)
                THEN 'ZAFRA ALIMENTOS'
                
            WHEN 
                EVENTO_ID IN (2390, 2391)
                THEN 'LIQUIDACION HALLOWEEN'
                
            WHEN EVENTO_ID BETWEEN 2414 AND 2441
                THEN 'TATA_COM'
            ELSE 'OTRA'
            
            END AS TIPO_OFERTA,
        PROM_FECHA_INICIO AS I
    FROM
        MSTRDB.DWH.FT_PROMOS
    WHERE
        PROM_FECHA_INICIO BETWEEN CURRENT_DATE AND (CURRENT_DATE + 3)
    '''

    cursor.execute(query)
    inicios = cursor.fetch_pandas_all()
    inicios['I'] = pd.to_datetime(inicios['I']).dt.date#.dt.strftime('%Y-%m-%d')
    inicios.sort_values(by = 'I')

    today = datetime.today().date()
    weekday = today.weekday()

    df_aux = pd.DataFrame()

    if weekday in (0, 1, 2, 3):  # Monday to Thursday
        df_aux = inicios[inicios['I'] == (today + timedelta(days=2))]
    elif weekday == 4:  # Friday
        df_aux = inicios.copy(deep=True)
    else:
        pass

    if not df_aux.empty:
        df_aux = df_aux[df_aux['TIPO_OFERTA'] == 'OTRA']

    if df_aux.empty:
        st.write('--> URLs: no hay promos que empiecen hoy')
    else:
        st.write('sumar URLs -->')
        st.write(", ".join(map(str, df_aux['EVENTO_ID'])))
        if 'urls' not in st.session_state:
            st.session_state.urls = []
        st.write('')
        url = st.text_input("Ingresar 1 URL")
        if url != '':
            st.session_state.urls.append(url)
        st.write('Ingreso las siguientes urls:')
        for u in st.session_state.urls:
            st.write(u)
        no_cargar = st.button('No cargar mas')
        if not(no_cargar):
            time.sleep(600)
        else:
            urls = list(set(st.session_state.urls))

    st.write('Termina descargar_promos_empiezan_dos_dias')
    st.write('')
    return urls