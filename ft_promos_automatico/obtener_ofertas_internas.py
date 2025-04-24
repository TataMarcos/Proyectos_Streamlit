import sys
import numpy as np
import pandas as pd
import snowflake.connector
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os
import re
import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from dateutil.relativedelta import relativedelta
from config import *
import streamlit as st
pd.options.mode.chained_assignment = None

# mangold-cuenta-servicio@projecto-promos.iam.gserviceaccount.com

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def descargar_ofertas_internas(cursor):

    st.write('')
    st.write('Inicia descargar_ofertas_internas')

    #url = 'https://docs.google.com/spreadsheets/d/1w7rYD5JrFwl-ZgtfkslL-6NJtiAUyIzRT2g7v6zFoWY/edit?gid=0#gid=0'
    url = urls['drive_ofertas_internas']

    ofertas_internas_google_drive = gc.open_by_url(url)

    """#### Sheet 1"""

    worksheet_1 = ofertas_internas_google_drive.get_worksheet(0)
    data = worksheet_1.get_all_values()

    ofertas_internas = pd.DataFrame(data[5:], columns=data[4])

    ofertas_internas.rename(
        {
        'Precio de Oferta':'PROM_PVP_OFERTA',
        'Formato de locales al que aplica':'GRUPO',
        'Local/les puntual ':'LOCALES',
        'Locales a excluir de los formatos':'EXCLUIR',
        'Desde':'PROM_FECHA_INICIO',
        'Hasta':'PROM_FECHA_FIN',
        'Área y responsable que comunica':'RESPONSABLE'
         },
        axis = 1,
        inplace = True)

    """#### Filtros y fechas"""

    ofertas_internas = ofertas_internas[ofertas_internas['PROM_FECHA_INICIO'] != '']
    ofertas_internas = ofertas_internas[ofertas_internas['PROM_FECHA_FIN'] != '']

    ofertas_internas['PROM_FECHA_INICIO'] = pd.to_datetime(ofertas_internas['PROM_FECHA_INICIO'], dayfirst=True)

    ofertas_internas['PROM_FECHA_FIN'] = pd.to_datetime(ofertas_internas['PROM_FECHA_FIN'], dayfirst = True)

    ofertas_internas = ofertas_internas[[
        'PROM_FECHA_INICIO',
        'PROM_FECHA_FIN',
        'GRUPO',
        'LOCALES',
        'EXCLUIR',
        'Orin / Item',
        'PROM_PVP_OFERTA',
        'RESPONSABLE'
    ]]

    # Excluyo filas con ORIN vacio

    ofertas_internas = ofertas_internas[~ofertas_internas['Orin / Item'].isna()]
    ofertas_internas = ofertas_internas[ofertas_internas['Orin / Item'] != '']

    ofertas_internas['LOCALES'] = ofertas_internas['LOCALES'].str.replace(';', ',')
    ofertas_internas['PROM_PVP_OFERTA'] = ofertas_internas['PROM_PVP_OFERTA'].str.replace('$', '')

    ofertas_internas['GRUPO'] = ofertas_internas['GRUPO'].str.upper()

    """#### Precio"""

    # Reemplazo separador de comas ',' por '.'

    ofertas_internas['PROM_PVP_OFERTA'] = list(map(lambda x: str(x).replace(',', '.'), ofertas_internas['PROM_PVP_OFERTA']))
    ofertas_internas.loc[ofertas_internas['PROM_PVP_OFERTA'].isna(), 'PROM_PVP_OFERTA'] = 0
    ofertas_internas.loc[ofertas_internas['PROM_PVP_OFERTA'] == '', 'PROM_PVP_OFERTA'] = 0

    ofertas_internas['PROM_PVP_OFERTA'] = pd.to_numeric(ofertas_internas['PROM_PVP_OFERTA'], errors='coerce')
    ofertas_internas['PROM_PVP_OFERTA'] = ofertas_internas['PROM_PVP_OFERTA'].round(0).astype('Int64')

    def is_number(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    precios_incorrectos_df = ofertas_internas[~ofertas_internas['PROM_PVP_OFERTA'].apply(is_number)]
    ofertas_internas = ofertas_internas[ofertas_internas['PROM_PVP_OFERTA'].apply(is_number)]

    """#### Sheet 2 - Grupos de Locales"""

    worksheet_2 = ofertas_internas_google_drive.get_worksheet(1) # ojo que hay una sheet oculta
    data = worksheet_2.get_all_values()

    grupos = pd.DataFrame(data, columns=data[0])
    grupos = grupos.iloc[1:, :3]

    """#### Check Grupos"""

    # Hay grupos en Sheet 1 que no se hayan definido en Sheet 2?
    check_grupos = ofertas_internas[~ofertas_internas['GRUPO'].isin(grupos['GRUPO'])]
    check_grupos = check_grupos[~check_grupos['GRUPO'].isna()]
    check_grupos = check_grupos[check_grupos['GRUPO'] != '']

    if not check_grupos.empty:
        st.write('Hay grupos en Sheet 1 que falten definir en Sheet 2')
        sys.exit()

    """### 2. Articulos"""

    # En Orin / Item puedo tener el ORIN o ESTADISTICO, asi que busco ambos

    query = '''
    SELECT
        ORIN,
        ARTC_ARTC_ID
    FROM
        MSTRDB.DWH.LU_ARTC_ARTICULO
    WHERE
        ORIN IN {snow_orines}
    '''

    cursor.execute(query.format(
        snow_orines = tuple(ofertas_internas['Orin / Item'])
    ))

    snow_articulos_1 = cursor.fetch_pandas_all()
    snow_articulos_1.drop_duplicates(inplace = True)
    snow_articulos_1 = snow_articulos_1[snow_articulos_1['ORIN'] != '-1']

    query = '''
    SELECT
        ARTC_ARTC_COD,
        ARTC_ARTC_ID,
        ORIN
    FROM
        MSTRDB.DWH.LU_ARTC_ARTICULO
    WHERE
        ARTC_ARTC_COD IN {snow_orines}
    '''

    cursor.execute(query.format(
        snow_orines = tuple(ofertas_internas['Orin / Item'])
    ))

    snow_articulos_2 = cursor.fetch_pandas_all()
    snow_articulos_2.drop_duplicates(inplace = True)
    snow_articulos_2 = snow_articulos_2[snow_articulos_2['ORIN'] != '-1']

    articulos_df_1 = ofertas_internas.merge(snow_articulos_1, left_on = 'Orin / Item', right_on = 'ORIN', how = 'inner')

    articulos_df_2 = ofertas_internas.merge(snow_articulos_2, left_on = 'Orin / Item', right_on = 'ARTC_ARTC_COD', how = 'inner')
    articulos_df_2.drop(['ARTC_ARTC_COD'], axis = 1, inplace = True)

    ofertas_internas = pd.concat([articulos_df_1, articulos_df_2])
    ofertas_internas.drop(['Orin / Item'], axis = 1, inplace = True)

    if ofertas_internas['ARTC_ARTC_ID'].isna().sum() > 0:
        st.write('Error: articulos')
        sys.exit()

    """### 3. Locales"""

    # Abro la df ofertas_internas, segun si tiene LOCALES o no (aparece vacio '')

    """#### Locales Puntuales"""

    ofertas_internas.loc[ofertas_internas['LOCALES'] == 'N/A', 'LOCALES'] = ''

    """#### Locales Puntuales"""

    ofertas_internas_locales = ofertas_internas[ofertas_internas['LOCALES'] != '']

    ofertas_internas_locales = ofertas_internas_locales[ofertas_internas_locales['LOCALES'] != '']

    ofertas_internas_locales = ofertas_internas_locales[ofertas_internas_locales['LOCALES'] != '1.01103E+35']

    ofertas_internas_locales['LOCALES'] = ofertas_internas_locales['LOCALES'].str.rstrip(',')
    ofertas_internas_locales['LOCAL'] = ofertas_internas_locales['LOCALES'].str.split(',').apply(
        lambda x: list(set([int(i.strip()) for i in x if i.strip()])))

    ofertas_internas_locales = ofertas_internas_locales.explode('LOCAL')

    """#### Grupos"""

    grupos.rename({'Número Local':'LOCAL'}, axis = 1, inplace = True)

    ofertas_internas_grupo = ofertas_internas[ofertas_internas['LOCALES'] == '']
    ofertas_internas_grupo_2 = ofertas_internas_grupo.merge(grupos[['GRUPO', 'LOCAL']], on = 'GRUPO', how = 'inner')

    ofertas_internas_grupo_2['GRUPO'].unique() == ofertas_internas['GRUPO'][ofertas_internas['GRUPO'] != ''].unique()

    """#### Excluir"""

    ofertas_internas_2 = pd.concat([ofertas_internas_grupo_2, ofertas_internas_locales])
    ofertas_internas_2['EXCLUIR'] = ofertas_internas_2['EXCLUIR'].str.replace(' ', '')
    ofertas_internas_2['EXCLUIR'] = ofertas_internas_2['EXCLUIR'].str.replace(';', ',')
    ofertas_internas_2['EXCLUIR'] = ofertas_internas_2['EXCLUIR'].apply(lambda x: set(str(x).split(',')))
    ofertas_internas_2['LOCAL'] = ofertas_internas_2['LOCAL'].astype(str)
    ofertas_internas_2['TO_DROP'] = ofertas_internas_2.apply(lambda row: row['LOCAL'] in row['EXCLUIR'], axis=1)

    ofertas_internas_2[ofertas_internas_2['TO_DROP'] == True]

    ofertas_internas_2 = ofertas_internas_2[ofertas_internas_2['TO_DROP'] != True]

    query = '''
    SELECT
        GEOG_LOCL_COD,
        GEOG_LOCL_ID
    FROM
        MSTRDB.DWH.LU_GEOG_LOCAL
    '''

    cursor.execute(query)
    locales = cursor.fetch_pandas_all()

    ofertas_internas_2 = ofertas_internas_2.merge(locales, left_on = 'LOCAL', right_on = 'GEOG_LOCL_COD', how = 'inner')

    ofertas_internas_2.drop([
        'TO_DROP',
        'EXCLUIR',
        'LOCALES',
        'GRUPO',
        'LOCAL'
        ], axis = 1, inplace = True)

    ofertas_internas_2['PRONOSTICO_VENTA'] = np.nan
    ofertas_internas_2['STOCK_INICIAL_PROMO'] = np.nan
    ofertas_internas_2['PROM_LOCAL_ACTIVO'] = np.nan
    ofertas_internas_2['PROM_ESTIBA'] = np.nan
    ofertas_internas_2['PROM_FECHA_INICIO'] = pd.to_datetime(ofertas_internas_2['PROM_FECHA_INICIO']).dt.strftime('%Y-%m-%d')
    ofertas_internas_2['PROM_FECHA_FIN'] = pd.to_datetime(ofertas_internas_2['PROM_FECHA_FIN']).dt.strftime('%Y-%m-%d')

    """### 4. Una fila por mes"""

    fecha_eventos_dict = {
        2284: '2024-05-01',
        2299: '2024-06-01',
        2300: '2024-07-01',
        2301: '2024-08-01',
        2302: '2024-09-01',
        2303: '2024-10-01',
        2304: '2024-11-01',
        2305: '2024-12-01',
        2451: '2025-01-01',
        2452: '2025-02-01',
        2453: '2025-03-01',
        2454: '2025-04-01',
        2455: '2025-05-01',
        2456: '2025-06-01',
        2457: '2025-07-01',
        2458: '2025-08-01',
        2459: '2025-09-01',
        2460: '2025-10-01',
        2461: '2025-11-01',
        2462: '2025-12-01'
    }
    fecha_eventos_dict_reversed = {v: k for k, v in fecha_eventos_dict.items()}
    fecha_eventos_df = pd.DataFrame(list(fecha_eventos_dict_reversed.items()), columns=['MES', 'EVENTO_ID'])

    tuple(fecha_eventos_df['EVENTO_ID'])

    ofertas_internas_3 = pd.DataFrame()
    rows_to_add = []

    for row in range(ofertas_internas_2.shape[0]):
        start_date = pd.to_datetime(ofertas_internas_2.iloc[row].iloc[0])
        end_date = pd.to_datetime(ofertas_internas_2.iloc[row].iloc[1])

        # DISTINTOS MESES
        if start_date.month != end_date.month:

            # PRIMER MES
            rows_to_add.append([start_date, start_date.replace(day=start_date.days_in_month)] + list(ofertas_internas_2.iloc[row, 2:]))

            # MESES ENTRE MEDIO (completos)
            meses = pd.date_range(
                start = start_date.replace(day=1) + pd.DateOffset(months = 1),
                end = end_date.replace(day=1) - pd.DateOffset(days=1),
                freq='MS'
                )

            for month in meses:
                rows_to_add.append([month, month.replace(day=month.days_in_month)] +list(ofertas_internas_2.iloc[row, 2:]))

            # ULTIMO MES
            rows_to_add.append([end_date.replace(day = 1), end_date] + list(ofertas_internas_2.iloc[row, 2:]))

        # MISMO MES
        else:
            rows_to_add.append([start_date, end_date] + list(ofertas_internas_2.iloc[row, 2:]))

    if rows_to_add:
        ofertas_internas_3 = pd.concat([
                pd.DataFrame(rows_to_add, columns = ofertas_internas_2.columns)
            ],
            ignore_index=True)

    ofertas_internas_3['MES'] = (pd.to_datetime(ofertas_internas_3['PROM_FECHA_INICIO']).dt.to_period('M').dt.to_timestamp()).astype(str)

    ofertas_internas_3 = ofertas_internas_3.merge(fecha_eventos_df, on='MES', how='inner')
    ofertas_internas_3.drop(['MES'], axis=1, inplace=True)

    ofertas_internas_3[(ofertas_internas_3['ORIN'] == '1000053769') & (ofertas_internas_3['GEOG_LOCL_COD'] == '101')]

    # Check - comparo antes y despues de abrir por mes
    ofertas_internas_2[(ofertas_internas_2['ORIN'] == '1000053769') & (ofertas_internas_2['GEOG_LOCL_COD'] == '101')]

    # Check
    if not ofertas_internas_3[pd.to_datetime(ofertas_internas_3['PROM_FECHA_INICIO']).dt.month != pd.to_datetime(ofertas_internas_3['PROM_FECHA_FIN']).dt.month].empty:
        st.write('Error: filas con distintos meses')
        sys.exit()

    """## 5. Check ofertas duplicadas"""

    # buscamos filas con mismo articulo en un mismo local para los mismos dias

    ofertas_internas_4 = ofertas_internas_3.copy(deep = True)

    # Varios ORINES en un mismo mes

    ofertas_internas_4['PERIODO'] = pd.to_datetime(ofertas_internas_4['PROM_FECHA_INICIO']).dt.to_period('M')

    # Caso 1 --> no duplicado (aparece 1 vez en Mayo)
    ofertas_internas_4[(ofertas_internas_4['ORIN'] == '1000025885') & (ofertas_internas_4['GEOG_LOCL_COD'] == '101') & (ofertas_internas_4['PERIODO'] == '2024-05')]


    # Caso 2 --> duplicado (aparece 4 veces en Mayo)
    ofertas_internas_4[(ofertas_internas_4['ORIN'] == '1000056970') & (ofertas_internas_4['GEOG_LOCL_COD'] == '145') & (ofertas_internas_4['PERIODO'] == '2024-05')]

    # Para no trabajar con miles de filas (ofertas_internas_4.shape[0], defino DataFrame duplicados)

    duplicados_group_by = ofertas_internas_4.groupby(['ORIN', 'PERIODO', 'GEOG_LOCL_COD'])['ARTC_ARTC_ID'].count().reset_index()
    duplicados_group_by.rename({'ARTC_ARTC_ID':'APARICIONES'}, axis = 1, inplace = True)
    duplicados_group_by = duplicados_group_by[duplicados_group_by['APARICIONES'] > 1]

    duplicados_group_by['K'] = duplicados_group_by['ORIN'].astype(str) + '/' + duplicados_group_by['GEOG_LOCL_COD'].astype(str) + '/' + duplicados_group_by['PERIODO'].astype(str)

    # Caso 1 --> no aparece en la DataFrame duplicados, lo cual es correcto
    duplicados_group_by[(duplicados_group_by['ORIN'] == '1000025885') & (duplicados_group_by['GEOG_LOCL_COD'] == '101') & (duplicados_group_by['PERIODO'] == '2024-05')]

    # Caso 2 --> aparece en DataFrame duplicados
    duplicados_group_by[(duplicados_group_by['ORIN'] == '1000056970') & (duplicados_group_by['GEOG_LOCL_COD'] == '145') & (duplicados_group_by['PERIODO'] == '2024-05')]

    ofertas_internas_4['K'] = ofertas_internas_4['ORIN'].astype(str) + '/' + ofertas_internas_4['GEOG_LOCL_COD'].astype(str) + '/' + ofertas_internas_4['PERIODO'].astype(str)


    duplicados_1 = ofertas_internas_4[ofertas_internas_4['K'].isin(duplicados_group_by['K'])]


    """### Produccion"""

    # 1. Genero una fila por cada fecha entre INICIO y FIN

    duplicados_2 = pd.DataFrame()

    for i in range(duplicados_1.shape[0]):
    #for i in range(2):

        df_aux = duplicados_1.iloc[[i]]

        dates = pd.date_range(start = pd.to_datetime(df_aux['PROM_FECHA_INICIO'].iloc[0]), end = pd.to_datetime(df_aux['PROM_FECHA_FIN'].iloc[0]), freq = 'D')

        exploded_df = pd.DataFrame({'DATE': dates})

        # # Merge the original DataFrame with the exploded DataFrame
        exploded_df_2 = pd.merge(exploded_df, df_aux, how = 'left', left_on = 'DATE', right_on  ='PROM_FECHA_INICIO')

    #     # Fill NaN values in the merged columns using ffill()
        exploded_df_2.ffill(inplace=True)
    #
        duplicados_2 = pd.concat([duplicados_2, exploded_df_2])

    # 2. Para cada ORIN, LOCAL, PERIODO tomo las DATE duplicadas

    duplicados_3 = pd.DataFrame()

    combinaciones_orin_local_periodo = duplicados_2[['ORIN', 'GEOG_LOCL_COD', 'PERIODO']].drop_duplicates()

    for orin, local, periodo in zip(combinaciones_orin_local_periodo['ORIN'], combinaciones_orin_local_periodo['GEOG_LOCL_COD'], combinaciones_orin_local_periodo['PERIODO']):

        df_aux = duplicados_2[(duplicados_2['ORIN'] == orin) & (duplicados_2['GEOG_LOCL_COD'] == local) & (duplicados_2['PERIODO'] == periodo)]

        # Veo cuantas filas hay en cada fecha
        dates_group_by = df_aux.groupby('DATE')['ORIN'].count().reset_index()

        # Me quedo con las fechas que tienen mas de una fila
        dates_group_by_2 = dates_group_by[dates_group_by['ORIN'] > 1]

        # Me quedo con la dataframe cuyas fechas estan duplicadas
        df_aux_2 = df_aux[df_aux['DATE'].isin(dates_group_by_2['DATE'])]

        duplicados_3 = pd.concat([duplicados_3, df_aux_2])

    # 3. Para cada ORIN, LOCAL, PERIODO tomo las PROM_FECHA_INICIO y PROM_FECHA_FIN duplicadas
    duplicados_4 = duplicados_3.copy(deep = True)
    duplicados_4.drop(['K', 'DATE'], axis = 1, inplace = True)
    duplicados_4 = duplicados_4[['PERIODO', 'PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'ORIN', 'GEOG_LOCL_COD']].drop_duplicates()

    alerta_duplicados = pd.DataFrame()

    ofertas_internas_4['K'] = ofertas_internas_4['PROM_FECHA_INICIO'].astype(str) + '/' +  ofertas_internas_4['PROM_FECHA_FIN'].astype(str) + '/' +  ofertas_internas_4['ORIN'].astype(str) + '/' +  ofertas_internas_4['GEOG_LOCL_COD'].astype(str) + '/' +  ofertas_internas_4['PERIODO'].astype(str)

    duplicados_4['K'] = duplicados_4['PROM_FECHA_INICIO'].astype(str) + '/' +  duplicados_4['PROM_FECHA_FIN'].astype(str) + '/' +  duplicados_4['ORIN'].astype(str) + '/' +  duplicados_4['GEOG_LOCL_COD'].astype(str) + '/' +  duplicados_4['PERIODO'].astype(str)

    alerta_duplicados = ofertas_internas_4[ofertas_internas_4['K'].isin(duplicados_4['K'])]

    """## 6. File Duplicados"""

    alerta_duplicados.loc[alerta_duplicados['RESPONSABLE'] == '', 'RESPONSABLE'] = 'Responsable no indicado'

    """## 7. Cargo FT PROMOS"""

    ofertas_internas_5 = ofertas_internas_4[~ofertas_internas_4['K'].isin(duplicados_4['K'])]

    if not alerta_duplicados.shape[0] + ofertas_internas_5.shape[0] == ofertas_internas_3.shape[0]:
        sys.exit()

    ofertas_internas_5['MES_INICIO'] = pd.to_datetime(ofertas_internas_5['PROM_FECHA_INICIO']).dt.to_period('M')
    ofertas_internas_5['MES_FIN'] = pd.to_datetime(ofertas_internas_5['PROM_FECHA_FIN']).dt.to_period('M')

    if not ofertas_internas_5[['EVENTO_ID', 'MES_INICIO', 'MES_FIN']].drop_duplicates().shape[0] == ofertas_internas_5['EVENTO_ID'].nunique():
        sys.exit()

    ofertas_internas_5.drop(['ORIN', 'RESPONSABLE', 'GEOG_LOCL_COD', 'K', 'PERIODO', 'MES_INICIO', 'MES_FIN'], axis = 1, inplace = True)

    ofertas_internas_5['PROM_FECHA_INICIO'] = ofertas_internas_5['PROM_FECHA_INICIO'].dt.strftime('%Y-%m-%d')
    ofertas_internas_5['PROM_FECHA_FIN'] = ofertas_internas_5['PROM_FECHA_FIN'].dt.strftime('%Y-%m-%d')

    start_of_month = datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    ofertas_internas_5 = ofertas_internas_5[
        pd.to_datetime(ofertas_internas_5['PROM_FECHA_INICIO']) >= start_of_month]

    ofertas_internas_5_excel = ofertas_internas_5.copy(deep = True)

    ofertas_internas_5_excel = ofertas_internas_5_excel[
        pd.to_datetime(ofertas_internas_5_excel['PROM_FECHA_INICIO']) > datetime.today().replace(day = 1) - relativedelta(days = 1)]

    # Function to format dates
    def format_date(date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{date_obj.month}/{date_obj.day}/{date_obj.year}"

    # List of columns to apply the formatting
    date_columns = ['PROM_FECHA_INICIO', 'PROM_FECHA_FIN']

    # Apply the formatting function to each column
    for col in date_columns:
        ofertas_internas_5_excel[col] = ofertas_internas_5_excel[col].apply(format_date)

    query = '''
    SELECT
        GEOG_LOCL_ID,
        GEOG_LOCL_COD
    FROM
        MSTRDB.DWH.LU_GEOG_LOCAL
    '''

    cursor.execute(query)
    local_snow = cursor.fetch_pandas_all()

    ofertas_internas_5_excel = ofertas_internas_5_excel.merge(local_snow, on = 'GEOG_LOCL_ID', how = 'inner')

    query = '''
    SELECT
        ARTC_ARTC_ID,
        ARTC_ARTC_COD
    FROM
        MSTRDB.DWH.LU_ARTC_ARTICULO
    '''

    cursor.execute(query)
    articulo_snow = cursor.fetch_pandas_all()

    ofertas_internas_5_excel = ofertas_internas_5_excel.merge(articulo_snow, on = 'ARTC_ARTC_ID', how = 'inner')

    path = paths['files_para_BI']
    file_name = f"Ofertas Internas - {datetime.today().strftime('%Y-%m-%d')}.xlsx"
    path_file_name = os.path.join(path, file_name)

    ofertas_internas_5_excel[[
        'PROM_FECHA_INICIO',
        'PROM_FECHA_FIN',
        'ARTC_ARTC_COD',
        'EVENTO_ID',
        'PRONOSTICO_VENTA',
        'STOCK_INICIAL_PROMO',
        'GEOG_LOCL_COD',
        'PROM_PVP_OFERTA',
        'PROM_LOCAL_ACTIVO',
        'PROM_ESTIBA'
    #]].to_excel(f"Cargar en Excel BI\\Ofertas Internas - {datetime.today().strftime('%Y-%m-%d')}.xlsx", index = False)
    ]].to_excel(f'{path_file_name}', index=False)

    """## 8. Resumen alertas"""

    alerta_duplicados = alerta_duplicados[alerta_duplicados['PROM_FECHA_FIN'] >= datetime.today().replace(day = 1)]
    alerta_duplicados['RESPONSABLE'] = alerta_duplicados['RESPONSABLE'].str.upper()
    alerta_duplicados.head(2)

    #ofertas_internas_5[ofertas_internas_5['ARTC_ARTC_ID'] == 118165]
    alerta_resumen = alerta_duplicados[['PERIODO', 'ORIN']].drop_duplicates().sort_values(by = ['ORIN', 'PERIODO'])

    """## 9. File Alertas"""

    if not alerta_resumen.empty:
        st.write('Hay duplicados en Ofertas Internas')

        path = paths['ofertas_internas_alertas']
        file_name = f"Ofertas Internas, alerta duplicados, {datetime.today().strftime('%d-%m-%Y')}.xlsx"
        path_file_name = os.path.join(path, file_name)

        with pd.ExcelWriter(f"{path_file_name}", engine='xlsxwriter') as excel_writer:

            alerta_resumen.to_excel(excel_writer, sheet_name = 'Resumen Duplicados', index = False)

            for responsable in alerta_duplicados['RESPONSABLE'].unique():
                df_aux = alerta_duplicados[alerta_duplicados['RESPONSABLE'] == responsable]
                df_aux.drop(
                    ['ARTC_ARTC_ID', 'GEOG_LOCL_ID', 'PRONOSTICO_VENTA', 'STOCK_INICIAL_PROMO', 'PROM_LOCAL_ACTIVO', 'PROM_ESTIBA', 'EVENTO_ID', 'K']
                    , axis=1, inplace=True)

                sheet_name = re.sub(r'[\/:*?[\]\\]', '_', responsable)[:31]
                df_aux.to_excel(excel_writer, sheet_name = sheet_name, index = False)
        st.write('Hay alertas duplicados')

    """## 10. Check Precio"""

    if not precios_incorrectos_df.empty:
        precios_incorrectos_df.to_excel('Ofertas Internas - Precios Incorrectos.xlsx', index = False)
        st.write('Tenemos errores de precio')

    st.write('Termina descargar_ofertas_internas')