import sys
import polars as pl
import numpy as np
import pandas as pd
import snowflake.connector
import json
from datetime import datetime, timedelta
import os
import re
from snowflake.connector.pandas_tools import write_pandas
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import MonthEnd
from config import *
import warnings
import streamlit as st

warnings.filterwarnings("ignore", message="Parsing dates in %d/%m/%Y format")

# Cuenta de Servicio Leo

# mangold-cuenta-servicio@projecto-promos.iam.gserviceaccount.com

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

def descargar_inducidas(cursor):

    st.write('')
    st.write('Inicia descargar_inducidas')

    #url = 'https://docs.google.com/spreadsheets/d/1sZet9eBzOmokS1wBMw3lT46xJQQIWdO-fMk50toKV7A/edit#gid=775311320'
    url = urls['drive_inducidas']
    inducidas_google_drive = gc.open_by_url(url)

    worksheet_1 = inducidas_google_drive.worksheet('Inducidas 2025 en adelante')

    data = worksheet_1.get_all_values()

    inducidas = pd.DataFrame(data[1:], columns=data[0])
    inducidas.columns = [column.split('\n')[0] for column in inducidas.columns]

    # AÑO / MES / /Item / PRECIO DE VENTA

    inducidas = inducidas[inducidas['Item'] != '']
    inducidas = inducidas[inducidas['Item'] != '-']
    inducidas['Item'] = list(map(lambda x: str(x).replace(' ', ''), inducidas['Item']))
    inducidas['Item'] = inducidas['Item'].map(lambda x: str(x).strip())

    month_dict = {
        'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
        'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
        'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
    }

    inducidas['PROM_FECHA_INICIO'] = inducidas.apply(
        lambda row: pd.Timestamp(f'{row["AÑO"]}-{month_dict[row["MES"]]}-01'), axis=1)

    inducidas['PROM_FECHA_FIN'] = inducidas['PROM_FECHA_INICIO'] + MonthEnd(0)

    fecha_eventos_dict = {
        2168: '2024-01-01',
        2217: '2024-02-01',
        2236: '2024-03-01',
        2252: '2024-04-01',
        2271: '2024-05-01',
        2292: '2024-06-01',
        2293: '2024-07-01',
        2294: '2024-08-01',
        2295: '2024-09-01',
        2296: '2024-10-01',
        2297: '2024-11-01',
        2298: '2024-12-01',
        2463: '2025-01-01',
        2464: '2025-02-01',
        2465: '2025-03-01',
        2466: '2025-04-01',
        2467: '2025-05-01',
        2468: '2025-06-01',
        2469: '2025-07-01',
        2470: '2025-08-01',
        2471: '2025-09-01',
        2472: '2025-10-01',
        2473: '2025-11-01',
        2474: '2025-12-01'

    }
    fecha_eventos_dict_reversed = {v: k for k, v in fecha_eventos_dict.items()}
    fecha_eventos_df = pd.DataFrame(list(fecha_eventos_dict_reversed.items()),
                                    columns=['PROM_FECHA_INICIO', 'EVENTO_ID'])

    inducidas['PROM_FECHA_INICIO'] = pd.to_datetime(inducidas['PROM_FECHA_INICIO'])
    fecha_eventos_df['PROM_FECHA_INICIO'] = pd.to_datetime(fecha_eventos_df['PROM_FECHA_INICIO'])

    inducidas = inducidas.merge(fecha_eventos_df, on='PROM_FECHA_INICIO', how='inner')

    inducidas['PROM_FECHA_INICIO'] = inducidas['PROM_FECHA_INICIO'].dt.strftime('%d/%m/%Y')
    inducidas['PROM_FECHA_FIN'] = inducidas['PROM_FECHA_FIN'].dt.strftime('%d/%m/%Y')

    inducidas = inducidas[['PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'EVENTO_ID', 'PRECIO DE VENTA', 'Item']]

    inducidas.rename({
        'Item': 'ORIN',
        'PRECIO DE VENTA': 'PROM_PVP_OFERTA'
    }, axis=1, inplace=True)

    inducidas.loc[inducidas['PROM_PVP_OFERTA'] == '', 'PROM_PVP_OFERTA'] = 0
    inducidas['PROM_PVP_OFERTA'].fillna(0, inplace=True)
    inducidas['PROM_PVP_OFERTA'] = inducidas['PROM_PVP_OFERTA'].astype(int)
    inducidas.loc[inducidas['PROM_PVP_OFERTA'] == 0, 'PROM_PVP_OFERTA'] = 0

    inducidas.dropna(inplace=True)

    query = '''
    WITH CTE_1 AS (
        SELECT
            LGL.GEOG_LOCL_COD
        FROM
            MSTRDB.DWH.LU_GEOG_LOCAL AS LGL
            LEFT JOIN MSTRDB.DWH.LU_GEOG_TIPO_LOCAL AS LGTL ON LGTL.GEOG_TLOC_ID = LGL.GEOG_TLOC_ID
        WHERE
            LGL.GEOG_UNNG_ID = 2
            AND LGL.GEOG_LOCL_COD NOT IN (196, 197, 198)
    )

    SELECT
        DISTINCT
        '{inicio_snow}' AS PROM_FECHA_INICIO,
        '{fin_snow}' AS PROM_FECHA_FIN,
        LAA.ARTC_ARTC_ID,
        LGL.GEOG_LOCL_ID,
        0 AS PRONOSTICO_VENTA,
        0 AS STOCK_INICIAL_PROMO,
        {evento_id_snow} AS EVENTO_ID,
        {precio_oferta_snow} AS PROM_PVP_OFERTA,
        0 AS PROM_LOCAL_ACTIVO,
        0 AS PROM_ESTIBA
    FROM
        MSTRDB.DWH.FT_STOCK AS FS
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
        INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON LGL.GEOG_LOCL_ID = FS.GEOG_LOCL_ID
    WHERE
        LGL.GEOG_LOCL_COD IN (SELECT * FROM CTE_1)
        AND FS.TIEM_DIA_ID = CURRENT_DATE() - 1
        AND LAA.ORIN = '{orines_snow}'
    '''

    carga_inducidas = pd.DataFrame()

    for orin, precio_oferta, inicio, fin, evento_id in zip(
            inducidas['ORIN'],
            inducidas['PROM_PVP_OFERTA'],
            inducidas['PROM_FECHA_INICIO'],
            inducidas['PROM_FECHA_FIN'],
            inducidas['EVENTO_ID']
    ):
        cursor.execute(query.format(
            precio_oferta_snow=precio_oferta,
            orines_snow=orin,
            inicio_snow=inicio,
            fin_snow=fin,
            evento_id_snow=evento_id
        ))

        df_aux = cursor.fetch_pandas_all()

        carga_inducidas = pd.concat([carga_inducidas, df_aux])

    carga_inducidas_excel = carga_inducidas.copy(deep=True)

    carga_inducidas_excel['PROM_FECHA_INICIO'] = pd.to_datetime(carga_inducidas_excel['PROM_FECHA_INICIO'], format='%d/%m/%Y').dt.date
    carga_inducidas_excel['PROM_FECHA_FIN'] = pd.to_datetime(carga_inducidas_excel['PROM_FECHA_FIN'], format='%d/%m/%Y').dt.date

    st.dataframe(carga_inducidas_excel[['EVENTO_ID', 'PROM_FECHA_INICIO', 'PROM_FECHA_FIN']].drop_duplicates())

    query = '''
    SELECT
        GEOG_LOCL_ID,
        GEOG_LOCL_COD
    FROM
        MSTRDB.DWH.LU_GEOG_LOCAL
    '''

    cursor.execute(query)
    local_snow = cursor.fetch_pandas_all()

    carga_inducidas_excel = carga_inducidas_excel.merge(local_snow, on='GEOG_LOCL_ID', how='inner')

    query = '''
    SELECT
        ARTC_ARTC_ID,
        ARTC_ARTC_COD
    FROM
        MSTRDB.DWH.LU_ARTC_ARTICULO
    '''

    cursor.execute(query)
    articulo_snow = cursor.fetch_pandas_all()

    carga_inducidas_excel = carga_inducidas_excel.merge(articulo_snow, on='ARTC_ARTC_ID', how='inner')

    carga_inducidas_excel = carga_inducidas_excel[
       pd.to_datetime(carga_inducidas_excel['PROM_FECHA_INICIO']) > datetime.today().replace(day=1) - relativedelta(
           days=1)
       ]

    path = paths['files_para_BI']
    file_name = f"Inducidas - {datetime.today().strftime('%Y-%m-%d')}.xlsx"
    path_file_name = os.path.join(path, file_name)

    carga_inducidas_excel[[
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
    ]].to_excel(path_file_name, index=False)

    st.write('Termina descargar_inducidas')