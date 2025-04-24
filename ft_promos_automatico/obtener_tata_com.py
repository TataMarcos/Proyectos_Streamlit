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
from config import *
from dateutil.relativedelta import relativedelta
import streamlit as st

# Cuenta de Servicio Leo

# mangold-cuenta-servicio@projecto-promos.iam.gserviceaccount.com

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

def descargar_tata_com(cursor):

    st.write('')
    st.write('Inicia descargar_tata_com')

    #url = 'https://docs.google.com/spreadsheets/d/1hZGHcDPjKp0ZCJR0QhImdt0_tvoSgHKE4AhO0BykIbk/edit?gid=0#gid=0'
    url = urls['drive_tata_com']

    # mangold-cuenta-servicio@projecto-promos.iam.gserviceaccount.com
    codigo_url = url.split('/')[-2]

    gs = gc.open_by_key(codigo_url)

    try:
        worksheetL = gs.worksheet('Hoja 1')
    except Exception as e:
        st.write(f"An error occurred: {e}")

    data = worksheetL.get_all_values()
    df = pd.DataFrame(data[8:], columns=data[7])
    df = df[['Desde', 'Hasta', 'ORIN']]
    df['Desde'] = pd.to_datetime(df['Desde'], format='%d/%m/%Y', dayfirst=True)
    df['Hasta'] = pd.to_datetime(df['Hasta'], format='%d/%m/%Y', dayfirst=True)
    df = df[df['Desde'] >= '2024-11-01']
    if df.empty:
        st.write('DataFrame vacia')

    # NULLs
    df = df.replace('', np.nan)
    df = df.dropna(how='all')  # elimino filas con todo NULL
    df_null = df[df.isnull().any(axis=1)]  # guardo filas con alguna columna NULL
    df = df.dropna()  # solo trabajo con filas sin NULL

    # Fechas

    # Una fila por mes
    df2 = pd.DataFrame()
    rows_to_add = []

    for row in range(df.shape[0]):
        start_date = pd.to_datetime(df.iloc[row].iloc[0])
        end_date = pd.to_datetime(df.iloc[row].iloc[1])

        # print(start_date, end_date)

        # DISTINTOS MESES
        if start_date.month != end_date.month:

            # PRIMER MES
            rows_to_add.append([start_date, start_date.replace(day=start_date.days_in_month)] + list(df.iloc[row, 2:]))

            # MESES ENTRE MEDIO (completos)
            meses = pd.date_range(
                start=start_date.replace(day=1) + pd.DateOffset(months=1),
                end=end_date.replace(day=1) - pd.DateOffset(days=1),
                freq='MS'
            )

            for month in meses:
                rows_to_add.append([month, month.replace(day=month.days_in_month)] + list(df.iloc[row, 2:]))

            # ULTIMO MES
            rows_to_add.append([end_date.replace(day=1), end_date] + list(df.iloc[row, 2:]))

        # MISMO MES
        else:
            rows_to_add.append([start_date, end_date] + list(df.iloc[row, 2:]))

    if rows_to_add:
        df2 = pd.concat([
            pd.DataFrame(rows_to_add, columns=df.columns)
        ],
            ignore_index=True)

    df2['MES'] = (df2['Desde'].dt.to_period('M').dt.to_timestamp()).astype(str)
    df2.sort_values(by=['ORIN', 'Desde', 'Hasta'])

    # Combinaciones unicas DIA / ORIN / MES
    def expand_dates(row):
        # Create a date range from 'Desde' to 'Hasta' (inclusive)
        date_range = pd.date_range(row['Desde'], row['Hasta'], freq='D')

        # Create DataFrame with 'DIA' column for each date in the range
        return pd.DataFrame({
            'DIA': date_range,
            'Desde': [row['Desde']] * len(date_range),
            'Hasta': [row['Hasta']] * len(date_range),
            'ORIN': [row['ORIN']] * len(date_range),
            'MES': [row['MES']] * len(date_range)
        })

    expanded_df = pd.concat([expand_dates(row) for _, row in df2.iterrows()], ignore_index=True)
    expanded_df_grouped = expanded_df[['DIA', 'ORIN', 'MES']].drop_duplicates()

    # Desde / Hasta consolidados

    # Convert 'DIA' to datetime
    expanded_df_grouped['DIA'] = pd.to_datetime(expanded_df_grouped['DIA'])

    # Sort the DataFrame by ORIN and DIA to ensure continuity
    expanded_df_grouped = expanded_df_grouped.sort_values(by=['ORIN', 'DIA'])

    # Identify the difference between consecutive dates for each ORIN
    expanded_df_grouped['diff'] = expanded_df_grouped.groupby('ORIN')['DIA'].diff().dt.days.fillna(1)

    # Create a 'group' column that identifies the continuous periods
    expanded_df_grouped['group'] = (expanded_df_grouped['diff'] > 1).cumsum()

    # Now aggregate by 'ORIN', 'MES', and 'group' to get the range for each continuous period
    result = expanded_df_grouped.groupby(['ORIN', 'MES', 'group'], as_index=False).agg(
        Desde=('DIA', 'min'),
        Hasta=('DIA', 'max')
    )

    # Format the 'Desde' and 'Hasta' columns to the desired date format
    result['Desde'] = result['Desde'].dt.strftime('%m/%d/%Y')
    result['Hasta'] = result['Hasta'].dt.strftime('%m/%d/%Y')

    df3 = result.copy(deep=True)
    df3.drop(['group'], axis=1, inplace=True)

    ## Locales
    locales = [488, 480, 487, 486, 483, 482, 484, 489, 490, 491, 492, 198]
    df4 = df3.loc[df3.index.repeat(len(locales))].reset_index(drop=True)
    df4['Local'] = locales * len(df3)

    ## Articulo Cod
    query = '''
    SELECT
        ORIN,
        ARTC_ARTC_COD
    FROM
        MSTRDB.DWH.LU_ARTC_ARTICULO
    '''

    cursor.execute(query)
    snow = cursor.fetch_pandas_all()

    df4['ORIN'] = df4['ORIN'].astype(str)
    snow['ORIN'] = snow['ORIN'].astype(str)

    df5 = df4.merge(snow, on='ORIN', how='inner')

    if not df5.shape[0] == df4.shape[0] or df5['ARTC_ARTC_COD'].isna().sum() > 0:
        print('ORINES no encontrados')
        sys.exit()

    ## Evento ID

    fecha_eventos_dict = {
        2414: '2024-11-01',
        2415: '2024-12-01',
        2416: '2025-01-01',
        2417: '2025-02-01',
        2418: '2025-03-01',
        2419: '2025-04-01',
        2420: '2025-05-01',
        2421: '2025-06-01',
        2422: '2025-07-01',
        2423: '2025-08-01',
        2424: '2025-09-01',
        2425: '2025-10-01',
        2426: '2025-11-01',
        2427: '2025-12-01'
    }

    fecha_eventos_dict_reversed = {v: k for k, v in fecha_eventos_dict.items()}
    fecha_eventos_df = pd.DataFrame(list(fecha_eventos_dict_reversed.items()), columns=['MES', 'EVENTO_ID'])

    df6 = df5.merge(fecha_eventos_df, on='MES', how='inner')
    if not df6.shape[0] == df5.shape[0] or df6['EVENTO_ID'].isna().sum() > 0:
        print('Evento ID no encontrados')
        sys.exit()

    ## Alertas

    df_null['Desde'] = df_null['Desde'].astype(str)
    df_null['Hasta'] = df_null['Hasta'].astype(str)

    path = 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\tata_com'
    file_name = f"tata_com_nulls_{datetime.today().strftime('%Y-%m-%d')}.xlsx"
    path_file_name = os.path.join(path, file_name)

    if not df_null.empty:
        df_null.to_excel(f'{path_file_name}', index=False)

    ## Columnas

    df_final = df6.copy(deep=True)
    df_final.rename({
        'Desde': 'PROM_FECHA_INICIO',
        'Hasta': 'PROM_FECHA_FIN',
        'Local': 'GEOG_LOCL_COD'
    }, axis=1, inplace=True)

    df_final['PRONOSTICO_VENTA'] = 0
    df_final['STOCK_INICIAL_PROMO'] = 0
    df_final['PROM_PVP_OFERTA'] = 0
    df_final['PROM_LOCAL_ACTIVO'] = 0
    df_final['PROM_ESTIBA'] = 0
    df_final['Costo'] = 0

    df_final = df_final[[
        'PROM_FECHA_INICIO',
        'PROM_FECHA_FIN',
        'ARTC_ARTC_COD',
        'EVENTO_ID',
        'PRONOSTICO_VENTA',
        'STOCK_INICIAL_PROMO',
        'GEOG_LOCL_COD',
        'PROM_PVP_OFERTA',
        'PROM_LOCAL_ACTIVO',
        'PROM_ESTIBA',
        'Costo'
    ]]

    df_final = df_final[pd.to_datetime(df_final['PROM_FECHA_FIN'])
                        >= datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)]

    ## Genero Excel
    path = paths['files_para_BI']
    file_name = 'tata_com.xlsx'
    path_file_name = os.path.join(path, file_name)

    df_final.to_excel(f'{path_file_name}', index=False)

    st.write('Termina descargar_tata_com')