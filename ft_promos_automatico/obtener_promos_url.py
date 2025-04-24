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
from gspread.exceptions import APIError, WorksheetNotFound
import time
from config import *
from dateutil.relativedelta import relativedelta
import logging
import warnings
import streamlit as st

# Suppress the specific FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Cuenta de Servicio Leo

# mangold-cuenta-servicio@projecto-promos.iam.gserviceaccount.com

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# Parte 1. Defino la funcion
# Inputs: url y worksheet


def retry_api_call(function, retries=5, initial_delay=1, log=True):
    """
    Retries the provided function if an APIError occurs, with exponential backoff.
    """
    for attempt in range(retries):
        try:
            return function()
        except APIError as e:
            error_code = getattr(e.response, 'status_code', None)
            if error_code in {503, 500}:
                if log:
                    logging.warning(f"API error {error_code}. Retrying in {initial_delay * (2 ** attempt)} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(initial_delay * (2 ** attempt))  # Exponential backoff
            else:
                raise e
    if log:
        logging.error("API is still unavailable after multiple retries.")
    raise APIError("Failed after multiple retries. The service may be down.")


def descargar_promos_iterativo(cursor, url, worksheet):

    # Llamo la funcion para cada combinacion url - worksheet correspondiente
    gs = gc.open_by_key(url)

    try:
        worksheetL = worksheet
    except gspread.exceptions.WorksheetNotFound:
        st.write(f"{worksheet} not found")

    evento_id = gs.title.split('(')[-1].split(')')[0]
    if not (evento_id.isdigit() and len(evento_id) ==4):
        st.write(f'Error en tomar evento_id: {gs.title}')
        sys.exit()

    time.sleep(5)

    data = worksheetL.get_all_values()
    juli0 = pd.DataFrame(data)

    evento_comercial_position = None
    for i, row in juli0.iterrows():
        for j, value in row.items():
            if value == 'EVENTO COMERCIAL':
                evento_comercial_position = (i, j)
                break
        if evento_comercial_position:
            break

    evento_comercial_position
    juli0.iloc[evento_comercial_position]

    add_nombre_evento = (0, 2)
    nombre_evento_position = (evento_comercial_position[0] + add_nombre_evento[0], evento_comercial_position[1] + add_nombre_evento[1])
    nombre_evento = juli0.iloc[nombre_evento_position]

    add_fecha_ini = (1, 2)
    fecha_ini_position = (evento_comercial_position[0] + add_fecha_ini[0], evento_comercial_position[1] + add_fecha_ini[1])
    fini = juli0.iloc[fecha_ini_position]

    add_fecha_fin = (2, 2)
    fecha_fin_position = (evento_comercial_position[0] + add_fecha_fin[0], evento_comercial_position[1] + add_fecha_fin[1])
    ffin = juli0.iloc[fecha_fin_position]

    add_estado_articulos = (3, 2)
    estado_articulos_position = (evento_comercial_position[0] + add_estado_articulos[0], evento_comercial_position[1] + add_estado_articulos[1])
    estado_articulos = juli0.iloc[estado_articulos_position]

    try:
        estado_articulos = str(estado_articulos)  # Convert to string
        # Split by comma or period and map the result to integers
        split_values = tuple(map(int, re.split(r'[,.]', estado_articulos)))

        # If the result has only one element, return it as a single integer
        if len(split_values) == 1:
            #estado_articulos = split_values[0]
            estado_articulos = '(' + str(split_values[0]) + ')'
        else:
            estado_articulos = split_values

        st.write(f"Estados: {estado_articulos}")
    except ValueError as e:
        st.write(f"{gs.title} - Error: {e}. Could not convert one or more items to integers.")
        return

    add_clave_estado_articulos = (3, 0)
    estado_clave_estado_articulos = (evento_comercial_position[0] + add_clave_estado_articulos[0], evento_comercial_position[1] + add_clave_estado_articulos[1])
    clave_estado_articulos = juli0.iloc[estado_clave_estado_articulos]

    # ESTADISTICO / ORIN / DESCRIPCION / PRECIO PLUS / ESTIMADO CERCANIAS / GO

    juli = pd.DataFrame()

    estadistico_position = None
    for i, row in juli0.iterrows():
        for j, value in row.items():
            if value == 'ESTADÍSTICO':
                estadistico_position = (i, j)
                break
        if estadistico_position:
            break

    if estadistico_position:
        start_row, start_col = estadistico_position
        juli = juli0.iloc[start_row:, juli0.columns.get_loc(start_col):]
        juli.columns = juli.iloc[0]
        juli = juli.drop(juli.index[0])
    else:
        st.write(f"{gs.title} - Value 'ESTADÍSTICO' not found in the DataFrame.")

    columns_to_consider = [0, 1, 2, 4]
    selected_columns = juli.columns[columns_to_consider].tolist()

    if 'ESTIMADO CERCANIAS' in juli.columns:
        selected_columns.append('ESTIMADO CERCANIAS')

    # GO

    try:
        go_sheet = gs.worksheet('Guia Op.')
    except gspread.exceptions.WorksheetNotFound:
        st.write(f"{gs.title} - Guia Op. worksheet not found")

    data = go_sheet.get_all_values()
    go_df = pd.DataFrame(data)
    go_df = go_df[[3]]
    go_df.rename({3:'GO'}, axis=1, inplace=True)
    go_df = go_df.dropna()
    go_df = go_df[go_df['GO'] != '']

    ## Comienza flujo

    df = juli.copy()

    if 'GO' not in df.columns:
        df['GO'] = np.nan

    df.loc[df['ORIN'].isin(go_df['GO']), 'GO'] = 1

    hoy = datetime.today().date().strftime('%Y-%m-%d')

    try:
        df.to_csv(f"{paths['respaldo_BI']}\\Drive Lu - {evento_id} - {hoy}.csv", index=False)
    except:
        df.to_csv(f"Drive Lu - {evento_id} - {hoy}.csv", index=False) # Si no puede guardar en la carpeta compartida, guarda en nuestro directorio local
        st.write(f"{gs.title} -Guardado en PC")

    #### LIMPIO ESTADISTICOS NO NUMERICOS
    df['ESTADÍSTICO'] = pd.to_numeric(df['ESTADÍSTICO'], errors='coerce')
    df = df.dropna(subset=['ESTADÍSTICO'])

    if df.empty:
        st.write(f"{gs.title} - No valid 'ESTADÍSTICO' data after cleaning, skipping.")
        #continue  # Skip to the next URL if no valid data
        return

    df['ESTADÍSTICO']=df['ESTADÍSTICO'].astype(int)
    df.rename(columns={
        "ESTADÍSTICO": "ESTADISTICO",
        'PRECIO PLUS':'PVP OFERTA',
        'DESCRIPCIÓN':'DESCRIPCION',
        'GO':'ESTIBA_GUIA_OPERATIVA'
    }, inplace=True)

    ### GENERO LISTA DE ESTADISTICOS Y ORINES PARA BUSCAR EN SNOW
    df['ESTADISTICO'] = df['ESTADISTICO'].apply(lambda x: str(x))
    df['ESTADISTICO']=df['ESTADISTICO'].str.strip()
    estadistico = df['ESTADISTICO'].tolist()
    estadistico = ', '.join(f"'{item}'" for item in estadistico)
    df['ORIN'] = df['ORIN'].apply(lambda x: str(x))
    df['ORIN']=df['ORIN'].str.strip()
    df['ORIN'].replace('nan', pd.NA, inplace=True)
    df_orin=df[['ORIN']].copy()
    df_orin=df_orin.drop_duplicates()
    df_orin= df_orin.dropna(subset=['ORIN'])
    ORIN = df_orin['ORIN'].tolist()
    ORIN = ', '.join(f"'{item}'" for item in ORIN)
    duplis_drive=df.groupby(['ESTADISTICO','ORIN'])['DESCRIPCION'].count().reset_index().sort_values(by='DESCRIPCION', ascending=False)
    df_t=df.sort_values(by=['ESTADISTICO','ORIN','PVP OFERTA'])
    df_no_duplicates = df_t.drop_duplicates(subset=['ESTADISTICO','ORIN'], keep='first')

    ### Locales

    cercania = pd.DataFrame()

    if 'ESTIMADO CERCANIAS' in df.columns:
        cercania = df[~df['ESTIMADO CERCANIAS'].isna()]
        cercania = df[df['ESTIMADO CERCANIAS'] != '']

    locales_mailing = 101,102,103,104,107,108,111,115,117,119,120,121,122,124,125,126,127,131,132,134,139,140,141,143,144,145,146,147,148,149,150,151,152,153,155,156,157,158,159,160,161,162,163,164,165,167,166,168,173,198,301,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,321,322,323,324,325,326,327,328,329,330,331,332,333,334,335,417,480,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497

    #len(locales_mailing) # supers + hipers + 2 express

    locales_cercania = 302, 320, 401, 403, 410, 411, 412, 418, 419, 405, 406
    #len(locales_cercania)

    if len([value for value in locales_cercania if value in locales_mailing]) != 0:
        st.write(f"{gs.title} - Error - se repiten locales")
        sys.exit()

    # 1. LOCALES MAILING

    if clave_estado_articulos.lower() == 'estado artículos': # me especifican estado articulo
        #print('entra en Estado Artículos')
        query = f'''
                SELECT
                    DISTINCT
                    '{fini}' AS "Fecha Desde",
                    '{ffin}' AS "Fecha Hasta",
                    LAA.ARTC_ARTC_COD AS ESTADISTICO,
                    LAA.ORIN,
                    {evento_id} AS "Nombre Evento",
                    NULL AS "Pronostico de Venta",
                    0 AS "Stock Inicial Promo",
                    LGL.GEOG_LOCL_COD AS LOCAL
                FROM
                    MSTRDB.DWH.FT_STOCK AS FS
                    INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
                    INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON
                        FS.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
                        AND LGL.GEOG_LOCL_COD IN {locales_mailing}
                WHERE
                    FS.TIEM_DIA_ID = CURRENT_DATE - 1
                    AND (LAA.ORIN IN ({ORIN}) OR LAA.ARTC_ARTC_COD IN ({estadistico}))
                    AND LAA.ORIN <> -1
                    AND FS.ARTC_ESTA_ID IN {estado_articulos}
                    '''

    else:
        #print('NO entra en Estado Artículos')
        query = f'''
                SELECT
                    DISTINCT
                    '{fini}' AS "Fecha Desde",
                    '{ffin}' AS "Fecha Hasta",
                    LAA.ARTC_ARTC_COD AS ESTADISTICO,
                    LAA.ORIN,
                    {evento_id} AS "Nombre Evento",
                    NULL AS "Pronostico de Venta",
                    0 AS "Stock Inicial Promo",
                    LGL.GEOG_LOCL_COD AS LOCAL
                FROM
                    MSTRDB.DWH.FT_STOCK AS FS
                    INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
                    INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON
                        FS.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
                        AND LGL.GEOG_LOCL_COD IN {locales_mailing}
                WHERE
                    FS.TIEM_DIA_ID = CURRENT_DATE - 1
                    AND (LAA.ORIN IN ({ORIN}) OR LAA.ARTC_ARTC_COD IN ({estadistico}))
                    AND LAA.ORIN <> -1
                    AND FS.ARTC_ESTA_ID IN (4, 6)
                    '''

    cursor.execute(query)
    info = cursor.fetch_pandas_all()

    # 2. LOCALES CERCANIA

    info_cercania = pd.DataFrame()

    if len(cercania) > 0:

        ORIN_cercania = cercania['ORIN'].unique().tolist()
        ORIN_cercania = ', '.join(f"'{item}'" for item in ORIN_cercania)

        estadistico_cercania = cercania['ESTADISTICO'].unique().tolist()
        estadistico_cercania = ', '.join(f"'{item}'" for item in estadistico_cercania)

        if clave_estado_articulos.lower() == 'estado artículos': # me especifican estado articulo
            query = f'''
                    SELECT
                        DISTINCT
                        '{fini}' AS "Fecha Desde",
                        '{ffin}' AS "Fecha Hasta",
                        LAA.ARTC_ARTC_COD AS ESTADISTICO,
                        LAA.ORIN,
                        {evento_id} AS "Nombre Evento",
                        NULL AS "Pronostico de Venta",
                        0 AS "Stock Inicial Promo",
                        LGL.GEOG_LOCL_COD AS LOCAL
                    FROM
                        MSTRDB.DWH.FT_STOCK AS FS
                        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
                        INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON
                            FS.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
                            AND LGL.GEOG_LOCL_COD IN {locales_cercania}
                    WHERE
                        FS.TIEM_DIA_ID = CURRENT_DATE - 1
                        AND (LAA.ORIN IN ({ORIN_cercania}) OR LAA.ARTC_ARTC_COD IN ({estadistico_cercania}))
                        AND LAA.ORIN <> -1
                        AND FS.ARTC_ESTA_ID IN {estado_articulos}
                    '''

        else:
            query = f'''
                    SELECT
                        DISTINCT
                        '{fini}' AS "Fecha Desde",
                        '{ffin}' AS "Fecha Hasta",
                        LAA.ARTC_ARTC_COD AS ESTADISTICO,
                        LAA.ORIN,
                        {evento_id} AS "Nombre Evento",
                        NULL AS "Pronostico de Venta",
                        0 AS "Stock Inicial Promo",
                        LGL.GEOG_LOCL_COD AS LOCAL
                    FROM
                        MSTRDB.DWH.FT_STOCK AS FS
                        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
                        INNER JOIN MSTRDB.DWH.LU_GEOG_LOCAL AS LGL ON
                            FS.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
                            AND LGL.GEOG_LOCL_COD IN {locales_cercania}
                    WHERE
                        FS.TIEM_DIA_ID = CURRENT_DATE - 1
                        AND (LAA.ORIN IN ({ORIN_cercania}) OR LAA.ARTC_ARTC_COD IN ({estadistico_cercania}))
                        AND LAA.ORIN <> -1
                        AND FS.ARTC_ESTA_ID IN (4, 6)
                    '''

        cursor.execute(query)
        info_cercania = cursor.fetch_pandas_all()

    info = pd.concat([info, info_cercania])

    ### check cantidad de articulos


    ## REVISO FALTANTES

    faltantes=df_no_duplicates.copy()
    info_unicos=info[['ESTADISTICO','ORIN','Nombre Evento']].drop_duplicates()

    faltantes=faltantes.merge(info_unicos[['ESTADISTICO','Nombre Evento']], on=['ESTADISTICO'], how='left')
    faltantes=faltantes.merge(info_unicos[['ORIN','Nombre Evento']], on=['ORIN'], how='left')

    faltantes['result'] = np.where(faltantes['Nombre Evento_x'].isna() & faltantes['Nombre Evento_y'].isna(), "NULOS", "Not NULOS")
    faltantes_final=faltantes[faltantes.result=='NULOS'].reset_index()
    faltantes_final.drop(columns={'index'}, inplace=True)

    faltantes_final['evento_id']=evento_id
    faltantes_final['Nombre Evento']=nombre_evento
    faltantes_final['Fecha Inicio']=fini
    faltantes_final['Fecha Fin']=ffin
    faltantes_final['fecha_carga'] = datetime.now()

    faltantes_final=faltantes_final[['ESTADISTICO', 'ORIN', 'DESCRIPCION', 'PVP OFERTA',  'evento_id', 'Nombre Evento',
                                     'Fecha Inicio', 'Fecha Fin', 'fecha_carga']]

    hoy = datetime.now().strftime('%Y-%m-%d').replace("-","_")

    
    # LEO

    if len(faltantes_final) > 0:
        try:
            worksheet1 = retry_api_call(lambda: gs.worksheet('FALTANTES'))
            worksheet1.clear()
        except WorksheetNotFound:
            # Only create a new worksheet if faltantes_final has valid dimensions
            if faltantes_final.shape[0] > 0 and faltantes_final.shape[1] > 0:
                worksheet1 = retry_api_call(
                    lambda: gs.add_worksheet(
                        title="FALTANTES", rows=faltantes_final.shape[0], cols=faltantes_final.shape[1]
                    )
                )
            else:
                raise ValueError("faltantes_final has invalid dimensions for worksheet creation.")

    faltantes_final['ORIN']=faltantes_final['ORIN'].str.strip()
    faltantes_final['ORIN']= faltantes_final['ORIN'].apply(lambda x: str(x))
    faltantes_orin_list = faltantes_final['ORIN'].tolist()
    faltantes_orin = ', '.join(f"'{item}'" for item in faltantes_orin_list)

    faltantes_final['ESTADISTICO'] = faltantes_final['ESTADISTICO'].str.strip()
    faltantes_final['ESTADISTICO'] = faltantes_final['ESTADISTICO'].apply(lambda x: str(x))
    faltantes_EST_list = faltantes_final['ESTADISTICO'].tolist()
    faltantes_EST = ', '.join(f"'{item}'" for item in faltantes_EST_list)

    if clave_estado_articulos == 'Estado Artículos':

        query = f'''
                select
                    distinct artc_artc_cod,
                    artc_artc_desc,
                    orin,
                    artc_esta_id
                from
                    mstrdb.dwh.ft_stock a
                    inner join mstrdb.dwh.lu_artc_articulo b on a.artc_artc_id=b.artc_artc_id
                    inner join mstrdb.dwh.lu_geog_local d on d.geog_locl_id=a.geog_locl_id and geog_locl_cod not in (100,199)
                    where
                        (orin in ({faltantes_orin}) or artc_artc_cod in ({faltantes_EST}))
                        and orin<>'-1'
                        and tiem_Dia_id=current_date()-1
                        and geog_locl_cod in {locales_mailing}
                        AND A.ARTC_ESTA_ID IN {estado_articulos}
                '''
    else:

        query = f'''
                select
                    distinct artc_artc_cod,
                    artc_artc_desc,
                    orin,
                    artc_esta_id
                from
                    mstrdb.dwh.ft_stock a
                    inner join mstrdb.dwh.lu_artc_articulo b on a.artc_artc_id=b.artc_artc_id
                    inner join mstrdb.dwh.lu_geog_local d on d.geog_locl_id=a.geog_locl_id and geog_locl_cod not in (100,199)
                    where
                        (orin in ({faltantes_orin}) or artc_artc_cod in ({faltantes_EST}))
                        and orin<>'-1'
                        and tiem_Dia_id=current_date()-1
                        and geog_locl_cod in {locales_mailing}
                        and artc_esta_id in (4,6)
                '''

    if len(faltantes_final)>0:
        cursor.execute(query)
        check_activos = cursor.fetch_pandas_all()

    else:
        check_activos = pd.DataFrame()

    if clave_estado_articulos == 'Estado Artículos':

        query = f'''
                select
                    distinct artc_artc_cod,
                    artc_artc_desc,
                    orin,
                    artc_esta_id ESTADO,
                    count(distinct a.geog_locl_id) locales
                from
                    mstrdb.dwh.ft_stock a
                    inner join mstrdb.dwh.lu_artc_articulo b on a.artc_artc_id=b.artc_artc_id
                    inner join mstrdb.dwh.lu_geog_local d on d.geog_locl_id=a.geog_locl_id and d.geog_locl_cod not in (100,199)
                where
                    (orin in ({faltantes_orin}) or artc_artc_cod in ({faltantes_EST}))
                    and orin<>'-1'
                    and tiem_Dia_id=current_date()-1
                    and geog_locl_cod in {locales_mailing}
                    AND A.ARTC_ESTA_ID IN {estado_articulos}
                group by
                    all
                '''
    else:

        query = f'''
                select
                    distinct artc_artc_cod,
                    artc_artc_desc,
                    orin,
                    artc_esta_id ESTADO,
                    count(distinct a.geog_locl_id) locales
                from
                    mstrdb.dwh.ft_stock a
                    inner join mstrdb.dwh.lu_artc_articulo b on a.artc_artc_id=b.artc_artc_id
                    inner join mstrdb.dwh.lu_geog_local d on d.geog_locl_id=a.geog_locl_id and d.geog_locl_cod not in (100,199)
                where
                    (orin in ({faltantes_orin}) or artc_artc_cod in ({faltantes_EST}))
                    and orin<>'-1'
                    and tiem_Dia_id=current_date()-1
                    and geog_locl_cod in {locales_mailing}
                    and artc_esta_id in (4,6)
                group by
                    all
                '''

    if len(faltantes_final)>0:
        cursor.execute(query)
        check_activos = cursor.fetch_pandas_all()

        check_activos.sort_values(by='ARTC_ARTC_COD')

    if len(faltantes_final)>0:
        cursor.execute(query)
        check_activos = cursor.fetch_pandas_all()

    else:
        check_activos = pd.DataFrame()

    if len(check_activos)>0:
        try:
            gs.worksheet('FALTANTES x ESTADO')
            worksheet1 = gs.worksheet('FALTANTES x ESTADO')
            worksheet1.clear()
            set_with_dataframe(worksheet=worksheet1, dataframe=check_activos, include_index=False,include_column_header=True, resize=True)

        except:
            new_worksheet = gs.add_worksheet(title="FALTANTES x ESTADO", rows=check_activos.shape[0], cols=check_activos.shape[1])
            worksheet1 = gs.worksheet('FALTANTES x ESTADO')
            set_with_dataframe(worksheet=worksheet1, dataframe=check_activos, include_index=False,include_column_header=True, resize=True)

    if len(check_activos) == 0:

        try:
            gs.worksheet('FALTANTES x ESTADO')
            worksheet1 = gs.worksheet('FALTANTES x ESTADO')
            worksheet1.clear()

        except:
            pass

    query=f"""select distinct artc_artc_cod,artc_artc_desc, orin, artc_artc_id, ARTC_ARTC_FFIN
            from  mstrdb.dwh.lu_artc_articulo b where (orin in ({faltantes_orin}) or artc_artc_cod in ({faltantes_EST}))
            and orin<>'-1' """

    if len(faltantes_final)>0:
        cursor.execute(query)
        check_articulos = cursor.fetch_pandas_all()

    if len(faltantes_final)>0:
        faltantes_en_lu_articulo0 = pd.DataFrame({'ORIN': faltantes_orin_list},index=range(len(faltantes_orin_list)))


        faltantes_en_lu_articulo = faltantes_en_lu_articulo0[~faltantes_en_lu_articulo0['ORIN'].isin(check_articulos['ORIN'])]


    if len(faltantes_final) > 0:
        if len(faltantes_en_lu_articulo) > 0:
            try:
                # Try to get the existing worksheet
                worksheet1 = retry_api_call(lambda: gs.worksheet('FALTANTES en MAESTRO ARTC'))
                worksheet1.clear()  # Clear the existing worksheet
            except WorksheetNotFound:
                # Create a new worksheet only if faltantes_final has valid dimensions
                if faltantes_final.shape[0] > 0 and faltantes_final.shape[1] > 0:
                    worksheet1 = retry_api_call(
                        lambda: gs.add_worksheet(
                            title="FALTANTES en MAESTRO ARTC",
                            rows=faltantes_final.shape[0],
                            cols=faltantes_final.shape[1]
                        )
                    )
                else:
                    raise ValueError("faltantes_final has invalid dimensions for worksheet creation.")

            # Insert values after clearing or creating the worksheet
            set_with_dataframe(
                worksheet=worksheet1,
                dataframe=df[df.ORIN.isin(faltantes_en_lu_articulo.ORIN)],
                include_index=False,  # Don't include index
                include_column_header=True,  # Include column headers
                resize=True  # Resize columns to fit content
            )


    precios=df_no_duplicates.copy()

    precios['PVP OFERTA']=precios['PVP OFERTA'].astype(str).str.replace('$',"")
    precios['PVP OFERTA'] = pd.to_numeric(precios['PVP OFERTA'], errors='coerce')
    precios = precios.dropna(subset=['PVP OFERTA'])
    precios['PVP OFERTA']=round(precios['PVP OFERTA'].astype(float)).astype(int).astype(str)
    a_cargar=info.merge(precios[['ESTADISTICO','PVP OFERTA']].drop_duplicates(), on='ESTADISTICO', how='left')
    a_cargar=a_cargar.merge(precios[['ORIN','PVP OFERTA']].drop_duplicates(), on='ORIN', how='left')
    a_cargar=a_cargar.merge(df[['ORIN', 'ESTIBA_GUIA_OPERATIVA']], on='ORIN', how='left')


    a_cargar['ESTIBA_GUIA_OPERATIVA'] = pd.to_numeric(a_cargar['ESTIBA_GUIA_OPERATIVA'], errors='coerce')
    a_cargar['ESTIBA_GUIA_OPERATIVA'].fillna(0, inplace=True)
    a_cargar['ESTIBA_GUIA_OPERATIVA'] = a_cargar['ESTIBA_GUIA_OPERATIVA'].astype(int)
    a_cargar['ESTIBA_GUIA_OPERATIVA'] = [1 if x != 0 else 0 for x in a_cargar['ESTIBA_GUIA_OPERATIVA']]
    a_cargar['ESTIBA_GUIA_OPERATIVA'] = a_cargar['ESTIBA_GUIA_OPERATIVA'].astype(str)

    len(info) == a_cargar.shape[0]

    # En caso de diferencia

    # El scrip lo soluciona
    # Deja los q tienen duplicado x un lado , les saca el precio y simplifica

    a_cargar['PVP OFERTA']=a_cargar['PVP OFERTA_x'].fillna(a_cargar['PVP OFERTA_y'])
    a_cargar=a_cargar[['Fecha Desde', 'Fecha Hasta', 'ESTADISTICO', 'ORIN', 'Nombre Evento',
                       'Pronostico de Venta', 'Stock Inicial Promo', 'LOCAL',   'PVP OFERTA', 'ESTIBA_GUIA_OPERATIVA']]

    duplis_precios=a_cargar.groupby(['ESTADISTICO','LOCAL'])['PVP OFERTA'].count().reset_index()

    duplis_precios.sort_values(by='PVP OFERTA', ascending=False).head()
    a_cargar_sin_duplis=a_cargar[~a_cargar.ESTADISTICO.isin(duplis_precios[duplis_precios['PVP OFERTA']>1]['ESTADISTICO'].unique())]
    a_cargar_con_dupli=a_cargar[a_cargar.ESTADISTICO.isin(duplis_precios[duplis_precios['PVP OFERTA']>1]['ESTADISTICO'].unique())]
    a_cargar_con_dupli.loc[:,'PVP OFERTA']=np.nan
    a_cargar_con_dupli=a_cargar_con_dupli.drop_duplicates()
    a_cargar_f=pd.concat([a_cargar_con_dupli,a_cargar_sin_duplis])
    a_cargar_base=a_cargar_f.copy()
    a_cargar_base.rename(columns={'Fecha Desde':'FECHA_DESDE','Fecha Hasta':'FECHA_HASTA', 'Nombre Evento':'EVENTO_ID'}, inplace=True)
    a_cargar_base['LOCAL_ACTIVO']=1
    a_cargar_base['USER']='LEO'
    a_cargar_base['FECHA_DE_CARGA'] = datetime.now().strftime('%Y-%m-%d %H:%M')
    a_cargar_base['DETALLE']='carga inicial'
    a_cargar_base=a_cargar_base[['FECHA_DESDE', 'FECHA_HASTA','ESTADISTICO', 'EVENTO_ID',
                                 'Pronostico de Venta', 'Stock Inicial Promo', 'LOCAL', 'PVP OFERTA','LOCAL_ACTIVO','ESTIBA_GUIA_OPERATIVA',
                                 'USER', 'FECHA_DE_CARGA','ORIN','DETALLE']]

    ## grabo cantidad de combinaciones cargadas

    agrupado_por_item=a_cargar_base.groupby(['ESTADISTICO'])['LOCAL'].count().reset_index().sort_values(by='LOCAL')
    agrupado_por_item.rename(columns={'LOCAL':'LOCALES ASIGNADOS'}, inplace=True)

    try:

        # Try to access the worksheet 'FINALMENTE CARGADO'
        try:
            worksheetF = gs.worksheet('FINALMENTE CARGADO')
            worksheetF.clear()  # Clear the existing data

        except gspread.exceptions.WorksheetNotFound:
            # Create a new worksheet if it doesn't exist
            #print("Worksheet 'FINALMENTE CARGADO' does not exist. Creating a new one.")
            worksheetF = gs.add_worksheet(title="FINALMENTE CARGADO", rows=check_activos.shape[0],
                                          cols=check_activos.shape[1])

        # Now, set the new data into the worksheet
        set_with_dataframe(worksheet=worksheetF, dataframe=agrupado_por_item, include_index=False,
                           include_column_header=True, resize=True)

    except gspread.exceptions.APIError as e:
        st.write(f"APIError: {e}")
    except Exception as e:
        st.write(f"An unexpected error occurred: {e}")

    ## Dejo registro de la carga

    try:
        # Guia Leo
        #excel_guia_leo = pd.read_excel('T:\\BI\\Comercial\\ofertas\\Leo Cargados.xlsx')
        excel_guia_leo = pd.read_excel(excels['excel_detalle_cargados'])

        # Eventos
        excel_eventos = pd.read_excel(excels['excel_BI'], sheet_name='Eventos')

        try:
            evento_descripcion = excel_eventos.loc[excel_eventos['evento_id'] == evento_id, 'evento_desc'].iloc[0]

        except:
            evento_descripcion = excel_eventos.loc[excel_eventos['evento_id'] == int(evento_id), 'evento_desc'].iloc[0]

        new_row = {
            'Fecha': datetime.today().strftime('%m/%d/%Y'),
            'Evento ID': evento_id,
            'Descripcion': evento_descripcion,
            'Filas': a_cargar_base.shape[0]
        }

        # Insert the new row at the end
        excel_guia_leo.loc[len(excel_guia_leo)] = new_row
        excel_guia_leo['Filas'].fillna(0, inplace =True)
        excel_guia_leo['Filas'] = excel_guia_leo['Filas'].astype(int)

        excel_guia_leo.to_excel(excels['excel_detalle_cargados'], index = False)

    except FileNotFoundError:
        st.write(f"{gs.title} - File not found. Please check the file paths.")

    except Exception as e:
        st.write("f{title} - An error occurred:", e)

    ## FT Promos (Excel)

    query = '''
            SELECT
                *
            FROM
                MSTRDB.DWH.FT_PROMOS
            WHERE
                EVENTO_ID = {snow_evento_id}
            '''

    cursor.execute(query.format(snow_evento_id = evento_id))
    snow = cursor.fetch_pandas_all()

    carga_snow = a_cargar_base.copy()

    carga_snow.rename({
        'FECHA_DESDE': 'PROM_FECHA_INICIO',
        'FECHA_HASTA': 'PROM_FECHA_FIN',
        'Fecha_Desde': 'PROM_FECHA_INICIO',
        'Fecha_Hasta': 'PROM_FECHA_FIN',
        'Evento': 'EVENTO_ID',
        'Pronostico de Venta': 'PRONOSTICO_VENTA',
        'Pronostico_Venta': 'PRONOSTICO_VENTA',
        'Stock Inicial Promo': 'STOCK_INICIAL_PROMO',
        'Stock_inicial': 'STOCK_INICIAL_PROMO',
        'PVP OFERTA': 'PROM_PVP_OFERTA',
        'LOCAL_ACTIVO': 'PROM_LOCAL_ACTIVO',
        'Local_activo': 'PROM_LOCAL_ACTIVO',
        'Estiba_guia_operativa': 'PROM_ESTIBA',
        'ESTIBA_GUIA_OPERATIVA': 'PROM_ESTIBA'
    }, axis=1, inplace=True)

    query = '''
            SELECT
                DISTINCT
                ARTC_ARTC_ID,
                ORIN
            FROM
                MSTRDB.DWH.LU_ARTC_ARTICULO
            '''

    cursor.execute(query)
    articulos = cursor.fetch_pandas_all()

    carga_snow = carga_snow.merge(articulos, on = 'ORIN', how = 'left')

    query = '''
            SELECT
                DISTINCT
                GEOG_LOCL_COD AS LOCAL,
                GEOG_LOCL_ID
            FROM
                MSTRDB.DWH.LU_GEOG_LOCAL
            '''

    cursor.execute(query)
    locales = cursor.fetch_pandas_all()

    carga_snow = carga_snow.merge(locales, on = 'LOCAL', how = 'left')

    carga_snow.drop([
        'ESTADISTICO',
        'USER',
        'FECHA_DE_CARGA',
        'ORIN',
        'DETALLE',
        'LOCAL'
    ], axis = 1, inplace = True)

    carga_snow['PROM_FECHA_INICIO'] = pd.to_datetime(carga_snow['PROM_FECHA_INICIO'], dayfirst = True).dt.strftime('%Y-%m-%d')
    carga_snow['PROM_FECHA_FIN'] = pd.to_datetime(carga_snow['PROM_FECHA_FIN'], dayfirst = True).dt.strftime('%Y-%m-%d')
    carga_snow_excel = carga_snow.copy(deep = True)

    # Function to format dates
    def format_date(date_str):
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{date_obj.month}/{date_obj.day}/{date_obj.year}"

    # List of columns to apply the formatting
    date_columns = ['PROM_FECHA_INICIO', 'PROM_FECHA_FIN']

    # Apply the formatting function to each column
    for col in date_columns:
        carga_snow_excel[col] = carga_snow_excel[col].apply(format_date)

    query = '''
            SELECT
                GEOG_LOCL_ID,
                GEOG_LOCL_COD
            FROM
                MSTRDB.DWH.LU_GEOG_LOCAL
            '''

    cursor.execute(query)
    local_snow = cursor.fetch_pandas_all()

    carga_snow_excel = carga_snow_excel.merge(local_snow, on = 'GEOG_LOCL_ID', how = 'inner')

    query = '''
            SELECT
                ARTC_ARTC_ID,
                ARTC_ARTC_COD
            FROM
                MSTRDB.DWH.LU_ARTC_ARTICULO
            '''

    cursor.execute(query)
    articulo_snow = cursor.fetch_pandas_all()

    carga_snow_excel = carga_snow_excel.merge(articulo_snow, on = 'ARTC_ARTC_ID', how = 'inner')

    carga_snow_excel = carga_snow_excel[[
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
    ]]

    path = paths['files_para_BI']
    file_name = f"{evento_id} - {gs.title.split('(')[0]}, {fini.replace('/', '.')} al {ffin.replace('/', '.')} - {datetime.today().strftime('%Y-%m-%d')}.xlsx"

    path_file_name = os.path.join(path, file_name)

    carga_snow_excel.to_excel(f"{path_file_name}", index = False)

    hoy = datetime.today().date().strftime('%Y-%m-%d')

    path = paths['respaldo_BI']
    file_name = f"{nombre_evento.replace('/', '.')} - {evento_id} - {str(fini).replace('/', '.')} a {str(ffin).replace('/', '.')}.csv"
    path_file_name = os.path.join(path, file_name)

    try:
        carga_snow_excel.to_csv(path_file_name,index=False)

    except:
        carga_snow_excel.to_csv(file_name, index=False) # Si no puede guardar en la carpeta compartida, guarda en nuestro directorio local
        st.write(f"{gs.title}, {fini} al {ffin} -Guardado en PC")

def descargar_promos_url(cursor, urls:list):
    # Obtengo las sheets correspondientes para cada url
    # Llamo la funcion descargar_promos_iterativo
    st.write('')
    st.write('Inicia descargar_promos_url')
    if 'urls' not in st.session_state:
        st.session_state.urls = urls
    st.write('')
    url = st.text_input("Ingresar 1 URL")
    if url != '':
        st.session_state.urls.append(url)
    st.write('Se van a procesar las siguientes urls:')
    for u in st.session_state.urls:
        st.write(u)
    no_cargar = st.button('No cargar mas')
    if not(no_cargar):
        time.sleep(600)
    else:
        urls = list(set(st.session_state.urls))
    
    #Procesamiento
    for codigo_url in urls:
        gs = gc.open_by_key(codigo_url)

        st.write('')
        try:
            worksheetL = gs.worksheet('Listado')
        except gspread.exceptions.WorksheetNotFound:
            try:
                worksheetL = gs.worksheet('ARMADO')
            except gspread.exceptions.WorksheetNotFound:
                st.write(f"{gs.title} - Listado and Armado worksheets not found")
                continue
        st.write(gs.title, worksheetL.title)

        descargar_promos_iterativo(cursor, codigo_url, worksheetL)
        st.write(gs.title, ' -- Listo')

    st.write('')
    st.write('Termina descargar_promos_url')