from utils import get_credentials, snowflake_login, enviar_email
import pandas as pd
import numpy as np
from snowflake.connector.pandas_tools import write_pandas
import xlwings as xw
import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from config import *
import streamlit as st
import time

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# Realizamos conexion
try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login()
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        snow = st.session_state.snow  # Reuse the existing Snowflake session
        user = st.session_state.user
        cursor = st.session_state.cursor
except:
    st.write('Aún no se ingresaron las credenciales')
    st.stop()

if 'flujo' not in st.session_state:
    try:
        url = st.text_input('Ingrese la url de la promoción:')

        codigo_url = url.split('/')[-2]     #Obtenemos el codigo del drive

        gs = gc.open_by_key(codigo_url)     #Leemos
    except:
        st.write('Aún no se ingresó una url válida')
        st.stop()

    try:                                        #Leemos la solapa que necesitamos
        worksheetL = gs.worksheet('Listado')
    except gspread.exceptions.WorksheetNotFound:
        try:
            worksheetL = gs.worksheet('ARMADO')
        except gspread.exceptions.WorksheetNotFound:
            st.write(f"{gs.title} - Listado and Armado worksheets not found")
            st.stop()

    evento_id = gs.title.split('(')[-1].split(')')[0]       #Tomamos el id de evento

    if not (evento_id.isdigit() and len(evento_id) == 4):    #Si no es codigo aceptado cerramos
        st.write(f'Error en tomar evento_id: {gs.title}')
        st.stop()

    #Ponemos todos los datos en un dataframe
    data = worksheetL.get_all_values()
    juli0 = pd.DataFrame(data)

    #Limpiamos
    evento_comercial_position = None
    for i, row in juli0.iterrows():
        for j, value in row.items():
            if value == 'EVENTO COMERCIAL':
                evento_comercial_position = (i, j)
                break
        if evento_comercial_position:
            break

    add_nombre_evento = (0, 2)
    nombre_evento_position = (evento_comercial_position[0] + add_nombre_evento[0], evento_comercial_position[1] + add_nombre_evento[1])
    nombre_evento = juli0.iloc[nombre_evento_position]

    add_fecha_ini = (1, 2)
    fecha_ini_position = (evento_comercial_position[0] + add_fecha_ini[0], evento_comercial_position[1] + add_fecha_ini[1])
    fini = juli0.iloc[fecha_ini_position]

    add_fecha_fin = (2, 2)
    fecha_fin_position = (evento_comercial_position[0] + add_fecha_fin[0], evento_comercial_position[1] + add_fecha_fin[1])
    ffin = juli0.iloc[fecha_fin_position]

    st.write('Promoción:')
    st.write(nombre_evento)
    st.write(fini)
    st.write(ffin)
    time.sleep(3)

    try:
        prom_fecha_inicio = pd.to_datetime(fini, dayfirst=True)#.strftime('%d-%m-%Y')
        prom_fecha_fin = pd.to_datetime(ffin, dayfirst=True)#.strftime('%d-%m-%Y')
    except:
        st.write('Problemas con las fechas de inicio y fin de la promoción')
        st.stop()

    st.write('')
    st.write('Fechas de inicio y fin:')
    st.write(prom_fecha_inicio, '/',  prom_fecha_fin)

    if not prom_fecha_inicio < prom_fecha_fin:
        st.write('')
        st.write('Error fecha inicio y fin')
        st.stop()

    guia_operativa_descripcion = 'GO ' + nombre_evento + ' (' + evento_id + ')'

    #Hiper
    try:                                        #Leemos la solapa que necesitamos
        worksheetL = gs.worksheet('GO Hiper')
    except gspread.exceptions.WorksheetNotFound:
        st.write('No se encontro sheet')
        st.stop()

    #Ponemos todos los datos en un dataframe.
    data = worksheetL.get_all_values()
    hiper = pd.DataFrame(data)
    hiper.columns = ['-', 'ESTIBA', '--', 'ORIN', '---', 'CANT. LOCALES', 'PRECIO REGULAR',
                    'PRECIO', 'DTO', 'PALLET ESPECIAL', '----', '-----', '------']
    hiper = hiper[['ESTIBA', 'ORIN', 'PRECIO', 'PALLET ESPECIAL']].replace('', np.NAN).dropna(subset='ORIN')
    hiper.ffill(inplace=True)
    st.write('')
    st.write('Guías Operativas Hiper:')
    st.dataframe(hiper)
    time.sleep(3)

    #Super
    try:                                        #Leemos la solapa que necesitamos
        worksheetL = gs.worksheet('GO Super')
    except gspread.exceptions.WorksheetNotFound:
        print('No se encontro sheet')

    #Ponemos todos los datos en un dataframe.
    data = worksheetL.get_all_values()
    super = pd.DataFrame(data)
    super.columns = ['-', 'ESTIBA', '--', 'ORIN', '---', 'CANT. LOCALES', 'PRECIO REGULAR',
                    'PRECIO', 'DTO', 'PALLET ESPECIAL', '----', '-----', '------']
    super = super[['ESTIBA', 'ORIN', 'PRECIO', 'PALLET ESPECIAL']].replace('', np.NAN).dropna(subset='ORIN')
    super.ffill(inplace=True)
    st.write('')
    st.write('Guías Operativas Super:')
    st.dataframe(super)
    time.sleep(3)

    #Descargamos datos. Hiper
    query = '''
WITH DIM AS (
SELECT
    ITEM, ROUND(SUPP_PACK_SIZE * TI * HI) AS MEDIAN_UNID_POR_PALLET, ROUND(MEDIAN_UNID_POR_PALLET/4) AS UNIDADES_CARGA,
    ROW_NUMBER() OVER(PARTITION BY ITEM ORDER BY SUPP_PACK_SIZE DESC) AS RN
FROM
MSTRDB.DWH.ITEM_SUPP_COUNTRY
)

SELECT
    CASE WHEN ORIN IN ({orines_snow}) THEN {estibas_snow} END AS GUIA_OPERATIVA, MPH.ARTC_ARTC_COD, MPH.ORIN, MPH.ARTC_ARTC_ID, MPH.ARTC_ARTC_DESC,
    MPH.GEOG_LOCL_COD, MPH.GEOG_LOCL_ID, UNIDADES_CARGA, MEDIAN_UNID_POR_PALLET, F.FORMATO, SUM(FVB.VENTA_BASAL) AS VENTA_BASAL
FROM
    SANDBOX_PLUS.DWH.MAESTRO_PRODUCTOS_HIST AS MPH
JOIN
    DIM
ON
    MPH.ORIN = DIM.ITEM AND DIM.RN = 1 AND DIM.ITEM IN ({orines_snow})
JOIN
    SANDBOX_PLUS.DWH.ESTIBAS_POR_LOCAL AS EPL
ON
    EPL.LOCAL = MPH.GEOG_LOCL_COD AND GUIA_OPERATIVA <= EPL.ESTIBAS AND EPL.ESTIBAS > 0
JOIN
    SANDBOX_PLUS.DWH.FORMATO_LOCALES_CON_LCL_ID F
ON
    F.GEOG_LOCL_ID = MPH.GEOG_LOCL_ID AND F.FORMATO = 'HIPER'
LEFT JOIN
    BIZMETRIKS.DWH.FT_VENTA_BASAL AS FVB
ON
    FVB.ARTC_ARTC_ID = MPH.ARTC_ARTC_ID AND FVB.GEOG_LOCL_ID = MPH.GEOG_LOCL_ID AND FVB.TIEM_DIA_ID >= DATEADD(DAYS, -30, CURRENT_DATE)
WHERE
    MONTH(MPH.MES) = MONTH(CURRENT_DATE - 1) AND YEAR(MPH.MES) = YEAR(CURRENT_DATE - 1)
GROUP BY ALL;
    '''

    df = pd.DataFrame()

    for estiba in hiper['ESTIBA'].unique():
        orin_values_str = "'"
        orin_values = hiper[hiper['ESTIBA']==estiba]['ORIN'].values
        orin_values_str += "', '".join(orin_values)
        orin_values_str += "'"
        print(estiba, orin_values_str)
        cursor.execute(query.format(orines_snow=orin_values_str, estibas_snow=estiba))
        df_aux = cursor.fetch_pandas_all()
        df_aux['UNIDADES_CARGA'] = df_aux['UNIDADES_CARGA'] / orin_values.size
        df_aux['UNIDADES_CARGA'] = df_aux['UNIDADES_CARGA'].astype('int64')
        df = pd.concat([df_aux, df])

    print(df.shape[0])

    df['PROM_FECHA_INICIO'] = prom_fecha_inicio
    df['PROM_FECHA_FIN'] = prom_fecha_fin

    df.sort_values(by='GUIA_OPERATIVA', ascending=True, inplace=True)
    df.rename({'GEOG_LOCL_COD':'LOCAL'}, axis=1, inplace=True)
    df_hiper = df.merge(hiper[['ORIN', 'PRECIO', 'PALLET ESPECIAL']], on = 'ORIN', how = 'inner')
    st.write('')
    st.write('Datos Hiper:')
    st.dataframe(df_hiper)
    time.sleep(3)

    #Super
    query = '''
    WITH DIM AS (
    SELECT
        ITEM, ROUND(SUPP_PACK_SIZE * TI * HI) AS MEDIAN_UNID_POR_PALLET, ROUND(MEDIAN_UNID_POR_PALLET/4) AS UNIDADES_CARGA,
        ROW_NUMBER() OVER(PARTITION BY ITEM ORDER BY SUPP_PACK_SIZE DESC) AS RN
    FROM
    MSTRDB.DWH.ITEM_SUPP_COUNTRY
    )

    SELECT
        CASE WHEN ORIN IN ({orines_snow}) THEN {estibas_snow} END AS GUIA_OPERATIVA, MPH.ARTC_ARTC_COD, MPH.ORIN, MPH.ARTC_ARTC_ID, MPH.ARTC_ARTC_DESC,
        MPH.GEOG_LOCL_COD, MPH.GEOG_LOCL_ID, UNIDADES_CARGA, MEDIAN_UNID_POR_PALLET, F.FORMATO, SUM(FVB.VENTA_BASAL) AS VENTA_BASAL
    FROM
        SANDBOX_PLUS.DWH.MAESTRO_PRODUCTOS_HIST AS MPH
    JOIN
        DIM
    ON
        MPH.ORIN = DIM.ITEM AND DIM.RN = 1 AND DIM.ITEM IN ({orines_snow})
    JOIN
        SANDBOX_PLUS.DWH.ESTIBAS_POR_LOCAL AS EPL
    ON
        EPL.LOCAL = MPH.GEOG_LOCL_COD AND GUIA_OPERATIVA <= EPL.ESTIBAS AND EPL.ESTIBAS > 0
    JOIN
        SANDBOX_PLUS.DWH.FORMATO_LOCALES_CON_LCL_ID F
    ON
        F.GEOG_LOCL_ID = MPH.GEOG_LOCL_ID AND F.FORMATO <> 'HIPER'
    LEFT JOIN
        BIZMETRIKS.DWH.FT_VENTA_BASAL AS FVB
    ON
        FVB.ARTC_ARTC_ID = MPH.ARTC_ARTC_ID AND FVB.GEOG_LOCL_ID = MPH.GEOG_LOCL_ID AND FVB.TIEM_DIA_ID >= DATEADD(DAYS, -30, CURRENT_DATE)
    WHERE
        MONTH(MPH.MES) = MONTH(CURRENT_DATE - 1) AND YEAR(MPH.MES) = YEAR(CURRENT_DATE - 1)
    GROUP BY ALL;
    '''

    df = pd.DataFrame()

    for estiba in super['ESTIBA'].unique():
        orin_values_str = "'"
        orin_values = super[super['ESTIBA']==estiba]['ORIN'].values
        orin_values_str += "', '".join(orin_values)
        orin_values_str += "'"
        print(estiba, orin_values_str)
        cursor.execute(query.format(orines_snow=orin_values_str, estibas_snow=estiba))
        df_aux = cursor.fetch_pandas_all()
        df_aux['UNIDADES_CARGA'] = df_aux['UNIDADES_CARGA'] / orin_values.size
        df_aux['UNIDADES_CARGA'] = df_aux['UNIDADES_CARGA'].astype('int64')
        df = pd.concat([df_aux, df])

    print(df.shape[0])

    df['PROM_FECHA_INICIO'] = prom_fecha_inicio
    df['PROM_FECHA_FIN'] = prom_fecha_fin

    df.sort_values(by='GUIA_OPERATIVA', ascending=True, inplace=True)
    df.rename({'GEOG_LOCL_COD':'LOCAL'}, axis=1, inplace=True)

    df_super = df.merge(super[['ORIN', 'PRECIO', 'PALLET ESPECIAL']], on = 'ORIN', how = 'inner')
    st.write('')
    st.write('Datos Super:')
    st.dataframe(df_super)
    time.sleep(3)

    df = pd.concat([df_hiper, df_super], ignore_index=True)
    df.rename({'PROM_FECHA_INICIO':'INICIO', 'PROM_FECHA_FIN':'FIN', 'ARTC_ARTC_COD':'ESTADISTICO',
            'ORIN':'ITEM', 'ARTC_ARTC_DESC':'DESCRIPCION', 'UNIDADES_CARGA':'EXHIBICION DATA',
            'MEDIAN_UNID_POR_PALLET':'UNID. X PALLET', 'VENTA_BASAL':'CONSUMO'}, axis = 1, inplace = True)
    st.write('')
    st.write('Juntamos y cambiamos nombre a las columnas:')
    st.dataframe(df)
    time.sleep(3)

    #Sumamos regionales
    regional = pd.read_excel('G:/Unidades compartidas/Inteligencia de Negocio/GO/Excels/Nuevas Regiones.xlsx')[['LOCAL', 'REGIONAL', 'GERENTE', 'CORREO']]
    regional['LOCAL'] = regional['LOCAL'].astype(int).astype(str)
    df = df.merge(regional, on = 'LOCAL', how = 'left')
    st.write('')
    st.write('Sumamos datos de los regionales:')
    st.dataframe(df.head())
    time.sleep(3)

    df['EXHIBICION GUIA'] = 'Estiba 0' + df['GUIA_OPERATIVA'].astype(str)

    #Si tienen pallet especial multiplicamos lo calculado
    df.loc[(df['PALLET ESPECIAL']=='SI') & (df['FORMATO']=='HIPER'), 'EXHIBICION DATA'] *= 4

    df.loc[(df['PALLET ESPECIAL']=='NO') & (df['FORMATO']=='HIPER'), 'EXHIBICION DATA'] *= 2

    df.loc[(df['LOCAL']!='167') & (df['PALLET ESPECIAL']=='SI') & (df['FORMATO']=='SUPER'), 'EXHIBICION DATA'] *= 2

    #Guardamos el respaldo
    df.to_excel(f"G:/Unidades compartidas/Inteligencia de Negocio/GO/Respaldo/{guia_operativa_descripcion}.xlsx",
                index = False)
    st.write(f"{guia_operativa_descripcion} - Respaldo generado")
    time.sleep(3)

    #Armamos df de carga
    carga = df[['INICIO', 'FIN', 'LOCAL', 'REGIONAL', 'GERENTE', 'CORREO', 'ESTADISTICO', 'ITEM',
                'DESCRIPCION', 'PRECIO', 'CONSUMO', 'EXHIBICION GUIA', 'UNID. X PALLET', 'EXHIBICION DATA']]
    carga.sort_values(by=['LOCAL', 'ITEM'], inplace=True)
    carga['INICIO'] = carga['INICIO'].dt.strftime('%d/%m/%Y')
    carga['FIN'] = carga['FIN'].dt.strftime('%d/%m/%Y')
    st.write('')
    st.write('Armamos tabla para cargar en drive:')
    st.dataframe(carga)
    time.sleep(3)

    # Open an existing Excel file
    # wb = xw.Book('G:/Unidades compartidas/Inteligencia de Negocio/GO/Excels/GO Base.xlsx')
    # sheet = wb.sheets[0]

    # # Cargo la DataFrame
    # sheet["A2"].options(pd.DataFrame, header = 0, index=False, expand='table').value = carga

    # start_row = carga.shape[0] + 2
    # start_column = 1
    # end_column = 50
    # end_row = sheet.range((start_row, start_column)).end('down').row

    # range_to_clear = sheet.range((start_row, start_column), (end_row, end_column))
    # range_to_clear.clear_contents()

    # st.write('')
    # st.write(guia_operativa_descripcion)

    # wb.save(f"G:/Unidades compartidas/Inteligencia de Negocio/GO/{guia_operativa_descripcion}.xlsx")

    # wb.close()

    # st.write('Listo')

    st.write('')
    st.write('REALIZAR LAS SIGUIENTES MODIFICACIONES MANUALES:')
    st.write('1. Copiar la tabla en GO Base.xlsx, renombrarla como ' + guia_operativa_descripcion +
             ' y cargar en la carpeta compartida https://drive.google.com/drive/folders/1pLSwf3-JAsrxSZekprBL-6tDm6D12P11')
    st.write('2. Abrirlo y guardarlo como Hoja de cálculo de Google')
    st.write('3. Mover excel (.xlsx) a Respaldos Leo. Dejar en GO únicamente la Google sheet')
    st.write('4. Abrir la Google Sheet y bloquear los headers, las columnas en rojo y las columnas en violeta (excepto la de comentarios)')
    st.write('5. Cerrar pestaña y continuar.')
    st.write('')
    st.session_state.flujo = nombre_evento
    st.session_state.carga = carga
else:
    carga = st.session_state.carga
    nombre_evento = st.session_state.flujo

if 'flujo' not in st.session_state:
    st.stop()

if 'go_mails' not in st.session_state:
    cont = st.button('Continuar y preparar mail')
    if cont:
        mails_comp = mails['receivers'] + list(carga['CORREO'].unique())
        st.session_state.go_mails = mails_comp
    else:
        st.stop()

#Definimos datos para enviar mail
try:
    if 'go_mail' not in st.session_state:
        go_mail = st.text_input('Ingrese su mail:')
        if go_mail == '':
            st.stop()
        else:
            st.session_state.go_mail = go_mail
    else:
        go_mail = st.session_state.go_mail

    if 'go_day' not in st.session_state:
        go_day = st.text_input('Ingrese día de la semana de fin de edición del drive:')
        if go_day == '':
            st.stop()
        else:
            st.session_state.go_day = go_day
    else:
        go_day = st.session_state.go_day

    if 'go_date' not in st.session_state:
        go_date = st.text_input('Ingrese fecha de fin de edición del drive:')
        if go_date == '':
            st.stop()
        else:
            st.session_state.go_date = go_date
    else:
        go_date = st.session_state.go_date

    if 'go_time' not in st.session_state:
        go_time = st.text_input('Ingrese hora de fin de edición del drive:')
        if go_time == '':
            st.stop()
        else:
            st.session_state.go_time = go_time
    else:
        go_time = st.session_state.go_time
except:
    st.write('Aún no se ingresaron datos para enviar el email')
    st.stop()

mail = st.button('Enviar mail')
if mail:
    enviar_email(sender=go_mail, receiver=st.session_state.go_mails,
                 subject='Guias Operativas ' + nombre_evento, files=[],
                 body=mails['body'].format(evento=nombre_evento, dia=go_day,
                                           fecha=go_date, hora=go_time))
    st.write('')
    st.write('Mail enviado')
    st.write('')
    st.write('Dar permisos a los siguientes mails:')
    for m in st.session_state.go_mails:
        st.write(m)