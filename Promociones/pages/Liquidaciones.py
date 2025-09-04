import re
from utils import snowflake_login
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from config import *
import streamlit as st
import time
from datetime import datetime

pd.options.mode.chained_assignment = None

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

def price (s):
    #Primero saco las , y . del registro. Si son de decimales quito los decimales
    e = re.sub('[^0-9xX,.]', '', str(s))
    if len(e)>2:
        if e[-3] == ',' or e[-2] == ',':
            e2 = ''.join(e.split(',')[:-1]).replace(',','').replace('.','')
        elif e[-3] == '.' or e[-2] == '.':
            e2 = ''.join(e.split('.')[:-1]).replace(',','').replace('.','')
        else:
            e2 = e.replace(',','').replace('.','')
        #Luego saco las 'x' de 2x99 por ejemplo. El PVP va a ser 44 en nuestro ejemplo
        if e2.lower().find('x') != -1:
            e3 = int(e2.lower().split('x')[0])
            e4 = int(e2.lower().split('x')[1])
            ef = int(e4/e3)
        else:
            ef = int(e2)
    else:
        try:
            ef = int(e)
        except:
            ef = 0
    return ef

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

query = '''
            SELECT
                ORIN,
                ARTC_ARTC_COD
            FROM
                MSTRDB.DWH.LU_ARTC_ARTICULO
            '''
cursor.execute(query)
articulos = cursor.fetch_pandas_all()

#URL de liquidaciones fija
url = 'https://docs.google.com/spreadsheets/d/1b_-2I_0UnSBh3teUpfIBTkxy-gooQX7eKl7m4lEdEUg/edit?'

codigo_url = url.split('/')[-2]     #Obtenemos el codigo del drive

gs = gc.open_by_key(codigo_url)     #Leemos

if 'sheet' not in st.session_state:
    sheet = st.text_input('Ingrese el nombre de la sheet:')
    try:
        worksheetL = gs.worksheet(sheet)        #Leemos la sheet
        st.session_state.sheet = sheet
    except:
        st.write('Aún no se ingresó una sheet válida')
        st.stop()
else:
    sheet = st.session_state.sheet

worksheetL = gs.worksheet(sheet)        #Leemos la sheet
#Tomamos los datos  los mostramos
data = worksheetL.get_all_values()
df = pd.DataFrame(data[1:], columns=['Local', 'Nombre Local', 'Dept', 'Group', 'CLASS_NAME', 'SUB_NAME', 'ORIN',
                                     'Descripción', 'PVP REGULAR', 'PVP LIQUIDACION', 'DESCUENTO', 'MONEDA',
                                     'FORMA DE LIQUIDAR', 'STOCK 30/06'])
st.write('Tabla seleccionada:')
st.dataframe(df)

fini = datetime.today().replace(day=int(sheet.split()[1].split('/')[0]),
                                month=int(sheet.split()[1].split('/')[1])).date().strftime(format='%d/%m/%y')
ffin = datetime.today().replace(day=int(sheet.split()[-1].split('/')[0]),
                                month=int(sheet.split()[-1].split('/')[1])).date().strftime(format='%d/%m/%y')
st.write('')
st.write('Controlar fecha de inicio y fin de liquidación')
st.write('Fecha inicio de liquidación: ' + fini)
st.write('Fecha fin de liquidación: ' + ffin)

if 'nev' not in st.session_state:
    nev = st.text_input('Ingrese el número de evento:')
    try:
        if len(nev) >= 4:
            nev = int(nev)
            st.session_state.nev = nev
    except:
        st.write('Aún no se ingresó un número de evento válido')
        st.stop()
else:
    nev = st.session_state.nev

if 'nev' not in st.session_state:
    st.stop()
    
st.write('')
st.write('Le damos formato a la tabla')
df2 = df.iloc[:,[0, 6, 9]].drop_duplicates().merge(articulos)
df2.columns = ['GEOG_LOCL_COD', 'ORIN', 'PROM_PVP_OFERTA', 'ARTC_ARTC_COD']
df2['PROM_FECHA_INICIO'] = fini
df2['PROM_FECHA_FIN'] = ffin
df2['EVENTO_ID'] = nev
df2['PRONOSTICO_VENTA'] = 0
df2['STOCK_INICIAL_PROMO'] = 0
df2['PROM_LOCAL_ACTIVO'] = 0
df2['PROM_ESTIBA'] = 0
df_liq = df2[['PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'ARTC_ARTC_COD', 'EVENTO_ID', 'PRONOSTICO_VENTA',
                'STOCK_INICIAL_PROMO', 'GEOG_LOCL_COD', 'PROM_PVP_OFERTA', 'PROM_LOCAL_ACTIVO',
                'PROM_ESTIBA']].astype({'GEOG_LOCL_COD':'str', 'ARTC_ARTC_COD':'str'})
df_liq['PROM_FECHA_INICIO'] = pd.to_datetime(df_liq['PROM_FECHA_INICIO'], dayfirst=True)
df_liq['PROM_FECHA_FIN'] = pd.to_datetime(df_liq['PROM_FECHA_FIN'], dayfirst=True)
df_liq['PROM_PVP_OFERTA'] = df_liq['PROM_PVP_OFERTA'].apply(price)
st.dataframe(df_liq)
try:
    df_liq.to_excel('G:/Unidades compartidas/Inteligencia de Negocio/Promos/Liquidaciones/Cargar/' +
                    sheet.replace('/', '-') + '.xlsx', index=False)
except:
    st.write('Descargar la tabla y guardarla en G:/Unidades compartidas/Inteligencia de Negocio/Promos/Liquidaciones/Cargar')