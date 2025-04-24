import pandas as pd
from .utils import insert_dataframe_into_sheet, get_credentials_drive
from .querys import reporte_articulos
from datetime import datetime
from config import *
import warnings
pd.options.mode.chained_assignment = None
warnings.simplefilter(action='ignore', category=FutureWarning)

def cargar_reporte_articulos(cursor):

    print('')
    print('Inicia carga_reporte_articulos')

    # DataFrames
    cursor.execute(reporte_articulos)
    df_reporte_articulos = cursor.fetch_pandas_all()
    df_reporte_articulos['EVENTO_ID'] = df_reporte_articulos['EVENTO_ID'].astype(str)

    #excel_eventos = pd.read_excel('T:\\BI\\Comercial\\ofertas\\Base ofertas para BI.xlsx', sheet_name='Eventos').astype('str')
    excel_eventos = pd.read_excel(excels['excel_BI'], sheet_name='Eventos').astype(
        'str')

    df_reporte_articulos = df_reporte_articulos.merge(
        excel_eventos[['evento_id', 'evento_desc']],
        left_on = 'EVENTO_ID',
        right_on = 'evento_id',
        how = 'left'
    )

    df_reporte_articulos.drop(['evento_id'], axis=1, inplace=True)

    df_reporte_articulos.rename({
        'EVENTO_ID':'Evento ID',
        'PROM_FECHA_INICIO':'Desde',
        'PROM_FECHA_FIN':'Hasta',
        'GEOG_LOCL_COD':'Codigo Local',
        'GEOG_LOCL_DESC':'Local',
        'ORIN':'ORIN',
        'ARTC_ARTC_COD':'Estadistico',
        'ARTC_ARTC_DESC':'Articulo',
        'evento_desc':'Evento'
    }, axis=1, inplace=True)

    df_reporte_articulos = df_reporte_articulos[[
        'Evento',
        'Evento ID',
        'Desde',
        'Hasta',
        'Codigo Local',
        'Local',
        'ORIN',
        'Estadistico',
        'Articulo'
    ]]

    df_reporte_articulos.sort_values(by='ORIN', inplace=True)

    # Google Sheets

    drive_credentials=get_credentials_drive()

    # Unidad Inteligencia de Negocio
    #url = 'https://docs.google.com/spreadsheets/d/1hqkPO6ych3MT3oJVFUkk9nBEvyhIcARPaVF86uFMPJ0/edit#gid=0'
    #url = 'https://docs.google.com/spreadsheets/d/1w5oGifgavq1GodX3z9t9UF8ALeOl6NzMWiu_4xPG8FE/edit?usp=sharing'

    #url = 'https://docs.google.com/spreadsheets/d/1w3Mmu1W_OVn_NOtZ0YE2xRSAqIYgcfYz57mjjkrJbRc/edit?gid=0#gid=0'
    url = urls['drive_reporte_articulos']

    spreadsheet_id = url.split('/')[-2]

    insert_dataframe_into_sheet(
        dataframe = df_reporte_articulos,
        spreadsheet_id = spreadsheet_id,
        credentials = drive_credentials,
        sheet_name = 'Info')

    print('Termina carga_reporte_articulos')
