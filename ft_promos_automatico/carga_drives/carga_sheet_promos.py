import pandas as pd
from .utils import *
from .querys import *
from datetime import datetime
from config import *
import warnings
pd.options.mode.chained_assignment = None
warnings.simplefilter(action='ignore', category=FutureWarning)

def cargar_sheet_promos(cursor):

    print('')
    print('Inicia carga_sheet_promos')

    # DataFrames

    # 1. Precios Oferta
    cursor.execute(precios_oferta_query)
    df_precios_oferta = cursor.fetch_pandas_all()

    df_precios_oferta['FIN'] = df_precios_oferta['FIN'].astype(str)
    df_precios_oferta['INICIO'] = df_precios_oferta['INICIO'].astype(str)
    df_precios_oferta['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 2. Precios Stock Mediano dia de ayer
    # cursor.execute(precios_stock_mediano_query)
    # df_precios_stock_mediano = cursor.fetch_pandas_all()
    # df_precios_stock_mediano['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 3. OPT
    cursor.execute(opt_query)
    df_opt = cursor.fetch_pandas_all()
    df_opt['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 4. Locales Activos Ayer

    cursor.execute(locales_activos_ayer_query)
    df_locales_activos_ayer = cursor.fetch_pandas_all()
    df_locales_activos_ayer['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 5. Days on Hand

    # Parte A - Articulos
    cursor.execute(days_on_hand_articulo_query)
    df_days_on_hand_articulo = cursor.fetch_pandas_all()

    df_days_on_hand_articulo = df_days_on_hand_articulo[df_days_on_hand_articulo['UNIDADES'] != 0]
    df_days_on_hand_articulo['DAYS ON HAND'] = df_days_on_hand_articulo['UNIDADES'] / df_days_on_hand_articulo[
        'UNIDADES_VENDIDAS']
    df_days_on_hand_articulo['DAYS ON HAND'][
        (df_days_on_hand_articulo['UNIDADES'] == 0) & (df_days_on_hand_articulo['UNIDADES_VENDIDAS'] == 0)] = 0
    df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] == np.inf] = 999999
    df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] == -np.inf] = -999999
    df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] > 999999] = 999999
    df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] < -999999] = -999999
    df_days_on_hand_articulo['DAYS ON HAND'].fillna(999999, inplace=True)
    df_days_on_hand_articulo['DAYS ON HAND'] = round(df_days_on_hand_articulo['DAYS ON HAND'], 0).astype(int)
    df_days_on_hand_articulo['UNIDADES'] = round(df_days_on_hand_articulo['UNIDADES'], 0).astype(int)
    df_days_on_hand_articulo['UNIDADES_VENDIDAS'] = round(df_days_on_hand_articulo['UNIDADES_VENDIDAS'], 0).astype(int)
    df_days_on_hand_articulo.rename(
        {
            'UNIDADES_VENDIDAS': 'UNIDADES VENDIDAS',
            'DAYS ON HAND': 'DAYS ON HAND ARTICULO'
        },
        axis=1, inplace=True)
    df_days_on_hand_articulo = df_days_on_hand_articulo[
        ['SUBCLASE', 'ORIN', 'UNIDADES', 'UNIDADES VENDIDAS', 'DAYS ON HAND ARTICULO']]

    df_days_on_hand_articulo = df_days_on_hand_articulo[df_days_on_hand_articulo['DAYS ON HAND ARTICULO'] != 999999]
    df_days_on_hand_articulo = df_days_on_hand_articulo[df_days_on_hand_articulo['DAYS ON HAND ARTICULO'] != -999999]

    # Parte B - Subclases
    cursor.execute(days_on_hand_subclase_query)
    df_days_on_hand_subclase = cursor.fetch_pandas_all()

    df_days_on_hand_subclase = df_days_on_hand_subclase[df_days_on_hand_subclase['UNIDADES'] != 0]
    df_days_on_hand_subclase['DAYS ON HAND'] = df_days_on_hand_subclase['UNIDADES'] / df_days_on_hand_subclase[
        'UNIDADES_VENDIDAS']
    df_days_on_hand_subclase['DAYS ON HAND'][
        (df_days_on_hand_subclase['UNIDADES'] == 0) & (df_days_on_hand_subclase['UNIDADES_VENDIDAS'] == 0)] = 0
    df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] == np.inf] = 999999
    df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] == -np.inf] = -999999
    df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] > 999999] = 999999
    df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] < -999999] = -999999
    df_days_on_hand_subclase['DAYS ON HAND'].fillna(999999, inplace=True)
    df_days_on_hand_subclase['DAYS ON HAND'] = round(df_days_on_hand_subclase['DAYS ON HAND'], 0).astype(int)
    df_days_on_hand_subclase['UNIDADES'] = round(df_days_on_hand_subclase['UNIDADES'], 0).astype(int)
    df_days_on_hand_subclase['UNIDADES_VENDIDAS'] = round(df_days_on_hand_subclase['UNIDADES_VENDIDAS'], 0).astype(int)
    df_days_on_hand_subclase.rename(
        {
            'UNIDADES_VENDIDAS': 'UNIDADES VENDIDAS',
            'DAYS ON HAND': 'DAYS ON HAND SUBCLASE'
        },
        axis=1, inplace=True)
    df_days_on_hand_subclase = df_days_on_hand_subclase[
        ['SUBCLASE', 'UNIDADES', 'UNIDADES VENDIDAS', 'DAYS ON HAND SUBCLASE']]

    # Parte C - Consolido en Articulos

    df_days_on_hand_articulo = df_days_on_hand_articulo.merge(
        df_days_on_hand_subclase[['SUBCLASE', 'DAYS ON HAND SUBCLASE']],
        on='SUBCLASE',
        how='left')

    df_days_on_hand_articulo['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 6. Top 5 articulos en Subclase

    cursor.execute(top_5_query)
    top = cursor.fetch_pandas_all()

    top['Z_Score'] = top.groupby('ORIN')['GB1'].transform(lambda x: (x - x.mean()) / x.std())
    top['Outlier'] = np.where((top['Z_Score'] > 3) | (top['Z_Score'] < -3), True, False)
    top = top[top['Outlier'] == False]
    top_2 = top.groupby(['SUBCLASE', 'ORIN', 'ARTC_ARTC_DESC'])['GB1'].sum().reset_index()
    top_2 = top_2[top_2['GB1'] > 0]
    top_2 = top_2[~top_2['ORIN'].isin(['-1', ''])]
    top_2['R'] = top_2.groupby(['SUBCLASE'])['GB1'].rank(method='min', ascending=False)
    top_2 = top_2[top_2['R'] <= 5]
    top_3 = top_2[['ORIN', 'ARTC_ARTC_DESC']].drop_duplicates()
    top_3['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 7. Unidades diarias REGULARES - ultimos 30

    cursor.execute(unidades_regulares_query)
    un_regular = cursor.fetch_pandas_all()

    un_regular['Z_Score'] = un_regular.groupby('ORIN')['UNIDADES'].transform(lambda x: (x - x.mean()) / x.std())
    un_regular['Outlier'] = np.where((un_regular['Z_Score'] > 3) | (un_regular['Z_Score'] < -3), True, False)
    un_regular = un_regular[un_regular['Outlier'] == False]
    un_regular_2 = un_regular.groupby(['ORIN', 'ARTC_ARTC_DESC', 'TIEM_DIA_ID'])['UNIDADES'].sum().reset_index()
    un_regular_2 = un_regular_2.groupby(['ORIN', 'ARTC_ARTC_DESC'])['UNIDADES'].median().reset_index()
    un_regular_2.rename({'UNIDADES': 'UNIDADES PROMEDIO'}, axis=1, inplace=True)
    un_regular_2 = un_regular_2[un_regular_2['UNIDADES PROMEDIO'] > 0]
    un_regular_2 = un_regular_2[~un_regular_2['ORIN'].isin(['-1', ''])]
    un_regular_2['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 8. Unidades diarias PROMO - en ultimas 3 promos (siempre que el inicio < current_date)

    cursor.execute(unidades_promo_query_1)
    un_promo_1 = cursor.fetch_pandas_all()

    un_promo_1.head(2)

    un_promo_1['ORIN'].value_counts()

    un_promo_1['R'] = un_promo_1.groupby('ORIN')['PROM_FECHA_FIN'].rank(method='min', ascending=False)
    un_promo_1 = un_promo_1[un_promo_1['R'] <= 3]
    un_promo_1[un_promo_1['ORIN'] == '1000385297']

    cursor.execute(unidades_promo_query_2)
    un_promo_2 = cursor.fetch_pandas_all()

    un_promo_2.head(2)

    un_promo_merged = un_promo_1.merge(un_promo_2, on=['ORIN'], how='left')

    for column in ['PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'TIEM_DIA_ID']:
        un_promo_merged[column] = pd.to_datetime(un_promo_merged[column])

    un_promo_merged = un_promo_merged[
        (un_promo_merged['TIEM_DIA_ID'] >= un_promo_merged['PROM_FECHA_INICIO'])
        &
        (un_promo_merged['TIEM_DIA_ID'] <= un_promo_merged['PROM_FECHA_FIN'])
        ]

    un_promo_merged[un_promo_merged['ORIN'] == '1000385297']

    un_promo_merged_2 = un_promo_merged.groupby(['ORIN', 'ARTC_ARTC_DESC'])['UNIDADES'].median().reset_index()
    un_promo_merged_2[un_promo_merged_2['ORIN'] == '1000385297']
    un_promo_merged_2['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 9. FT Stock ayer: stock y precio de lista

    cursor.execute(ft_stock_ayer_query)
    stock_ayer = cursor.fetch_pandas_all()
    stock_ayer['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 10. Pack Size

    cursor.execute(pack_size)
    pack_size_df = cursor.fetch_pandas_all()
    pack_size_df['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 11. Aceleracion

    cursor.execute(aceleracion)
    aceleracion_df = cursor.fetch_pandas_all()

    aceleracion_df = aceleracion_df[['ORIN', 'ACELERACION', 'EVENTO_DESC', 'PROM_FECHA_FIN']].drop_duplicates()
    aceleracion_df = aceleracion_df[~aceleracion_df['EVENTO_DESC'].str.lower().str.contains(pat='Liquidac', case=False)]
    aceleracion_df = aceleracion_df[~aceleracion_df['ACELERACION'].isna()]
    aceleracion_df['PROM_FECHA_FIN'] = pd.to_datetime(aceleracion_df['PROM_FECHA_FIN'])

    aceleracion_df['R'] = aceleracion_df.groupby('ORIN')['PROM_FECHA_FIN'].rank(method='min', ascending=False)
    aceleracion_df = aceleracion_df[aceleracion_df['R'] <= 3]

    aceleracion_grouped = aceleracion_df.groupby(['ORIN'])['ACELERACION'].median().reset_index()
    aceleracion_grouped['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    #print('OK - dataframes generadas')
    #print('Comienza a cargar las sheets')

    # Google Sheets

    drive_credentials=get_credentials_drive()

    # Unidad Inteligencia de Negocio
    #url = 'https://docs.google.com/spreadsheets/d/1hqkPO6ych3MT3oJVFUkk9nBEvyhIcARPaVF86uFMPJ0/edit#gid=0'
    #url = 'https://docs.google.com/spreadsheets/d/1w5oGifgavq1GodX3z9t9UF8ALeOl6NzMWiu_4xPG8FE/edit?usp=sharing'


    #url = 'https://docs.google.com/spreadsheets/d/1w5oGifgavq1GodX3z9t9UF8ALeOl6NzMWiu_4xPG8FE/edit'
    url = urls['drive_sheets_promos']

    spreadsheet_id = url.split('/')[-2]

    # Create a Google Sheets API service --> esto se usa en caso de usar la funcion 2
    service = build('sheets', 'v4', credentials=drive_credentials)

    # Create sheets and insert the DataFrames

    dataframes_dict = {
        'df_precios_oferta': 'Precios Oferta',
        'df_opt': 'OPT',
        'df_locales_activos_ayer': 'Locales Activos Ayer',
        'df_days_on_hand_articulo': 'Days on Hand - Articulos',
        'top_3': 'Top 5 articulos',
        'un_regular_2': 'Venta Regular - Unidades Promedio',
        'un_promo_merged_2': 'Venta Promo - Unidades Promedio',
        'stock_ayer': 'Stock Ayer',
        'pack_size_df': 'Pack Size',
        'aceleracion_grouped': 'Aceleracion'
    }

    for df_name, sheet_name in dataframes_dict.items():
        #print(sheet_name)

        # Elimino la primera sheet
        delete_first_sheet(spreadsheet_id, drive_credentials)

        df = locals()[df_name]  # Retrieve the DataFrame using its name
        #print(f"Procesando sheet: {sheet_name}")

        # Inserto la DataFrame en una nueva sheet, que ocupa la ultima posicion
        insert_dataframe_into_sheet(df, spreadsheet_id, drive_credentials, sheet_name)

    print('Termina carga_sheet_promos')
