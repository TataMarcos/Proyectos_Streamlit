import pandas as pd
from utils import snowflake_login, get_credentials
import streamlit as st
from datetime import datetime, timedelta

#Conectamos a snowflake
credentials_snowflake = get_credentials("snow")

try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login(
                                    user = credentials_snowflake['USER'],
                                    password = credentials_snowflake['PASS'],
                                    account = credentials_snowflake['ACCOUNT']
                                    )
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

fechas = []
for i in range((datetime.today().date() - datetime.today().replace(year=datetime.today().date().year - 1).date()).days):
    fechas.append((datetime.today().replace(year=datetime.today().date().year - 1).date()
                   + timedelta(days=i)).strftime('%Y-%m-%d'))
fechas.reverse()

fecha = st.selectbox('Seleccionar fecha de inicio:', options=fechas)

st.info('Arrastrá el archivo Excel con las columnas: **[LOCAL, ITEM]**')
uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

if uploaded_file is None:
    st.stop()

cons = pd.read_excel(uploaded_file)
cons.columns = cons.columns.str.upper()
cons.dropna(subset=['ITEM', 'LOCAL'], inplace=True)
cons['ITEM'] = cons['ITEM'].astype('int64').astype('str')
cons['LOCAL'] = cons['LOCAL'].astype('int64')

with st.spinner('Consultando en Snowflake...'):
    try:
        cursor.execute(f'''
SELECT 
    LG.GEOG_LOCL_COD AS LOCAL, LAA.ORIN AS ITEM, LAA.ARTC_ARTC_DESC, ROUND(SUM(FT.VNTA_IMPORTE_SIN_IVA), 2) AS VENTA, ROUND(SUM(FT.VNTA_UNIDADES), 2) AS VENTA_UNID,
    ROUND(SUM(FT.VNTA_UNIDADES * COALESCE(FT.VNTA_COSTO_PROM_POND, 0)), 2) AS COSTO, ROUND(VENTA - COSTO, 2) AS GB1, ROUND(DIV0(GB1, VENTA), 4) AS MARGEN,
    ROUND(VENTA - GB1, 2) AS CPP_VENDIDO, ROUND(FS.STCK_UNIDADES * FC.UNIT_COST, 2) AS CPP_ACTUAL, LA.ARTC_ESTA_DESC
FROM
    MSTRDB.DWH.FT_VENTAS AS FT
JOIN
    MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON LAA.ARTC_ARTC_ID = FT.ARTC_ARTC_ID
JOIN
    MSTRDB.DWH.LU_GEOG_LOCAL AS LG ON FT.GEOG_LOCL_ID = LG.GEOG_LOCL_ID
LEFT JOIN
    MSTRDB.DWH.FT_STOCK AS FS ON FS.ARTC_ARTC_ID = FT.ARTC_ARTC_ID AND FS.GEOG_LOCL_ID = FT.GEOG_LOCL_ID AND FS.TIEM_DIA_ID = CURRENT_DATE() - 1
LEFT JOIN
    MSTRDB.DWH.LU_ARTC_ESTADO_ARTICULO LA ON LA.ARTC_ESTA_ID = FS.ARTC_ESTA_ID
LEFT JOIN
    MSTRDB.DWH.FT_COSTO_UNITARIO_RMS AS FC ON FC.ARTC_ARTC_ID = FT.ARTC_ARTC_ID AND FC.GEOG_LOCL_ID = FT.GEOG_LOCL_ID AND FC.TIEM_DIA_ID = CURRENT_DATE() - 1
WHERE
    FT.TIEM_DIA_ID BETWEEN '{fecha}' AND CURRENT_DATE - 1
AND
    LAA.ORIN IN ('{"', '".join(str(l) for l in cons['ITEM'].unique())}')
AND
    LG.GEOG_LOCL_COD IN ({','.join(str(l) for l in cons['LOCAL'].unique())})
GROUP BY ALL;
''')
        df = cursor.fetch_pandas_all()
        df['ITEM'] = df['ITEM'].astype('int64').astype('str')
        df['LOCAL'] = df['LOCAL'].astype('int64')
        df_final = df.merge(cons)

        csv = df_final.to_csv(index=False, sep=';', decimal=',')
        st.dataframe(df_final.head(10), use_container_width=True)
        st.download_button(label='⬇️ Descargar tabla', data=csv, file_name='Consulta.csv', mime='text/csv')
    except Exception as e:
        st.error(f'El archivo tiene un formato erróneo. Verificá las columnas LOCAL e ITEM. {e}')