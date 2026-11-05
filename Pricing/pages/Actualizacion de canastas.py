import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, carga_snow_generic, get_credentials, snowflake_login

st.set_page_config(page_title="Canastas", page_icon="🧺", layout="wide")

def normDep (d:str):
    return d.upper().strip().replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')

st.title("🧺 Actualización de canastas")
st.divider()

# Conexión a Snowflake
credentials_snowflake = get_credentials("snow")

try:
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login(
            user=credentials_snowflake['USER'],
            password=credentials_snowflake['PASS'],
            account=credentials_snowflake['ACCOUNT']
        )
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        snow = st.session_state.snow
        user = st.session_state.user
        cursor = st.session_state.cursor
except:
    st.error('Error de conexión. Verificá las credenciales de Snowflake.')
    st.stop()

prog = st.selectbox('Seleccione el programa:', ['Actualización de canastas', 'Descarga de canastas',
                                                'Consulta items sin canasta'])

if prog == 'Descarga de canastas':
    with st.spinner('Descargando datos...'):
        cursor.execute(f'''
        SELECT
            DISTINCT G.GERENCIA, G.GROUP_NAME, D.DEPT_NAME, CL.CLASS_NAME, SC.SUB_NAME,
            IF.ITEM_PARENT AS FAMILIA, IF.HNO AS HERMANO, C.ITEM, LAA.ARTC_ARTC_DESC, MO.MG, C.CANASTA
        FROM SPIKE.SPIKE.CANASTAS C
        JOIN MSTRDB.DWH.LU_ARTC_ARTICULO LAA ON C.ITEM::TEXT = LAA.ORIN
        JOIN MSTRDB.DWH.ITEM_MASTER IM ON LAA.ORIN = IM.ITEM AND IM.ITEM_NUMBER_TYPE = 'ITEM'
        JOIN MSTRDB.DWH.DEPS D ON IM.DEPT = D.DEPT
        JOIN SANDBOX_PLUS.DWH.GERENCIAS G ON D.GROUP_NO = G.GROUP_NO
        JOIN MSTRDB.DWH.CLASS CL ON IM.DEPT = CL.DEPT AND IM.CLASS = CL.CLASS
        JOIN MSTRDB.DWH.SUBCLASS SC ON IM.DEPT = SC.DEPT AND IM.CLASS = SC.CLASS AND IM.SUBCLASS = SC.SUBCLASS
        JOIN MSTRDB.DWH.LU_GEOG_LOCAL LGL ON C.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
        LEFT JOIN BIZMETRIKS.DWH.ITEM_FAMILIAS IF ON IF.ITEM = LAA.ORIN
        LEFT JOIN SANDBOX_PLUS.DWH.INPUT_PRICING_MARGEN_OBJETIVO MO ON LAA.ORIN = MO.ORIN;''')
        desc = cursor.fetch_pandas_all().astype({'ITEM': 'str'})
    st.metric("Registros encontrados", len(desc))
    st.dataframe(desc.head(), use_container_width=True)
    csv = desc.to_csv(index=False)
    st.download_button(label='⬇️ Descargar tabla', data=csv, file_name='Canastas.csv',
                    mime='text/csv', use_container_width=False)
    
elif prog == 'Actualización de canastas':
    st.info('Arrastrá el archivo Excel con las columnas **[ITEM, CANASTA]**.')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()

    try:
        canastas = pd.read_excel(uploaded_file)
        canastas.columns = canastas.columns.str.upper()
        canastas = canastas[['ITEM', 'CANASTA']].astype({'ITEM':'str'})
        canastas['CANASTA'] = canastas['CANASTA'].str.lower()
    except:
        st.error('El archivo no tiene el formato correcto.')
        st.stop()

    with st.spinner('Descargando locales...'):
        cursor.execute('SELECT GEOG_LOCL_ID FROM SANDBOX_PLUS.DWH.FORMATO_LOCALES_CON_LCL_ID;')
        locales = cursor.fetch_pandas_all()

    canastas['aux'] = 1
    locales['aux'] = 1
    carga = canastas[canastas['CANASTA'].isin(['referente', 'mercado', 'surtido', 'excluido',
                                               'marca propia'])].merge(locales).drop(columns='aux')
    st.error('Los siguientes item no tienen una canasta aceptable.  No van a cargarse.')
    st.dataframe(canastas[~canastas['CANASTA'].isin(['referente', 'mercado', 'surtido', 'excluido',
                                                     'marca propia'])].drop(columns='aux'))
    if st.button('Cargar'):
        with st.spinner('Borrando registros...'):
            try:
                query = 'DELETE FROM SPIKE.SPIKE.CANASTAS WHERE ITEM IN {items}'
                cursor.execute(query.format(items=tuple(carga['ITEM'].unique())))
                st.success('Registros borrados correctamente.')
            except:
                st.error('Hubo problemas para borrar los registros. Contactar con el área de Data.')
                st.stop()
        
        st.divider()
        with st.spinner('Subiendo nuevos datos...'):
            try:
                carga_snow_generic(df=carga, ctx=snow, database='SPIKE', schema='SPIKE', table='CANASTAS')
                st.success('Registros subidos correctamente.')
            except:
                st.error('Hubo problemas para subir los registros. Contactar con el área de Data.')
                st.stop()

        st.divider()
        st.success("Proceso completado.")

elif prog == 'Consulta items sin canasta':
    with st.spinner('Descargando datos...'):
        cursor.execute('''
SELECT DISTINCT LAA.ORIN, MO.MG, F.FAMILIA FROM MSTRDB.DWH.LU_ARTC_ARTICULO LAA
JOIN MSTRDB.DWH.ITEM_MASTER IM ON IM.ITEM = LAA.ORIN AND IM.ITEM_NUMBER_TYPE = 'ITEM'
JOIN MSTRDB.DWH.DEPS DD ON DD.DEPT = IM.DEPT AND DD.GROUP_NO IN (1, 3, 4, 6, 9, 15, 18)
JOIN MSTRDB.DWH.FT_STOCK FS ON FS.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID AND FS.TIEM_DIA_ID = CURRENT_DATE() - 1 AND FS.ARTC_ESTA_ID = 4
LEFT JOIN SANDBOX_PLUS.DWH.INPUT_PRICING_MARGEN_OBJETIVO MO ON LAA.ORIN = MO.ORIN
LEFT JOIN SANDBOX_PLUS.DWH.RELACIONES_ITEM_PARENT_ACTUALIZADOS F ON LAA.ORIN = F.ITEM
WHERE LAA.ORIN NOT IN (SELECT DISTINCT ITEM::TEXT FROM SPIKE.SPIKE.CANASTAS);''')
        df = cursor.fetch_pandas_all()
    st.metric("Registros encontrados", len(df))
    st.dataframe(df.head(), use_container_width=True)
    csv = df.to_csv(index=False, sep=';')
    st.download_button(label='⬇️ Descargar tabla', data=csv, file_name='Items sin canastas.csv',
                       mime='text/csv', use_container_width=False)