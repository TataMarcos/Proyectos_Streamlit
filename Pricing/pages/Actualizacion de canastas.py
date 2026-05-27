import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, carga_snow_generic, get_credentials, snowflake_login

st.set_page_config(page_title="Canastas", page_icon="🧺", layout="wide")

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

prog = st.selectbox('Seleccione el programa:', ['Referente', 'Mercado + Surtido', 'Descarga de canastas'])

if prog == 'Descarga de canastas':
    c = st.selectbox('Seleccione la canasta:', ['referente', 'mercado', 'surtido', 'excluido', 'marca propia'])
    with st.spinner('Descargando datos...'):
        cursor.execute(f'''
            SELECT LGL.GEOG_LOCL_COD, C.ITEM, LAA.ARTC_ARTC_DESC, C.CANASTA
            FROM SPIKE.SPIKE.CANASTAS C
            JOIN MSTRDB.DWH.LU_ARTC_ARTICULO LAA ON C.ITEM::TEXT = LAA.ORIN
            JOIN MSTRDB.DWH.LU_GEOG_LOCAL LGL ON C.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
            WHERE C.CANASTA = \'{c}\';''')
        desc = cursor.fetch_pandas_all().astype({'ITEM': 'str'})
    st.metric("Registros encontrados", len(desc))
    st.dataframe(desc.head(), use_container_width=True)
    csv = desc.to_csv(index=False)
    st.download_button(label='⬇️ Descargar tabla', data=csv, file_name='Canastas.csv',
                       mime='text/csv', use_container_width=False)

elif prog == 'Referente':
    st.info('Arrastrá el archivo Excel con el formato establecido.')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()

    try:
        referentes = pd.read_excel(uploaded_file).melt(
            id_vars='ORIN',
            value_vars=pd.read_excel(uploaded_file).columns[1:],
            var_name='GEOG_DPTO_DESC',
            value_name='REFERENTE'
        ).rename(columns={'ORIN': 'ITEM'})
        referentes = referentes[referentes['REFERENTE'] == 1].drop(columns='REFERENTE')
    except:
        st.error('El archivo no tiene el formato correcto.')
        st.stop()

    if 'ref' not in st.session_state:
        with st.spinner('Descargando datos actuales desde Snowflake...'):
            locales = descargar_segmento(cursor, 'LOCALES')
            ref = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'referente'"])
            merca = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'mercado'"])
            surti = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'surtido'"])
            exclu = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'excluido'"])
            mp = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'marca propia'"])

        referentes['aux'] = 1
        ref['aux'] = 1
        locales['aux'] = 1

        carga = referentes.merge(locales).drop(columns=['aux', 'GEOG_DPTO_DESC'])
        carga['CANASTA'] = 'referente'

        mercado1 = ref[~ref['ITEM'].isin(referentes['ITEM'])].drop(columns='GEOG_LOCL_ID').drop_duplicates().merge(locales)[ref.columns].drop(columns='aux')
        mercado2 = locales.drop(columns='GEOG_DPTO_DESC').merge(referentes.drop(columns='GEOG_DPTO_DESC').drop_duplicates()).drop(columns='aux').merge(carga, how='left', indicator=True)
        mercado = pd.concat([mercado1, mercado2[mercado2['_merge'] != 'both'].drop(columns='_merge')], ignore_index=True)
        mercado['CANASTA'] = 'mercado'

        carga_mercado = pd.concat([
            merca[(~merca['ITEM'].isin(carga['ITEM'].unique())) & (~merca['ITEM'].isin(mercado['ITEM'].unique()))],
            mercado
        ], ignore_index=True)

        carga_surtido = surti[(~surti['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                              (~surti['ITEM'].isin(carga['ITEM'].unique()))]
        carga_excluido = exclu[(~exclu['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                               (~exclu['ITEM'].isin(carga['ITEM'].unique()))]
        carga_mp = mp[(~mp['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                      (~mp['ITEM'].isin(carga['ITEM'].unique()))]

        st.divider()
        st.subheader("Situación actual")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Referente (actual)", ref['ITEM'].unique().size, help=f"{len(ref)} combinaciones")
        col2.metric("Mercado (actual)", merca['ITEM'].unique().size, help=f"{len(merca)} combinaciones")
        col3.metric("Surtido (actual)", surti['ITEM'].unique().size, help=f"{len(surti)} combinaciones")
        col4.metric("Excluido (actual)", exclu['ITEM'].unique().size, help=f"{len(exclu)} combinaciones")
        col5.metric("Marca Propia (actual)", mp['ITEM'].unique().size, help=f"{len(mp)} combinaciones")

        st.subheader("A cargar")
        col1b, col2b, col3b, col4b, col5b = st.columns(5)
        col1b.metric("Referente", carga['ITEM'].unique().size, help=f"{len(carga)} combinaciones")
        col2b.metric("Mercado", carga_mercado['ITEM'].unique().size, help=f"{len(carga_mercado)} combinaciones")
        col3b.metric("Surtido", carga_surtido['ITEM'].unique().size, help=f"{len(carga_surtido)} combinaciones")
        col4b.metric("Excluido", carga_excluido['ITEM'].unique().size, help=f"{len(carga_excluido)} combinaciones")
        col5b.metric("Marca Propia", carga_mp['ITEM'].unique().size, help=f"{len(carga_mp)} combinaciones")

        cont = st.text_input('¿Está de acuerdo con estos cambios? (si/no)')
        if cont.lower().strip() == 'si':
            with st.spinner('Borrando registros actuales...'):
                for canasta in ['referente', 'mercado', 'surtido', 'excluido', 'marca propia']:
                    cursor.execute(f"DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = '{canasta}';")
            st.success('Registros anteriores eliminados.')

            with st.spinner('Cargando nuevos registros...'):
                carga_snow_generic(df=carga, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
                carga_snow_generic(df=carga_mercado, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
                carga_snow_generic(df=carga_surtido, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
                carga_snow_generic(df=carga_excluido, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
                carga_snow_generic(df=carga_mp, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')

            st.success(f'Nuevos registros cargados: {len(carga) + len(carga_mercado) + len(carga_surtido) + len(carga_excluido) + len(carga_mp)} combinaciones en total.')

            st.session_state.ref = carga
            st.session_state.mer = carga_mercado
            st.session_state.sur = carga_surtido
            st.session_state.exc = carga_excluido
            st.session_state.mp = carga_mp

    if 'ref' not in st.session_state:
        st.stop()
    else:
        carga = st.session_state.ref
        carga_mercado = st.session_state.mer
        carga_surtido = st.session_state.sur
        carga_excluido = st.session_state.exc
        carga_mp = st.session_state.mp

        st.divider()
        st.subheader("Descargar resultados")

        tabs = st.tabs(["Referente", "Mercado", "Surtido", "Excluido", "Marca Propia"])

        for tab, df_tab, nombre in zip(tabs,
                                       [carga, carga_mercado, carga_surtido, carga_excluido, carga_mp],
                                       ['Referentes', 'Mercado', 'Surtido', 'Excluido', 'Marca Propia']):
            with tab:
                st.dataframe(df_tab.head(10), use_container_width=True)
                st.download_button(
                    label=f'⬇️ Descargar {nombre}',
                    data=df_tab.to_csv(index=False),
                    file_name=f'{nombre}.csv',
                    mime='text/csv'
                )

    st.divider()
    st.success("Proceso completado.")

elif prog == 'Mercado + Surtido':
    st.info('Arrastrá el archivo Excel con las columnas **[ITEM, CANASTA]**.')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()

    canastas = pd.read_excel(uploaded_file)

    with st.spinner('Descargando locales...'):
        locales = descargar_segmento(cursor, 'LOCALES')
        can = descargar_segmento(cursor, 'CANASTAS', conds=[""])

    canastas['aux'] = 1
    locales['aux'] = 1
    carga = canastas.merge(locales).drop(columns=['aux', 'GEOG_DPTO_DESC'])
    borrar = can[can['ITEM'].isin(canastas['ITEM'].unique())]

    st.divider()
    st.subheader("Registros a reemplazar")
    col1, col2, col3 = st.columns(3)
    col1.metric("Referente a borrar", len(borrar[borrar['CANASTA'] == 'referente']),
                help=f"{borrar[borrar['CANASTA']=='referente']['ITEM'].unique().size} artículos")
    col2.metric("Mercado a borrar", len(borrar[borrar['CANASTA'] == 'mercado']),
                help=f"{borrar[borrar['CANASTA']=='mercado']['ITEM'].unique().size} artículos")
    col3.metric("Surtido a borrar", len(borrar[borrar['CANASTA'] == 'surtido']),
                help=f"{borrar[borrar['CANASTA']=='surtido']['ITEM'].unique().size} artículos")

    ac = st.text_input('Estos se reemplazarán por lo que se encuentra en el archivo. ¿Está de acuerdo? (si/no)')
    if ac.lower().strip() == 'si':
        with st.spinner('Borrando registros...'):
            try:
                query = 'DELETE FROM SPIKE.SPIKE.CANASTAS WHERE ITEM IN {items}'
                cursor.execute(query.format(items=tuple(borrar['ITEM'].unique())))
                st.success('Registros borrados correctamente.')
            except:
                st.error('Hubo problemas para borrar los registros. Contactar con el área de Data.')
                st.stop()

        with st.spinner('Subiendo nuevos datos...'):
            try:
                carga_snow_generic(df=carga, ctx=snow, database='SPIKE', schema='SPIKE', table='CANASTAS')
                st.success('Registros subidos correctamente.')
            except:
                st.error('Hubo problemas para subir los registros. Contactar con el área de Data.')
                st.stop()

        st.divider()
        st.success("Proceso completado.")
