import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, carga_snow_generic, get_credentials, snowflake_login

st.title('Carga de familias')

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

prog = st.selectbox('Seleccione el programa: ', ['Referente', 'Mercado + Surtido'])

if prog == 'Referente':
    #Cargamos el archivo 
    st.write('')
    st.write('Arrastrá el archivo excel con el formato establecido')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()
    else:
        #Armamos archivos con referentes
        referentes = pd.read_excel(uploaded_file).melt(id_vars='ORIN', value_vars=pd.read_excel(uploaded_file).columns[1:],
                                                       var_name='GEOG_DPTO_DESC', value_name='REFERENTE').rename(columns={'ORIN':'ITEM'})
        referentes = referentes[referentes['REFERENTE']==1].drop(columns='REFERENTE')
    
    #Descargamos los locales, los referentes actuales y los de mercado actuales
    locales = descargar_segmento(cursor, 'LOCALES')
    ref = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'referente'"])
    merca = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'mercado'"])

    #Agregamos columnas auxiliares en los df que corresponda para explotar
    referentes['aux'] = 1
    ref['aux'] = 1
    locales['aux'] = 1

    #Armamos archivo a cargar
    carga = referentes.merge(locales).drop(columns=['aux', 'GEOG_DPTO_DESC'])
    carga['CANASTA'] = 'referente'

    #Vemos los referentes que pasan a ser mercado y los juntamos
    mercado1 = ref[~ref['ITEM'].isin(referentes['ITEM'])].drop(columns='GEOG_LOCL_ID').drop_duplicates().merge(locales)[ref.columns].drop(columns='aux')
    mercado2 = locales.drop(columns='GEOG_DPTO_DESC').merge(referentes.drop(columns='GEOG_DPTO_DESC').drop_duplicates()).drop(columns='aux').merge(carga, how='left', indicator=True)
    mercado = pd.concat([mercado1, mercado2[mercado2['_merge']!='both'].drop(columns='_merge')],
                        ignore_index=True)
    mercado['CANASTA'] = 'mercado'

    #Sumamos los que ya eran mercado
    carga_mercado = pd.concat([mercado, merca[~merca['ITEM'].isin(mercado['ITEM'].unique())]],
                              ignore_index=True)
    
    #Borramos los articulos que actualmente están como referentes y como mercado
    cursor.execute("DELETE FROM SANDBOX_PLUS.DWH.CANASTAS WHERE CANASTA = 'referente';")
    st.write('Combinaciones con canasta "referente" borradas')
    cursor.execute("DELETE FROM SANDBOX_PLUS.DWH.CANASTAS WHERE CANASTA = 'mercado';")
    st.write('Combinaciones con canasta "mercado" borradas')
    st.write('')

    #Cargamos productos nuevos
    carga_snow_generic(df=carga, ctx=snow, table='CANASTAS', database='SANDBOX_PLUS', schema='DWH')
    st.write(f'Se cargaron {len(carga)} combinaciones con canasta "referente"')
    carga_snow_generic(df=carga_mercado, ctx=snow, table='CANASTAS', database='SANDBOX_PLUS', schema='DWH')
    st.write(f'Se cargaron {len(carga_mercado)} combinaciones con canasta "mercado"')
    
    #Mostramos las primeras filas del dataframe y botón de descarga. Referentes
    st.write('')
    st.write('Referentes actuales (primeras 10 filas):')
    st.write('')
    st.dataframe(carga.head(10))
    csv_ref = carga.to_csv(index=False)
    st.download_button(label='Descargar referentes', data=csv_ref, file_name='Referentes.csv', mime='text/csv')
    
    #Mercado
    st.write('')
    st.write('Mercado actuales (primeras 10 filas):')
    st.write('')
    st.dataframe(carga_mercado.head(10))
    csv_mer = carga_mercado.to_csv(index=False)
    st.download_button(label='Descargar mercado', data=csv_mer, file_name='Mercado.csv', mime='text/csv')

    st.write('')
    st.write("Programa finalizado. Manteniéndose abierto...")
elif prog == 'Mercado + Surtido':
    #Cargamos el archivo 
    st.write('')
    st.write('Arrastrá el archivo excel con las columnas [ITEM, CANASTA]')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()
    else:
        #Armamos archivos con referentes
        canastas = pd.read_excel(uploaded_file)
    
    #Descargamos locales
    locales = descargar_segmento(cursor, 'LOCALES')

    #Sumamos auxiliares donde corresponda
    canastas['aux'] = 1
    locales['aux'] = 1

    #Armamos archivo a cargar
    carga = canastas.merge(locales).drop(columns=['aux', 'GEOG_DPTO_DESC'])

    #Descargamos las canastas
    can = descargar_segmento(cursor, 'CANASTAS', conds=[""])

    borrar = can[can['ITEM'].isin(canastas['ITEM'].unique())]

    st.write(f'Se van a borrar {len(borrar[borrar['CANASTA']=='referente'])} registros de {borrar[borrar['CANASTA']=='referente']['ITEM'].unique().size} artículos referentes')
    st.write(f'Se van a borrar {len(borrar[borrar['CANASTA']=='mercado'])} registros de {borrar[borrar['CANASTA']=='mercado']['ITEM'].unique().size} artículos mercado')
    st.write(f'Se van a borrar {len(borrar[borrar['CANASTA']=='surtido'])} registros de {borrar[borrar['CANASTA']=='surtido']['ITEM'].unique().size} artículos surtido')
    st.write(f'Se van a borrar {len(borrar[borrar['CANASTA']=='excluido'])} registros de {borrar[borrar['CANASTA']=='excluido']['ITEM'].unique().size} artículos excluidos')
    st.write(f'Se van a borrar {len(borrar[borrar['CANASTA']=='marca propia'])} registros de {borrar[borrar['CANASTA']=='marca propia']['ITEM'].unique().size} artículos marca propia')

    ac = st.text_input('Estos se reemplazarán por lo que se encuentra en el archivo. Está de acuerdo? (si/no)')
    if ac.lower().strip() == 'si':
        try:
            st.write('')
            st.write('Borrando')
            query = 'DELETE FROM SANDBOX_PLUS.DWH.CANASTAS WHERE ITEM IN {items}'
            cursor.execute(query.format(items=tuple(borrar['ITEM'].unique())))
            st.write('Registros borrados correctamente.')
        except:
            st.write('Hubo problemas para borrar los registros. Contactar con area de Data.')
            st.stop()
        
        try:
            st.write('')
            st.write('Subiendo nuevos datos')
            carga_snow_generic(df=carga, ctx=snow, database='SANDBOX_PLUS',
                               schema='DWH', table='CANASTAS')
            st.write('Registros subidos correctamente.')
        except:
            st.write('Hubo problemas para subir los registros. Contactar con area de Data.')
            st.stop()
        
        st.write('')
        st.write("Programa finalizado. Manteniéndose abierto...")