import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, carga_snow_generic, get_credentials, snowflake_login

st.title('Actualización de canastas')

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

prog = st.selectbox('Seleccione el programa: ', ['Referente', 'Mercado + Surtido', 'Descarga de canastas'])

if prog == 'Descarga de canastas':
    c = st.selectbox('Seleccione la canasta: ', ['referente', 'mercado', 'surtido', 'excluido', 'marca propia'])
    cursor.execute(f'''
                    SELECT
                        LGL.GEOG_LOCL_COD, C.ITEM, LAA.ARTC_ARTC_DESC, C.CANASTA FROM SPIKE.SPIKE.CANASTAS C
                    JOIN
                        MSTRDB.DWH.LU_ARTC_ARTICULO LAA
                    ON C.ITEM::TEXT = LAA.ORIN
                    JOIN
                        MSTRDB.DWH.LU_GEOG_LOCAL LGL
                    ON C.GEOG_LOCL_ID = LGL.GEOG_LOCL_ID
                    WHERE
                        C.CANASTA = '{c}';''')
    desc = cursor.fetch_pandas_all().astype({'ITEM':'str'})
    st.dataframe(desc.head())
    csv = desc.to_csv(index=False)
    st.download_button(label='Descargar tabla', data=csv, file_name='Canastas.csv', mime='text/csv')
elif prog == 'Referente':
    #Cargamos el archivo 
    st.write('')
    st.write('Arrastrá el archivo excel con el formato establecido')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    if uploaded_file is None:
        st.stop()
    else:
        try:
            #Armamos archivos con referentes
            referentes = pd.read_excel(uploaded_file).melt(id_vars='ORIN', value_vars=pd.read_excel(uploaded_file).columns[1:],
                                                           var_name='GEOG_DPTO_DESC', value_name='REFERENTE').rename(columns={'ORIN':'ITEM'})
            referentes = referentes[referentes['REFERENTE']==1].drop(columns='REFERENTE')
        except:
            st.write('El archivo no tiene el formato correcto')
            st.stop()
    
    if 'ref' not in st.session_state:
        #Descargamos los locales, los referentes actuales y los de mercado actuales
        locales = descargar_segmento(cursor, 'LOCALES')
        ref = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'referente'"])
        merca = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'mercado'"])
        surti = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'surtido'"])
        exclu = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'excluido'"])
        mp = descargar_segmento(cursor, 'CANASTAS', conds=[" WHERE CANASTA = 'marca propia'"])

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
        carga_mercado = pd.concat([merca[(~merca['ITEM'].isin(carga['ITEM'].unique())) & (~merca['ITEM'].isin(mercado['ITEM'].unique()))],
                                mercado], ignore_index=True)
        
        #Armamos los nuevos surtido
        carga_surtido = surti[(~surti['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                            (~surti['ITEM'].isin(carga['ITEM'].unique()))]
        
        #Armamos los nuevos excluidos
        carga_excluido = exclu[(~exclu['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                            (~exclu['ITEM'].isin(carga['ITEM'].unique()))]
        
        #Armamos los nuevos surtido
        carga_mp = mp[(~mp['ITEM'].isin(carga_mercado['ITEM'].unique())) &
                    (~mp['ITEM'].isin(carga['ITEM'].unique()))]
        
        #Resumimos cambios
        st.write('')
        st.write('Combinaciones y items actuales:')
        st.write(f"Referentes: {ref['ITEM'].unique().size} articulos con {len(ref)} combinaciones.")
        st.write(f"Mercado: {merca['ITEM'].unique().size} articulos con {len(merca)} combinaciones.")
        st.write(f"Surtido: {surti['ITEM'].unique().size} articulos con {len(surti)} combinaciones.")
        st.write(f"Excluido: {exclu['ITEM'].unique().size} articulos con {len(exclu)} combinaciones.")
        st.write(f"Marca Propia: {mp['ITEM'].unique().size} articulos con {len(mp)} combinaciones.")

        st.write('')
        st.write('Combinaciones y items a cargar:')
        st.write(f"Referentes: {carga['ITEM'].unique().size} articulos con {len(carga)} combinaciones.")
        st.write(f"Mercado: {carga_mercado['ITEM'].unique().size} articulos con {len(carga_mercado)} combinaciones.")
        st.write(f"Surtido: {carga_surtido['ITEM'].unique().size} articulos con {len(carga_surtido)} combinaciones.")
        st.write(f"Excluido: {carga_excluido['ITEM'].unique().size} articulos con {len(carga_excluido)} combinaciones.")
        st.write(f"Marca Propia: {carga_mp['ITEM'].unique().size} articulos con {len(carga_mp)} combinaciones.")
    
        cont = st.text_input('Está de acuerdo con estos cambios? (si/no)')
        if cont.lower().strip() == 'si':
            #Borramos los articulos que actualmente están como referentes y como mercado
            cursor.execute("DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = 'referente';")
            st.write('Combinaciones con canasta "referente" borradas')
            cursor.execute("DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = 'mercado';")
            st.write('Combinaciones con canasta "mercado" borradas')
            cursor.execute("DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = 'surtido';")
            st.write('Combinaciones con canasta "surtido" borradas')
            cursor.execute("DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = 'excluido';")
            st.write('Combinaciones con canasta "excluido" borradas')
            cursor.execute("DELETE FROM SPIKE.SPIKE.CANASTAS WHERE CANASTA = 'marca propia';")
            st.write('Combinaciones con canasta "marca propia" borradas')
            st.write('')

            #Cargamos productos nuevos
            carga_snow_generic(df=carga, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
            st.write(f'Se cargaron {len(carga)} combinaciones con canasta "referente"')
            carga_snow_generic(df=carga_mercado, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
            st.write(f'Se cargaron {len(carga_mercado)} combinaciones con canasta "mercado"')
            carga_snow_generic(df=carga_surtido, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
            st.write(f'Se cargaron {len(carga_surtido)} combinaciones con canasta "surtido"')
            carga_snow_generic(df=carga_excluido, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
            st.write(f'Se cargaron {len(carga_excluido)} combinaciones con canasta "excluido"')
            carga_snow_generic(df=carga_mp, ctx=snow, table='CANASTAS', database='SPIKE', schema='SPIKE')
            st.write(f'Se cargaron {len(carga_mp)} combinaciones con canasta "marca propia"')

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

        #Surtido
        st.write('')
        st.write('Surtido actuales (primeras 10 filas):')
        st.write('')
        st.dataframe(carga_surtido.head(10))
        csv_sur = carga_surtido.to_csv(index=False)
        st.download_button(label='Descargar surtido', data=csv_sur, file_name='Surtido.csv', mime='text/csv')

        #Excluido
        st.write('')
        st.write('Excluido actuales (primeras 10 filas):')
        st.write('')
        st.dataframe(carga_excluido.head(10))
        csv_exc = carga_excluido.to_csv(index=False)
        st.download_button(label='Descargar excluido', data=csv_exc, file_name='Excluido.csv', mime='text/csv')

        #Marca Propia
        st.write('')
        st.write('Marca Propia actuales (primeras 10 filas):')
        st.write('')
        st.dataframe(carga_mp.head(10))
        csv_mp = carga_mp.to_csv(index=False)
        st.download_button(label='Descargar marca propia', data=csv_mp, file_name='Marca Propia.csv', mime='text/csv')

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

    st.write(f"Se van a borrar {len(borrar[borrar['CANASTA']=='referente'])} registros de {borrar[borrar['CANASTA']=='referente']['ITEM'].unique().size} artículos referentes")
    st.write(f"Se van a borrar {len(borrar[borrar['CANASTA']=='mercado'])} registros de {borrar[borrar['CANASTA']=='mercado']['ITEM'].unique().size} artículos mercado")
    st.write(f"Se van a borrar {len(borrar[borrar['CANASTA']=='surtido'])} registros de {borrar[borrar['CANASTA']=='surtido']['ITEM'].unique().size} artículos surtido")
    st.write(f"Se van a borrar {len(borrar[borrar['CANASTA']=='excluido'])} registros de {borrar[borrar['CANASTA']=='excluido']['ITEM'].unique().size} artículos excluidos")
    st.write(f"Se van a borrar {len(borrar[borrar['CANASTA']=='marca propia'])} registros de {borrar[borrar['CANASTA']=='marca propia']['ITEM'].unique().size} artículos marca propia")

    ac = st.text_input('Estos se reemplazarán por lo que se encuentra en el archivo. Está de acuerdo? (si/no)')
    if ac.lower().strip() == 'si':
        try:
            st.write('')
            st.write('Borrando')
            query = 'DELETE FROM SPIKE.SPIKE.CANASTAS WHERE ITEM IN {items}'
            cursor.execute(query.format(items=tuple(borrar['ITEM'].unique())))
            st.write('Registros borrados correctamente.')
        except:
            st.write('Hubo problemas para borrar los registros. Contactar con area de Data.')
            st.stop()
        
        try:
            st.write('')
            st.write('Subiendo nuevos datos')
            carga_snow_generic(df=carga, ctx=snow, database='SPIKE', schema='SPIKE', table='CANASTAS')
            st.write('Registros subidos correctamente.')
        except:
            st.write('Hubo problemas para subir los registros. Contactar con area de Data.')
            st.stop()
        
        st.write('')
        st.write("Programa finalizado. Manteniéndose abierto...")