import pandas as pd
import time
import pandas as pd
from utils import snowflake_login, descargar_segmento
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import streamlit as st

st.title('Carga de precios')

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
    
if 'tabla' not in st.session_state:
    #Cargamos el archivo
    st.write('Arrastrá el archivo excel con las siguientes columnas [CODIGO_TIENDA,ORIN,PVP_NUEVO,EFFECTIVE_DATE]')
    uploaded_file = st.file_uploader("Cargar el archivo", type="xlsx")

    #Leemos el archivo
    if uploaded_file is not None:
        df = pd.read_excel(uploaded_file)
        df.columns = df.columns.str.upper()
    else:
        st.stop()

    #Le damos el formato correcto al archivo
    try:
        df = df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE']]
    except KeyError:
        st.write('')
        st.write("❌ Hay un error con las columnas. Verifica que todas existan en el archivo.")
        st.stop()

    # Validar que las columnas númericas contengan solo números enteros
    try:
        df['CODIGO_TIENDA'] = df['CODIGO_TIENDA'].astype('int64')
        df['ORIN'] = df['ORIN'].astype('int64').astype('str')
        df['PVP_NUEVO'] = df['PVP_NUEVO'].astype('int64')
        df['EFFECTIVE_DATE'] = df['EFFECTIVE_DATE'].dt.strftime('%Y-%m-%d')
    except KeyError:
        st.write('')
        st.write("❌ Hay un error con las columnas. Verificar las columnas numéricas.")
        st.stop()

    st.write("✅ Archivo validado correctamente. Puedes continuar.")
    st.write('')
    st.write('El archivo seleccionado es asi:')
    st.dataframe(df.head())

    st.write('')
    st.write('El archivo tiene un total de ', len(df), ' combinaciones para cambios de precio')

    if len(df[df.PVP_NUEVO==0]) > 0:
        st.write('')
        st.write('Hay', len(df[df.PVP_NUEVO==0]), ' lineas con precio 0$. Voy a descartar estos datos')

    df = df[df.PVP_NUEVO!=0]

    f = datetime.now().strftime('%Y%m%d_%H%M%S')

    st.write(f)

    df['TABLA']=str(f)
    df.columns=df.columns.str.upper()
    df=df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE', 'TABLA']]

    #cargo a snowflake las combinaciones a impactar
    try:
        st.write('')
        st.write('Cargando tabla auxiliar')
        time.sleep(3)
        success, nchunks, nrows, _ = write_pandas(snow, df, table_name='INPUT_PRICING_ACUMULADO',
                                                    database='SANDBOX_PLUS', schema='DWH')
        st.write('')
        st.write(f"Éxito: {success}, Chunks: {nchunks}, Filas insertadas: {nrows}")
        st.session_state.tabla = f
    except Exception as e:
        st.write(f"Error al cargar en Snowflake: {e}")
        st.stop()

if 'tabla' in st.session_state:
    time.sleep(2)
else:
    st.stop()

#Filtros
if 'filtros_carga_precios' not in st.session_state:
    filtros_posibles = {'EXCLUIDOS COMERCIAL':'EXCLUIDO_COMERCIAL = 0',
                        'PROMOCION':'ESTA_EN_PROMO = 0', 'ACTIVOS':'ARTC_ESTA_ID = 4',
                        'PRECIO DIFERENTE':'S.STCK_PRECIO_VENTA_DIA_CIVA <> A.PVP_NUEVO'}
    filtros = st.multiselect('Filtros:', list(filtros_posibles.keys()), list(filtros_posibles.keys()))

    st.write('Seleccionar los filtros y aguardar...')
    time.sleep(10)

    todos_los_filtros = []
    for f in filtros:
        todos_los_filtros.append(filtros_posibles[f])
    if len(todos_los_filtros) > 0:
        todos_los_filtros = ' AND ' + ' AND '.join(todos_los_filtros)
    else:
        todos_los_filtros = ''
    st.session_state.filtros_carga_precios = todos_los_filtros

if 'filtros_carga_precios' in st.session_state:
    todos_los_filtros = st.session_state.filtros_carga_precios
    st.write('')
    st.write('Filtros guardados')
else:
    st.stop()

if 'checks_carga_precios' not in st.session_state:
    st.write('')
    st.write('')
    st.write('-------------------------------------------------------------------------')
    st.write('--------------------HAGO UNOS CHEQUEOS-----------------------------------')

    time.sleep(3)

    duplicados = descargar_segmento(cursor, 'DUPLICADOS', cond="'" + st.session_state.tabla + "'")
    if len(duplicados) > 0:
        st.write('')
        st.write('Se encontraron ', len(duplicados), ' duplicados:')
        st.write('')
        st.dataframe(duplicados)
        st.write('')
        st.write('Descarto los duplicados')
    else:
        st.write('')
        st.write('No hay duplicados de items-local')

    time.sleep(3)

    checks = descargar_segmento(cursor, query='TOTAL', cond="'" + st.session_state.tabla + "'")
    checks.columns = checks.columns.str.lower()
    checks.integrantes_familia = checks.integrantes_familia.astype('int64')

    st.write('Hay un total de ', len(checks), ' combinaciones local sin aplicar filtros')

    time.sleep(3)

    familia = checks[checks.integrantes_familia > 1]
    st.write('Hay ', len(familia[['familia', 'geog_locl_cod']].drop_duplicates()),
            ' combinaciones familia-local')
    st.write('Generan un total de ', len(familia[['orin', 'geog_locl_cod']]),
            ' combinaciones item-local')

    time.sleep(3)

    ex = checks[checks.excluido_comercial == 1]
    if len(ex[['orin', 'geog_locl_cod']]) > 0:
        st.write('Hay ', len(ex[['orin', 'geog_locl_cod']]),
                ' combinaciones item-local excluidos por comercial')
        st.write('Hay ', len(ex[['orin']].drop_duplicates()), 'items unicos excluidos por comercial')
    else:
        st.write('No hay items excluidos por comercial para cargar')

    time.sleep(3)

    promo = checks[checks.esta_en_promo == 1]
    if len(promo[['orin', 'geog_locl_cod']]) > 0:
        st.write('Hay ', len(promo[['orin', 'geog_locl_cod']]), ' combinaciones item-local en promo')
    else:
        st.write('No hay items en promo')

    time.sleep(3)

    activo = checks[checks.estado != 4]
    if len(activo[['orin', 'geog_locl_cod']]) > 0:
        st.write('Hay ', len(activo[['orin', 'geog_locl_cod']]), ' combinaciones item-local no activas')
    else:
        st.write('Todas las combinaciones estan activas')

    time.sleep(3)

    precio = checks[checks.precio_diferente == 0]
    st.write('Hay ', len(precio[['orin', 'geog_locl_cod']]),
             ' combinaciones item-local con ese mismo precio')
    st.session_state.checks_carga_precios = True

if 'checks_carga_precios' in st.session_state:
    time.sleep(2)
else:
    st.stop()

#una vez validado inserto los precios en recopilacion modificaciones precio
if 'final_carga_precios' not in st.session_state:
    query = f"""
INSERT INTO SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS
WITH LIMPIO AS (
SELECT
    DISTINCT TABLA, EFFECTIVE_DATE, CODIGO_TIENDA, FAMILIA, PVP_NUEVO
FROM (
    SELECT
        DISTINCT TABLA, CODIGO_TIENDA,A.EFFECTIVE_DATE ,A.ORIN,B.FAMILIA, PVP_NUEVO,
        ROW_NUMBER() OVER (PARTITION BY GEOG_LOCL_COD,B.FAMILIA ORDER BY A.PVP_NUEVO) RN_PRECIO
    FROM
        SANDBOX_PLUS.DWH.INPUT_PRICING_ACUMULADO A
    LEFT JOIN
        SANDBOX_PLUS.DWH.RESULTADO_PRICING_HISTORICO_BIS B
    ON
        A.ORIN = B.ORIN AND A.CODIGO_TIENDA = B.GEOG_LOCL_COD
    WHERE
        FECHA_EJECUCION = (SELECT MAX(FECHA_EJECUCION) FROM SANDBOX_PLUS.DWH.RESULTADO_PRICING_HISTORICO_BIS)
    AND
        TABLA='{st.session_state.tabla}'
    QUALIFY
        RN_PRECIO=1
    ORDER BY 1,3,2
    )
)

, FILTRO AS (
SELECT
    B.ORIN, B.GEOG_LOCL_COD, A.PVP_NUEVO, A.TABLA, NULL AS PRIORIDAD, A.EFFECTIVE_DATE, B.FAMILIA, B.INTEGRANTES_FAMILIA, B.CANASTA,
    B.ARTC_ARTC_DESC, B.STCK_AYER, B.COSTO_UNITARIO, B.PRECIO_SUGERIDO, B.PRECIO_COMPETENCIA_UTILIZADO_POR_MODELO, B.STCK_PRECIO_VENTA_DIA_CIVA,
    B.FECHA_EJECUCION AS FECHA_MODELO, B.UNIDADES_VENDIDAS_TREINTA_DIAS, B.VNTA_TREINTA_DIAS,
    CASE WHEN B.CANASTA = 'REFERENTE' THEN 1 WHEN B.CANASTA = 'MERCADO' THEN 2 WHEN B.CANASTA = 'SURTIDO' THEN 3
        WHEN B.CANASTA = 'MARCA PROPIA' THEN 4 WHEN B.CANASTA IS NULL THEN 5 ELSE 6 END AS PRIORIDAD_CANASTA,
    NULL AS PRIORIDAD_FUENTE, CURRENT_DATE AS FECHA_ANALISIS
FROM
    SANDBOX_PLUS.DWH.RESULTADO_PRICING_HISTORICO_BIS B 
JOIN
    LIMPIO A
ON
    A.FAMILIA = B.FAMILIA AND A.CODIGO_TIENDA = B.GEOG_LOCL_COD
JOIN
    MSTRDB.DWH.FT_STOCK S
ON
    S.ARTC_ARTC_ID = B.ARTC_ARTC_ID AND S.GEOG_LOCL_ID = B.GEOG_LOCL_ID AND S.TIEM_DIA_ID = CURRENT_DATE - 1
WHERE
    FECHA_EJECUCION = (SELECT MAX(FECHA_EJECUCION) FROM SANDBOX_PLUS.DWH.RESULTADO_PRICING_HISTORICO_BIS)
    {todos_los_filtros}
ORDER BY
    FAMILIA, B.GEOG_LOCL_COD
)

SELECT
    DISTINCT CANASTA, FAMILIA, INTEGRANTES_FAMILIA, ORIN, GEOG_LOCL_COD, STCK_PRECIO_VENTA_DIA_CIVA, PRECIO_COMPETENCIA_UTILIZADO_POR_MODELO,
    PRECIO_SUGERIDO, PVP_NUEVO AS PRECIO_NUEVO_AJUSTADO, COSTO_UNITARIO, UNIDADES_VENDIDAS_TREINTA_DIAS, VNTA_TREINTA_DIAS, STCK_AYER,
    PRIORIDAD_CANASTA, PRIORIDAD_FUENTE, FECHA_MODELO,
    ROW_NUMBER() OVER (PARTITION BY GEOG_LOCL_COD ORDER BY UNIDADES_VENDIDAS_TREINTA_DIAS DESC) AS RN,
    NULL AS LIMITE_CAMBIOS, CURRENT_DATE AS FECHA_ANALISIS, EFFECTIVE_DATE, TABLA, '{user}' AS USUARIO
FROM
    FILTRO;
    """
    cursor.execute(query)

    time.sleep(3)

    #ULTIMO CHEQUEO
    final = descargar_segmento(cursor, query='FINAL', cond="'" + st.session_state.tabla + "'")
    final.columns = final.columns.str.upper()
    st.dataframe(final)

    st.write('')
    st.write('Finalmente quedaron ', len(final), ' combinaciones para impactar')

    time.sleep(3)

    #genero archivos para la web
    datos = descargar_segmento(cursor, 'DATOS', cond="'" + st.session_state.tabla + "'")
    datos['CHANGE_AMOUNT'] = datos['CHANGE_AMOUNT'].astype('int64')
    datos['EFFECTIVE_DATE'] = pd.to_datetime(datos['EFFECTIVE_DATE'])

    # Cambiar el formato a día/mes/año
    datos['EFFECTIVE_DATE'] = datos['EFFECTIVE_DATE'].dt.strftime('%d/%m/%Y')
    d = datos[['LOCATION', 'EFFECTIVE_DATE',
                'CHANGE_AMOUNT']].groupby(['LOCATION',
                                            'EFFECTIVE_DATE']).count().reset_index().sort_values(by='CHANGE_AMOUNT', ascending=False)
    d.rename(columns={'CHANGE_AMOUNT':'COMBINACIONES'}, inplace=True)
    st.write('')
    st.write('Combinaciones por local:')
    st.dataframe(d)

    time.sleep(3)

    st.write('')
    st.session_state.final_carga_precios = datos

if 'final_carga_precios' in st.session_state:
    st.write('Archivo a cargar:')
    st.dataframe(st.session_state.final_carga_precios)
    st.write('')
    borrado = st.text_input("Desea registrar estos precios como enviados? (si/no)").strip().lower()
    if borrado == '':
        st.stop()
    elif borrado=='no':
        cursor.execute(f"DELETE FROM SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS WHERE TABLA='{st.session_state.tabla}'")
        st.write('Datos descartados')
    else:
        st.write('Datos grabados')
else:
    st.stop()

time.sleep(3)

st.write('')
st.write("Programa finalizado. Manteniéndose abierto...")