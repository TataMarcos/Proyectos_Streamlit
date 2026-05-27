import pandas as pd
import time
from datetime import datetime
import streamlit as st
from utils import descargar_segmento, snowflake_login, get_credentials, carga_snow_generic
import io
import zipfile

st.set_page_config(page_title="Carga de Precios", page_icon="📥", layout="wide")

st.title("📥 Carga de precios")
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

if 'tabla' not in st.session_state:
    st.info('Arrastrá el archivo Excel con las columnas: **CODIGO_TIENDA, ORIN, PVP_NUEVO, EFFECTIVE_DATE**')
    uploaded_file = st.file_uploader("Cargar archivo", type="xlsx")

    if uploaded_file is not None:
        df = pd.read_excel(uploaded_file)
        df.columns = df.columns.str.upper()
    else:
        st.stop()

    try:
        df = df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE']]
    except KeyError:
        st.error("Error en las columnas. Verificá que todas existan en el archivo.")
        st.stop()

    try:
        df['CODIGO_TIENDA'] = df['CODIGO_TIENDA'].astype('int64')
        df['ORIN'] = df['ORIN'].astype('int64').astype('str')
        df['PVP_NUEVO'] = df['PVP_NUEVO'].astype('int64')
        df['EFFECTIVE_DATE'] = pd.to_datetime(df['EFFECTIVE_DATE']).dt.strftime('%Y-%m-%d')
    except KeyError:
        st.error("Error en columnas numéricas. Verificá los tipos de dato.")
        st.stop()

    st.success("Archivo validado correctamente.")

    col_tot, col_cero = st.columns(2)
    col_tot.metric("Combinaciones cargadas", len(df))
    if len(df[df.PVP_NUEVO == 0]) > 0:
        col_cero.metric("Líneas con precio $0 (descartadas)", len(df[df.PVP_NUEVO == 0]))
        st.warning(f"Se descartarán {len(df[df.PVP_NUEVO==0])} líneas con precio $0.")

    df = df[df.PVP_NUEVO != 0]
    st.dataframe(df.head(), use_container_width=True)

    f = datetime.now().strftime('%Y%m%d_%H%M%S')
    df['TABLA'] = str(f)
    df.columns = df.columns.str.upper()
    df = df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE', 'TABLA']]

    with st.spinner('Cargando tabla auxiliar en Snowflake...'):
        time.sleep(3)
        try:
            success, nchunks, nrows, _ = carga_snow_generic(
                df=df, ctx=snow, database='SANDBOX_PLUS', schema='DWH',
                table='INPUT_PRICING_ACUMULADO'
            )
            st.success(f"Tabla cargada: {nrows} filas insertadas.")
            st.session_state.tabla = f
        except Exception as e:
            st.error(f"Error al cargar en Snowflake: {e}")
            st.stop()

if 'tabla' in st.session_state:
    time.sleep(2)
else:
    st.stop()

# Selección de filtros
if 'filtros_carga_precios' not in st.session_state:
    st.divider()
    st.subheader("Filtros de validación")
    filtros_posibles = {
        'EXCLUIDOS COMERCIAL': 'EXCLUIDO_COMERCIAL = 0',
        'PROMOCION': 'ESTA_EN_PROMO = 0',
        'ACTIVOS': 'ARTC_ESTA_ID = 4',
        'PRECIO DIFERENTE': 'S.STCK_PRECIO_VENTA_DIA_CIVA <> A.PVP_NUEVO'
    }
    filtros = st.multiselect('Filtros:', list(filtros_posibles.keys()), list(filtros_posibles.keys()))

    with st.spinner('Aplicando filtros...'):
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
    st.info("Filtros guardados.")
else:
    st.stop()

# Chequeos de validación
if 'checks_carga_precios' not in st.session_state:
    st.divider()
    st.subheader("Validaciones")

    conds = ["'" + st.session_state.tabla + "'"]

    with st.spinner('Verificando duplicados...'):
        time.sleep(3)
        duplicados = descargar_segmento(cursor, 'DUPLICADOS', conds=conds)

    if len(duplicados) > 0:
        st.warning(f"Se encontraron {len(duplicados)} duplicados. Serán descartados.")
        with st.expander("Ver duplicados"):
            st.dataframe(duplicados, use_container_width=True)
    else:
        st.success("Sin duplicados de ítem-local.")

    with st.spinner('Ejecutando chequeos generales...'):
        time.sleep(3)
        checks = descargar_segmento(cursor, query='TOTAL', conds=conds)
        checks.columns = checks.columns.str.lower()
        checks.integrantes_familia = checks.integrantes_familia.astype('int64')

    familia = checks[checks.integrantes_familia > 1]
    ex = checks[checks.excluido_comercial == 1]
    promo = checks[checks.esta_en_promo == 1]
    activo = checks[checks.estado != 4]
    precio = checks[checks.precio_diferente == 0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Combinaciones totales", len(checks))
    col2.metric("Combinaciones familia-local", len(familia[['familia', 'geog_locl_cod']].drop_duplicates()))
    col3.metric("Combinaciones ítem-local (familias)", len(familia[['orin', 'geog_locl_cod']]))

    col4, col5, col6 = st.columns(3)
    col4.metric("Excluidos por comercial", len(ex[['orin', 'geog_locl_cod']]))
    col5.metric("Ítems en promoción", len(promo[['orin', 'geog_locl_cod']]))
    col6.metric("Combinaciones no activas", len(activo[['orin', 'geog_locl_cod']]))

    st.metric("Con mismo precio actual", len(precio[['orin', 'geog_locl_cod']]))

    st.session_state.checks_carga_precios = checks

if 'checks_carga_precios' in st.session_state:
    time.sleep(2)
else:
    st.stop()

# Inserción final
if 'final_carga_precios' not in st.session_state:
    conds = ["'" + st.session_state.tabla + "'"]
    todos_los_filtros = st.session_state.filtros_carga_precios

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

    with st.spinner('Insertando precios validados...'):
        time.sleep(3)
        cursor.execute(query)

    final = descargar_segmento(cursor, query='FINAL', conds=conds)
    final.columns = final.columns.str.upper()

    st.divider()
    st.subheader("Resultado final")
    st.metric("Combinaciones a impactar", len(final))
    st.dataframe(final, use_container_width=True)

    with st.spinner('Generando archivos para la web...'):
        time.sleep(3)
        datos = descargar_segmento(cursor, 'DATOS', conds=conds)
        datos['CHANGE_AMOUNT'] = datos['CHANGE_AMOUNT'].astype('int64')
        datos['EFFECTIVE_DATE'] = pd.to_datetime(datos['EFFECTIVE_DATE']).dt.strftime('%d/%m/%Y')

    d = (datos[['LOCATION', 'EFFECTIVE_DATE', 'CHANGE_AMOUNT']]
         .groupby(['LOCATION', 'EFFECTIVE_DATE']).count().reset_index()
         .sort_values(by='CHANGE_AMOUNT', ascending=False))
    d.rename(columns={'CHANGE_AMOUNT': 'COMBINACIONES'}, inplace=True)
    with st.expander("Combinaciones por local"):
        st.dataframe(d, use_container_width=True)

    st.session_state.final_carga_precios = datos

if 'final_carga_precios' in st.session_state:
    datos = st.session_state.final_carga_precios
    checks = st.session_state.checks_carga_precios
    f = st.session_state.tabla

    if 'final_descarga_precios' in st.session_state:
        st.divider()
        st.subheader("Resumen de validaciones")
        familia = checks[checks.integrantes_familia > 1]
        ex = checks[checks.excluido_comercial == 1]
        promo = checks[checks.esta_en_promo == 1]
        activo = checks[checks.estado != 4]
        precio = checks[checks.precio_diferente == 0]

        col1, col2, col3 = st.columns(3)
        col1.metric("Combinaciones totales", len(checks))
        col2.metric("Combinaciones familia-local", len(familia[['familia', 'geog_locl_cod']].drop_duplicates()))
        col3.metric("Excluidos por comercial", len(ex[['orin', 'geog_locl_cod']]))

        col4, col5, col6 = st.columns(3)
        col4.metric("En promoción", len(promo[['orin', 'geog_locl_cod']]))
        col5.metric("No activos", len(activo[['orin', 'geog_locl_cod']]))
        col6.metric("Con mismo precio", len(precio[['orin', 'geog_locl_cod']]))

    st.divider()
    filas_por_archivo = 2999
    num_archivos = len(datos) // filas_por_archivo + (1 if len(datos) % filas_por_archivo else 0)

    col_fa, col_ca = st.columns(2)
    col_fa.metric("Filas por archivo", filas_por_archivo)
    col_ca.metric("Cantidad de archivos", num_archivos)

    buffer_zip = io.BytesIO()
    with zipfile.ZipFile(buffer_zip, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for i in range(num_archivos):
            inicio = i * filas_por_archivo
            fin = inicio + filas_por_archivo
            segmento = datos.iloc[inicio:fin]
            nombre_archivo = f"Pricing_WEB_{f}_file_{i+1}.csv"
            csv = segmento.to_csv(index=False, sep=';')
            zip_file.writestr(nombre_archivo, csv)
    buffer_zip.seek(0)

    col_zip, col_cons = st.columns(2)
    with col_zip:
        st.download_button(
            label="⬇️ Descargar todos los archivos (ZIP)",
            data=buffer_zip,
            file_name="Pricing_WEB_archivos.zip",
            mime="application/zip",
            use_container_width=True
        )
    cons = datos[['ITEM', 'LOCATION', 'CHANGE_AMOUNT']]
    cons.columns = ['ORIN', 'CODIGO_TIENDA', 'PVP_NUEVO']
    csv = cons.to_csv(index=False, sep=';')
    with col_cons:
        st.download_button(
            label="⬇️ Descargar consolidado",
            data=csv,
            file_name='Consolidado.csv',
            mime='text/csv',
            use_container_width=True
        )

    desc = st.button('Terminó descarga')
    if desc:
        st.session_state.final_descarga_precios = True

if 'final_descarga_precios' in st.session_state:
    st.divider()
    st.subheader("Confirmar envío")
    st.dataframe(st.session_state.final_carga_precios, use_container_width=True)
    borrado = st.text_input("¿Registrar estos precios como enviados? (si/no)").strip().lower()
    if borrado == '':
        st.stop()
    elif borrado == 'no':
        cursor.execute(f"DELETE FROM SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS WHERE TABLA='{st.session_state.tabla}'")
        st.warning('Datos descartados.')
    else:
        st.success('Datos registrados como enviados.')
else:
    st.stop()

time.sleep(3)

st.divider()
st.success("Proceso completado.")
