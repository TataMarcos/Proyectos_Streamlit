import pandas as pd
import time
from datetime import datetime, date
import streamlit as st
from utils import descargar_segmento, snowflake_login, get_credentials, carga_snow_generic
from io import BytesIO

st.set_page_config(page_title="Proveedores", page_icon="🏭", layout="wide")

st.title("🏭 Carga de costos de proveedores")
st.divider()

def inflacion_movil(series, ventana):
    tasas = series.iloc[-ventana:]
    acum = 1.0
    for t in tasas:
        acum *= (1 + t / 100)
    return round((acum - 1) * 100, 4)

def calcular_factor_acumulado(fecha_ultimo_cambio: str) -> float:
    try:
        periodo_cambio = pd.to_datetime(fecha_ultimo_cambio).to_period("M")
        periodo_inicio = periodo_cambio + 1
        mascara = (inflacion["Mes"] >= periodo_inicio) & (inflacion["Mes"] <= ultimo_periodo)
        factores = inflacion.loc[mascara, "factor"]
        if factores.empty:
            return 1.0
        return factores.prod()
    except:
        return 1.0

def calcular_factor_acumulado_usa(fecha_ultimo_cambio: str) -> float:
    try:
        periodo_cambio = pd.to_datetime(fecha_ultimo_cambio).to_period("M")
        periodo_inicio = periodo_cambio + 1
        mascara = (inflacion_usa["Mes"] >= periodo_inicio) & (inflacion_usa["Mes"] <= ultimo_periodo)
        factores = inflacion_usa.loc[mascara, "factor"]
        if factores.empty:
            return 1.0
        return factores.prod()
    except:
        return 1.0

def agregaInflacion(k: int, p: str):
    if p == 'Uruguay':
        k += 1
        per_nuevo = st.text_input('Ingrese período con formato (2001-01):', key=k)
        k += 1
        inf_nueva = st.text_input('Ingrese inflación del período ingresado con formato (0.1%):', key=k)
        if (per_nuevo.strip() == '' or inf_nueva.strip() == '' or per_nuevo.find('-') == -1 or
                len(per_nuevo.split('-')[0]) != 4 or len(per_nuevo.split('-')[1]) != 2):
            st.info('Aún no se ingresaron valores con formato correcto.')
            st.stop()
        else:
            try:
                a_nuevo = int(per_nuevo.split('-')[0].strip())
                m_nuevo = int(per_nuevo.split('-')[1].strip())
            except:
                st.error('El formato del período no es correcto.')
                st.stop()
            if a_nuevo < int(max(inflacion['Mes']).split('-')[0]):
                st.error('El período ingresado no es mayor al máximo período.')
                st.stop()
            elif m_nuevo <= int(max(inflacion['Mes']).split('-')[1]):
                st.error('El período ingresado no es mayor al máximo período.')
                st.stop()
            else:
                if len(str(m_nuevo)) == 2:
                    per_nuevo = str(a_nuevo) + '-' + str(m_nuevo)
                else:
                    per_nuevo = str(a_nuevo) + '-0' + str(m_nuevo)
            try:
                inf_nueva = float(inf_nueva.replace(',', '.').replace('%', ''))
            except:
                st.error('El formato del valor de inflación no es correcto.')
                st.stop()
            inflacion.loc[len(inflacion)] = (per_nuevo, inf_nueva)
            st.dataframe(inflacion, use_container_width=True)
            k += 1
            dec_inf = st.selectbox('¿Desea agregar un período más?', options=['Si', 'No'], key=k)
            if dec_inf.lower() == 'si':
                agregaInflacion(k=k + 1, p=p)
            elif dec_inf.lower() == 'no':
                inflacion.to_excel("Inflación.xlsx", index=False)
            else:
                st.stop()
    else:
        k += 1
        per_nuevo = st.text_input('Ingrese período con formato (2001-01):', key=k)
        k += 1
        inf_nueva = st.text_input('Ingrese inflación del período ingresado con formato (0.1%):', key=k)
        if (per_nuevo.strip() == '' or inf_nueva.strip() == '' or per_nuevo.find('-') == -1 or
                len(per_nuevo.split('-')[0]) != 4 or len(per_nuevo.split('-')[1]) != 2):
            st.info('Aún no se ingresaron valores con formato correcto.')
            st.stop()
        else:
            try:
                a_nuevo = int(per_nuevo.split('-')[0].strip())
                m_nuevo = int(per_nuevo.split('-')[1].strip())
            except:
                st.error('El formato del período no es correcto.')
                st.stop()
            if a_nuevo < int(max(inflacion_usa['Mes']).split('-')[0]):
                st.error('El período ingresado no es mayor al máximo período.')
                st.stop()
            elif m_nuevo <= int(max(inflacion_usa['Mes']).split('-')[1]):
                st.error('El período ingresado no es mayor al máximo período.')
                st.stop()
            else:
                if len(str(m_nuevo)) == 2:
                    per_nuevo = str(a_nuevo) + '-' + str(m_nuevo)
                else:
                    per_nuevo = str(a_nuevo) + '-0' + str(m_nuevo)
            try:
                inf_nueva = float(inf_nueva.replace(',', '.').replace('%', ''))
            except:
                st.error('El formato del valor de inflación no es correcto.')
                st.stop()
            inflacion_usa.loc[len(inflacion_usa)] = (per_nuevo, inf_nueva)
            st.dataframe(inflacion_usa, use_container_width=True)
            k += 1
            dec_inf = st.selectbox('¿Desea agregar un período más?', options=['Si', 'No'], key=k)
            if dec_inf.lower() == 'si':
                agregaInflacion(k=k + 1, p=p)
            elif dec_inf.lower() == 'no':
                inflacion_usa.to_excel("Inflacion USA.xlsx", index=False)
            else:
                st.stop()

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

# Carga de datos
if 'prov' not in st.session_state:
    cursor.execute('SELECT * FROM SANDBOX_PLUS.DWH.COSTO_PROVEEDORES WHERE ESTADO IS NULL;')
    prov = cursor.fetch_pandas_all().drop(columns='FECHA_ANALISIS').sort_values(
        ['LISTA', 'GEOG_LOCL_COD']).reset_index(drop=True)
else:
    prov = st.session_state.prov

opc = ["APROBADA", "RECHAZADA"]
dis = list(prov.columns)
dis.remove('ESTADO')
dt = date.today().strftime('20%y-%m-%d')
meses = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
         7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

# Edición y aprobación
if 'proveedores' not in st.session_state:
    prov['LISTA-SUP'] = prov['LISTA'].astype('str') + ' - ' + prov['SUP_NAME']
    lista = st.multiselect('Listas', options=prov['LISTA-SUP'].unique(), default=prov['LISTA-SUP'].unique())
    cont = st.selectbox('Contrato firmado', ['TODOS', 'SI', 'NO'])

    df = prov[prov['LISTA-SUP'].isin(lista)]
    if cont == 'SI':
        df = df[~df['CONTRATOS_DESDE'].isna()]
    elif cont == 'NO':
        df = df[df['CONTRATOS_DESDE'].isna()]

    st.divider()
    st.subheader("Inflación de referencia")

    inflacion = pd.read_excel("Inflación.xlsx")
    inflacion_usa = pd.read_excel("Inflacion USA.xlsx")

    col_uy, col_usa = st.columns(2)
    col_uy.metric("Último período Uruguay", max(inflacion['Mes']))
    col_usa.metric("Último período USA", max(inflacion_usa['Mes']))

    ultimo_periodo = pd.Period(max(inflacion['Mes']), freq="M")
    inflacion["Mes"] = inflacion["Mes"].apply(lambda x: pd.Period(x, freq="M"))
    inflacion_usa["Mes"] = inflacion_usa["Mes"].apply(lambda x: pd.Period(x, freq="M"))

    dec_inf = st.selectbox('¿Desea agregar un período más?', options=['Si', 'No'], key=1)
    if dec_inf.lower() == 'si':
        pais = st.selectbox('¿Para qué país?', options=['Uruguay', 'Estados Unidos'], key=2)
        if pais.strip().capitalize() == 'Uruguay':
            agregaInflacion(k=3, p='Uruguay')
        elif pais.strip() == 'Estados Unidos':
            agregaInflacion(k=3, p='Estados Unidos')
        else:
            st.info('Aún no se eligió una opción correcta.')
            st.stop()
    elif dec_inf.lower() == 'no':
        pass
    else:
        st.stop()

    inf_12 = round(inflacion_movil(inflacion["Inflacion"], 12) / 100, 4)
    inf_24 = round(inflacion_movil(inflacion["Inflacion"], 24) / 100, 4)
    inf_12_usa = round(inflacion_movil(inflacion_usa["Inflacion"], 12) / 100, 4)
    inf_24_usa = round(inflacion_movil(inflacion_usa["Inflacion"], 24) / 100, 4)

    df['GEOG_LOCL_COD'] = df['GEOG_LOCL_COD'].fillna(0)
    df['COSTO X INFLACION AM'] = df['COSTO_ACTUAL'] * (1 + inf_12)
    df['COSTO X INFLACION 2AM'] = df['COSTO_ACTUAL'] * (1 + inf_24)
    df['COSTO X INFLACION AM USA'] = df['COSTO_ACTUAL'] * (1 + inf_12_usa)
    df['COSTO X INFLACION 2AM USA'] = df['COSTO_ACTUAL'] * (1 + inf_24_usa)
    df["factor_inflacion"] = df["ULTIMO_CAMBIO"].apply(calcular_factor_acumulado)
    df['COSTO X INFLACION'] = df["COSTO_ACTUAL"] * (df["factor_inflacion"] + 0.18)
    df["factor_inflacion_usa"] = df["ULTIMO_CAMBIO"].apply(calcular_factor_acumulado_usa)
    df['COSTO X INFLACION USA'] = df["COSTO_ACTUAL"] * df["factor_inflacion_usa"]
    df.loc[df[df['MONEDA'] == 'USD'].index, 'COSTO X INFLACION'] = df.loc[
        df[df['MONEDA'] == 'USD'].index, 'COSTO X INFLACION USA']
    df['COSTO X INFLACION'] = df['COSTO X INFLACION'].round(2)

    # Reglas de aprobación
    st.divider()
    st.subheader("Reglas de aprobación")
    ap = (df[df['COSTO X INFLACION'] >= df['COSTO_NUEVO']][['LISTA',
                                                            'PESO_VENTA']].groupby('LISTA').sum().reset_index())
    ap.columns = ['LISTA', 'PESO_APROB']
    st.dataframe(ap, use_container_width=True)

    df = df.merge(ap, how='left')
    df['SUGERENCIA'] = 'Rechazar'
    df.loc[df[df['PESO_APROB'] >= 0.95].index, 'SUGERENCIA'] = 'Aprobar'
    df.loc[df[(df['PESO_APROB'] >= 0.9) & (df['PESO_APROB'] < 0.95)].index, 'SUGERENCIA'] = 'Revisar'

    df['GEOG_LOCL_COD'] = df['GEOG_LOCL_COD'].replace(0, pd.NA)
    df = df[dis + ['SUGERENCIA', 'ESTADO']]

    st.divider()
    st.subheader("Revisión por lista")
    proveedores = st.data_editor(
        df, use_container_width=True,
        disabled=dis + ['SUGERENCIA'],
        key="editor_proveedores",
        column_config={"ESTADO": st.column_config.SelectboxColumn("Estado", options=opc, required=True)}
    )

    aux = proveedores[['LISTA', 'ESTADO']].dropna().drop_duplicates()
    if len(aux) > 0:
        counts = aux['LISTA'].value_counts()
        listas_validas = counts[counts == 1].index
        aux = aux[aux['LISTA'].isin(listas_validas)]
        nuevo_df = prov.drop(columns='ESTADO').merge(aux, how='left')
        if 'prov' not in st.session_state or not nuevo_df.equals(st.session_state.prov):
            st.session_state.prov = nuevo_df
            st.rerun()

    cont = st.button('Preparar documentos', use_container_width=False)
    if cont:
        st.session_state.proveedores = proveedores.drop(columns=['SUGERENCIA', 'LISTA-SUP'])
        st.rerun()

# Generación de archivos
if 'proveedores' in st.session_state:
    st.divider()
    st.subheader("Generación de archivos")

    proveedores = st.session_state.proveedores
    carga = proveedores[proveedores['ESTADO'].isin(['APROBADA', 'RECHAZADA'])]
    carga['FECHA_ANALISIS'] = dt

    with st.spinner('Subiendo datos a Snowflake...'):
        success = carga_snow_generic(df=carga[carga['GEOG_LOCL_COD'].isna()], ctx=snow,
                                     database='SANDBOX_PLUS', schema='DWH', table='INPUT_PROVEEDORES_ITEM')
        cursor.execute('''
MERGE INTO SANDBOX_PLUS.DWH.COSTO_PROVEEDORES AS TARGET
USING SANDBOX_PLUS.DWH.INPUT_PROVEEDORES_ITEM AS SOURCE
ON
    TARGET.SUPPLIER = SOURCE.SUPPLIER AND TARGET.LISTA = SOURCE.LISTA AND TARGET.ORIN = SOURCE.ORIN
WHEN MATCHED THEN
    UPDATE SET TARGET.ESTADO = SOURCE.ESTADO, TARGET.FECHA_ANALISIS = SOURCE.FECHA_ANALISIS;
''')
        success = carga_snow_generic(df=carga[~carga['GEOG_LOCL_COD'].isna()], ctx=snow,
                                     database='SANDBOX_PLUS', schema='DWH', table='INPUT_PROVEEDORES_ITEM_LOC')
        cursor.execute('''
MERGE INTO SANDBOX_PLUS.DWH.COSTO_PROVEEDORES AS TARGET
USING SANDBOX_PLUS.DWH.INPUT_PROVEEDORES_ITEM_LOC AS SOURCE
ON
    TARGET.SUPPLIER = SOURCE.SUPPLIER AND TARGET.LISTA = SOURCE.LISTA AND TARGET.ORIN = SOURCE.ORIN AND TARGET.GEOG_LOCL_COD = SOURCE.GEOG_LOCL_COD
WHEN MATCHED THEN
    UPDATE SET TARGET.ESTADO = SOURCE.ESTADO, TARGET.FECHA_ANALISIS = SOURCE.FECHA_ANALISIS;
''')

    st.success("Datos actualizados en Snowflake.")

    it = proveedores[(proveedores['GEOG_LOCL_COD'].isna()) & (proveedores['ESTADO'] == 'APROBADA')]
    loc = proveedores[(~proveedores['GEOG_LOCL_COD'].isna()) & (proveedores['ESTADO'] == 'APROBADA')]

    # Archivo 1: ITEM
    cc = loc[['SUPPLIER', 'LISTA']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['Descripción de cambio de costo'] = 'Costo proveedor ' + cc['SUPPLIER'].astype(str) + '. Lista ' + cc['LISTA'].astype(str)
    cc['Motivo'] = 5
    cc['Fecha activa'] = str(date.today().day) + '-' + meses[date.today().month] + '-' + str(date.today().year)[-2:]
    cc['Estado'] = 'Hoja de trabajo'
    cc['Origen de cambio de costo'] = 'Por artículo'
    camb_costo = cc[['Acción', 'Cambio de costo', 'Descripción de cambio de costo', 'Motivo',
                     'Fecha activa', 'Estado', 'Origen de cambio de costo']].reset_index(drop=True)

    cc = it[['SUPPLIER', 'LISTA', 'ORIN', 'COSTO_NUEVO']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['ID de país de origen'] = 'UY'
    cc['Tipo de cambio de costo'] = 'Costo fijo'
    cc['Recálculo de órdenes'] = 'Costo fijo'
    cc['ID de país de entrega'] = pd.NA
    camb_costo_art = cc[['Acción', 'Cambio de costo', 'SUPPLIER', 'ID de país de origen', 'ORIN',
                         'Tipo de cambio de costo', 'COSTO_NUEVO', 'Recálculo de órdenes',
                         'ID de país de entrega']].reset_index(drop=True)
    camb_costo_art.columns = ['Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen',
                              'Artículo', 'Tipo de cambio de costo', 'Valor de cambio de costo',
                              'Recálculo de órdenes', 'ID de país de entrega']

    camb_costo_ubi = pd.DataFrame({col: [pd.NA] for col in [
        'Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen', 'Artículo',
        'Tipo de ubicación', 'Ubicación', 'Tipo de cambio de costo',
        'Valor de cambio de costo', 'Recálculo de órdenes', 'ID de país de entrega'
    ]})

    cc_pago = pd.DataFrame({'Cambio de Costo': [pd.NA], 'Condiciones de Pago': [pd.NA]})

    output1 = BytesIO()
    with pd.ExcelWriter(output1, engine="odf") as writer:
        camb_costo.to_excel(writer, sheet_name="Cambios_de_costo", index=False)
        camb_costo_art.to_excel(writer, sheet_name="Artículos_de_cambio_de_costo", index=False)
        camb_costo_ubi.to_excel(writer, sheet_name="Ubicaciones_de_cambio_de_costo", index=False)
        cc_pago.to_excel(writer, sheet_name="Cond_Pago_de_Cambios_Costo", index=False)
    output1.seek(0)

    # Archivo 2: ITEM-LOCAL
    cc = loc[['SUPPLIER', 'LISTA']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['Descripción de cambio de costo'] = 'Costo proveedor ' + cc['SUPPLIER'].astype(str) + '. Lista ' + cc['LISTA'].astype(str)
    cc['Motivo'] = 5
    cc['Fecha activa'] = str(date.today().day) + '-' + meses[date.today().month] + '-' + str(date.today().year)[-2:]
    cc['Estado'] = 'Hoja de trabajo'
    cc['Origen de cambio de costo'] = 'Por artículo'
    camb_costo2 = cc[['Acción', 'Cambio de costo', 'Descripción de cambio de costo', 'Motivo',
                      'Fecha activa', 'Estado', 'Origen de cambio de costo']].reset_index(drop=True)

    camb_costo_art2 = pd.DataFrame({col: [pd.NA] for col in [
        'Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen', 'Artículo',
        'Tipo de cambio de costo', 'Valor de cambio de costo', 'Recálculo de órdenes', 'ID de país de entrega'
    ]})

    cc = loc[['SUPPLIER', 'LISTA', 'GEOG_LOCL_COD', 'ORIN', 'COSTO_NUEVO']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['ID de país de origen'] = 'UY'
    cc['Tipo de ubicación'] = 'Tienda'
    cc['Tipo de cambio de costo'] = 'Costo fijo'
    cc['Recálculo de órdenes'] = 'Costo fijo'
    cc['ID de país de entrega'] = pd.NA
    camb_costo_ubi2 = cc[['Acción', 'Cambio de costo', 'SUPPLIER', 'ID de país de origen', 'ORIN',
                          'Tipo de ubicación', 'GEOG_LOCL_COD', 'Tipo de cambio de costo', 'COSTO_NUEVO',
                          'Recálculo de órdenes', 'ID de país de entrega']].reset_index(drop=True)
    camb_costo_ubi2.columns = ['Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen',
                               'Artículo', 'Tipo de ubicación', 'Ubicación', 'Tipo de cambio de costo',
                               'Valor de cambio de costo', 'Recálculo de órdenes', 'ID de país de entrega']

    cc_pago2 = pd.DataFrame({'Cambio de Costo': [pd.NA], 'Condiciones de Pago': [pd.NA]})

    output2 = BytesIO()
    with pd.ExcelWriter(output2, engine="odf") as writer:
        camb_costo2.to_excel(writer, sheet_name="Cambios_de_costo", index=False)
        camb_costo_art2.to_excel(writer, sheet_name="Artículos_de_cambio_de_costo", index=False)
        camb_costo_ubi2.to_excel(writer, sheet_name="Ubicaciones_de_cambio_de_costo", index=False)
        cc_pago2.to_excel(writer, sheet_name="Cond_Pago_de_Cambios_Costo", index=False)
    output2.seek(0)

    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button(
            label="⬇️ Descargar archivo Item.ods",
            data=output1,
            file_name="Item.ods",
            mime="application/vnd.oasis.opendocument.spreadsheet",
            use_container_width=True
        )
    with col_dl2:
        st.download_button(
            label="⬇️ Descargar archivo Item-loc.ods",
            data=output2,
            file_name="Item-loc.ods",
            mime="application/vnd.oasis.opendocument.spreadsheet",
            use_container_width=True
        )

    st.divider()
    st.success("Proceso completado.")
