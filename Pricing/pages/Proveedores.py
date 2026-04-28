import pandas as pd
import time
from datetime import datetime, date
import streamlit as st
from utils import descargar_segmento, snowflake_login, get_credentials, carga_snow_generic
from io import BytesIO

# Título de la aplicación en Streamlit
st.title('Carga de costos de proveedores')

# ================================
# 🔐 CONEXIÓN A SNOWFLAKE
# ================================

# Obtiene credenciales desde almacenamiento seguro
credentials_snowflake = get_credentials("snow")

try:
    # Si no hay sesión activa, se crea una nueva conexión
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login(
            user=credentials_snowflake['USER'],
            password=credentials_snowflake['PASS'],
            account=credentials_snowflake['ACCOUNT']
        )
        # Se guarda la sesión para reutilizarla
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        # Si ya existe sesión, se reutiliza
        snow = st.session_state.snow
        user = st.session_state.user
        cursor = st.session_state.cursor

# Si falla la conexión, se detiene la ejecución
except:
    st.write('Aún no se ingresaron las credenciales')
    st.stop()

# ================================
# 📥 CARGA DE DATOS
# ================================

# Si no hay datos cargados en sesión, se descargan desde Snowflake
if 'prov' not in st.session_state:
    cursor.execute('SELECT * FROM SANDBOX_PLUS.DWH.COSTO_PROVEEDORES WHERE ESTADO IS NULL;')
    prov = cursor.fetch_pandas_all().drop(columns='FECHA_ANALISIS').sort_values('LISTA').reset_index(drop=True)
else:
    # Si ya existen, se reutilizan
    prov = st.session_state.prov

# ================================
# ⚙️ CONFIGURACIÓN DE LA INTERFAZ
# ================================

# Opciones permitidas en la columna ESTADO
opc = ["APROBADA", "RECHAZADA"]

# Columnas que NO se pueden editar
dis = list(prov.columns)
dis.remove('ESTADO')

# Fecha actual
dt = date.today().strftime('20%y-%m-%d')

# Diccionario para formatear meses
meses = {1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
         7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic"}

# ================================
# ✏️ EDICIÓN DE DATOS
# ================================

if 'proveedores' not in st.session_state:
    # Tabla editable (solo columna ESTADO)
    proveedores = st.data_editor(prov, use_container_width=True, disabled=dis, key="editor_proveedores",
                                 column_config={"ESTADO": st.column_config.SelectboxColumn("Estado",
                                                                                           options=opc,
                                                                                           required=True)})
    # Botón para procesar datos
    # exp = st.button('Explotar')
    # if exp:
    # # Se seleccionan columnas relevantes y se limpian datos
    #     aux = proveedores[['LISTA', 'ESTADO']].dropna().drop_duplicates()

    #     if len(aux) > 0:
    #         # Elimina LISTAS duplicadas (regla de negocio)
    #         for i in range(aux['LISTA'].value_counts().size):
    #             if aux['LISTA'].value_counts().values[i] > 1:
    #                 aux = aux[aux['LISTA'] != aux['LISTA'].value_counts().index[i]]
    #             else:
    #                 break

    #         # Se actualizan los datos en sesión
    #         st.session_state.prov = prov.drop(columns='ESTADO').merge(aux, how='left')

    #             # Recarga la app
    #         st.rerun()

    # Procesamiento automático
    aux = proveedores[['LISTA', 'ESTADO']].dropna().drop_duplicates()

    if len(aux) > 0:
        # eliminar duplicados de LISTA según tu regla
        counts = aux['LISTA'].value_counts()
        listas_validas = counts[counts == 1].index
        aux = aux[aux['LISTA'].isin(listas_validas)]

        nuevo_df = prov.drop(columns='ESTADO').merge(aux, how='left')

        # Solo actualizar si hubo cambios (evita loops innecesarios)
        if 'prov' not in st.session_state or not nuevo_df.equals(st.session_state.prov):
            st.session_state.prov = nuevo_df
            st.rerun()

    # Botón para avanzar al armado de archivos
    cont = st.button('Preparar documentos')

    if cont:
        st.session_state.proveedores = proveedores
        st.rerun()

# ===============================================
# 📄 GENERACIÓN DE ARCHIVOS Y CARGADO DE TABLAS
# ===============================================

if 'proveedores' in st.session_state:
    proveedores = st.session_state.proveedores
    carga = proveedores[proveedores['ESTADO'].isin(['APROBADA', 'RECHAZADA'])]
    carga['FECHA_ANALISIS'] = dt
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
    it = proveedores[(proveedores['GEOG_LOCL_COD'].isna()) & (proveedores['ESTADO'] == 'APROBADA')]
    loc = proveedores[(~proveedores['GEOG_LOCL_COD'].isna()) & (proveedores['ESTADO'] == 'APROBADA')]

    # ================================
    # 📦 ARCHIVO 1: ITEM
    # ================================

    # Hoja 1
    cc = loc[['SUPPLIER', 'LISTA']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['Descripción de cambio de costo'] = 'Costo proveedor ' + cc['SUPPLIER'].astype(str) + '. Lista ' + cc['LISTA'].astype(str)
    cc['Motivo'] = 5
    cc['Fecha activa'] = str(date.today().day) + '-' +meses[date.today().month] + '-' +str(date.today().year)[-2:]
    cc['Estado'] = 'Hoja de trabajo'
    cc['Origen de cambio de costo'] = 'Por artículo'
    camb_costo = cc[['Acción', 'Cambio de costo', 'Descripción de cambio de costo', 'Motivo',
                     'Fecha activa', 'Estado', 'Origen de cambio de costo']].reset_index(drop=True)
    # Hoja 2
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
    # Renombrar columnas
    camb_costo_art.columns = ['Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen',
                              'Artículo', 'Tipo de cambio de costo', 'Valor de cambio de costo',
                              'Recálculo de órdenes', 'ID de país de entrega']
    # Hoja 3
    camb_costo_ubi = pd.DataFrame()
    camb_costo_ubi['Acción'] = [pd.NA]
    camb_costo_ubi['Cambio de costo'] = pd.NA
    camb_costo_ubi['Proveedor'] = pd.NA
    camb_costo_ubi['ID de país de origen'] = pd.NA
    camb_costo_ubi['Artículo'] = pd.NA
    camb_costo_ubi['Tipo de ubicación'] = pd.NA
    camb_costo_ubi['Ubicación'] = pd.NA
    camb_costo_ubi['Tipo de cambio de costo'] = pd.NA
    camb_costo_ubi['Valor de cambio de costo'] = pd.NA
    camb_costo_ubi['Recálculo de órdenes'] = pd.NA
    camb_costo_ubi['ID de país de entrega'] = pd.NA

    # Hoja 4
    cc = pd.DataFrame()
    cc['Cambio de Costo'] = [pd.NA]
    cc['Condiciones de Pago'] = pd.NA
    
    # Crear archivo en memoria
    output1 = BytesIO()

    with pd.ExcelWriter(output1, engine="odf") as writer:
        camb_costo.to_excel(writer, sheet_name="Cambios_de_costo", index=False)
        camb_costo_art.to_excel(writer, sheet_name="Artículos_de_cambio_de_costo", index=False)
        camb_costo_ubi.to_excel(writer, sheet_name="Ubicaciones_de_cambio_de_costo", index=False)
        cc.to_excel(writer, sheet_name="Cond_Pago_de_Cambios_Costo", index=False)

    # Volver al inicio del archivo
    output1.seek(0)

    # Botón de descarga
    st.download_button(label="Descargar archivo Item.ods", data=output1, file_name="Item.ods",
                       mime="application/vnd.oasis.opendocument.spreadsheet")

    # ================================
    # 📦 ARCHIVO 2: ITEM-LOCAL
    # ================================

    # Hoja 1
    cc = loc[['SUPPLIER', 'LISTA']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['Descripción de cambio de costo'] = 'Costo proveedor ' + cc['SUPPLIER'].astype(str) + '. Lista ' + cc['LISTA'].astype(str)
    cc['Motivo'] = 5
    cc['Fecha activa'] = str(date.today().day) + '-' +meses[date.today().month] + '-' +str(date.today().year)[-2:]
    cc['Estado'] = 'Hoja de trabajo'
    cc['Origen de cambio de costo'] = 'Por artículo'
    camb_costo = cc[['Acción', 'Cambio de costo', 'Descripción de cambio de costo', 'Motivo',
                     'Fecha activa', 'Estado', 'Origen de cambio de costo']].reset_index(drop=True)
    # Hoja 2
    camb_costo_art = pd.DataFrame()
    camb_costo_art['Acción'] = [pd.NA]
    camb_costo_art['Cambio de costo'] = pd.NA
    camb_costo_art['Proveedor'] = pd.NA
    camb_costo_art['ID de país de origen'] = pd.NA
    camb_costo_art['Artículo'] = pd.NA
    camb_costo_art['Tipo de cambio de costo'] = pd.NA
    camb_costo_art['Valor de cambio de costo'] = pd.NA
    camb_costo_art['Recálculo de órdenes'] = pd.NA
    camb_costo_art['ID de país de entrega'] = pd.NA

    # Hoja 3
    cc = loc[['SUPPLIER', 'LISTA', 'GEOG_LOCL_COD', 'ORIN', 'COSTO_NUEVO']].drop_duplicates()
    cc['Acción'] = 'Crear'
    cc['Cambio de costo'] = 324235334
    cc['ID de país de origen'] = 'UY'
    cc['Tipo de ubicación'] = 'Tienda'
    cc['Tipo de cambio de costo'] = 'Costo fijo'
    cc['Recálculo de órdenes'] = 'Costo fijo'
    cc['ID de país de entrega'] = pd.NA
    camb_costo_ubi = cc[['Acción', 'Cambio de costo', 'SUPPLIER', 'ID de país de origen', 'ORIN',
                         'Tipo de ubicación', 'GEOG_LOCL_COD', 'Tipo de cambio de costo', 'COSTO_NUEVO',
                         'Recálculo de órdenes', 'ID de país de entrega']].reset_index(drop=True)
    # Renombrar columnas
    camb_costo_ubi.columns = ['Acción', 'Cambio de costo', 'Proveedor', 'ID de país de origen',
                              'Artículo', 'Tipo de ubicación', 'Ubicación', 'Tipo de cambio de costo',
                              'Valor de cambio de costo', 'Recálculo de órdenes', 'ID de país de entrega']
    # Hoja 4
    cc = pd.DataFrame()
    cc['Cambio de Costo'] = [pd.NA]
    cc['Condiciones de Pago'] = pd.NA
    
    # Crear archivo en memoria
    output2 = BytesIO()

    with pd.ExcelWriter(output2, engine="odf") as writer:
        camb_costo.to_excel(writer, sheet_name="Cambios_de_costo", index=False)
        camb_costo_art.to_excel(writer, sheet_name="Artículos_de_cambio_de_costo", index=False)
        camb_costo_ubi.to_excel(writer, sheet_name="Ubicaciones_de_cambio_de_costo", index=False)
        cc.to_excel(writer, sheet_name="Cond_Pago_de_Cambios_Costo", index=False)

    # Volver al inicio del archivo
    output2.seek(0)

    # Botón de descarga
    st.download_button(label="Descargar archivo Item-loc.ods", data=output2, file_name="Item-loc.ods",
                       mime="application/vnd.oasis.opendocument.spreadsheet")