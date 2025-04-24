from utils import get_credentials, snowflake_login, lock_excel_file, unlock_excel_file, copy_excel_file
from obtener_fechas_unicas import check_fechas_ft_promos
from obtener_ofertas_internas import descargar_ofertas_internas
from obtener_promos_empiezan_tomorrow import descargar_promos_empiezan_dos_dias
from obtener_inducidas import descargar_inducidas
from obtener_promos_url import descargar_promos_url
from cargar_excel_BI import cargar_excel_BI
from carga_drives.carga_sheet_promos import cargar_sheet_promos
from carga_drives.carga_reporte_articulos import cargar_reporte_articulos
from obtener_tata_com import descargar_tata_com
from obtener_tata_com_solo_mdeo import descargar_tata_com_solo_mdeo
from config import *
import streamlit as st

st.title('FT_PROMOS')
st.write('')

if __name__ == '__main__':

    if 'lock' not in st.session_state:
        unlock_excel_file(excels['excel_BI'])
        
        copy_excel_file(excels['excel_BI']) #Genera un respaldo del excel de BI y borra respaldos viejos

        lock_excel_file(excels['excel_BI']) #Bloqueo Excel Base BI
        st.session_state.lock = True

    # Obtenemos credenciales para enviar mail automatico
    # if 'amc' not in st.session_state:
    #     automated_mail_credentials = get_credentials('correo_automated')

    # Realizamos conexion
    if 'snow' not in st.session_state:
        user, cursor, snow = snowflake_login()
        st.session_state.user = user
        st.session_state.cursor = cursor
        st.session_state.snow = snow
    else:
        snow = st.session_state.snow  # Reuse the existing Snowflake session
        user = st.session_state.user
        cursor = st.session_state.cursor

    # Chequeamos las fechas
    if 'fechas' not in st.session_state:
        check_fechas_ft_promos(cursor) # una promo que abarque más de dos meses me llama la atención
        st.session_state.fechas = True

    if 'urls_main' not in st.session_state:
        urls = descargar_promos_empiezan_dos_dias(cursor) # Check: promos a cargar porque empiezan hoy o el finde (si estoy a Lunes)
        st.write(urls)
        st.session_state.urls_main = urls
    else:
        urls = st.session_state.urls_main

    lista_promos = ['Ofertas Internas', 'URL', 'Inducidas',
                    'Tata.com', 'Tata.com - Solo Montevideo']
    
    if 'inicio' not in st.session_state:
        promos = st.multiselect('Seleccione las promociones a cargar:', lista_promos)
        inicio = st.button('INICIAR')
        if inicio:
            st.session_state.inicio = True
            st.session_state.promos = promos
    else:
        promos = st.session_state.promos
        inicio = st.session_state.inicio

    if inicio:
        if 'Ofertas Internas' in promos:
            descargar_ofertas_internas(cursor) #Generar file Ofertas Interna
        if 'URL' in promos:
            descargar_promos_url(cursor, urls) #Descargamos las promos cuyas url estan indicadas en el URLs.txt
        if 'Inducidas' in promos:
            descargar_inducidas(cursor) #Generar file Inducidas
        if 'Tata.com' in promos:
            descargar_tata_com(cursor)
        if 'Tata.com - Solo Montevideo' in promos:
            descargar_tata_com_solo_mdeo(cursor)

        cargar_excel_BI(cursor, excels['excel_BI']) #Modificar excel BI

        # cargar_sheet_promos(cursor) #Cargamos las sheets que alimentan los controles en los drives de Lu Yanneo

        # cargar_reporte_articulos(cursor) #Cargamos la sheet de articulos en proximas promos

        snow.commit()
        snow.close()
