from queries import check_fechas_unicas
import streamlit as st
import time
import os
import psutil

def check_fechas_ft_promos(cursor):
    st.write('')
    st.write('Inicia check_fechas_ft_promos')
    cursor.execute(check_fechas_unicas)
    check_snow_fechas = cursor.fetch_pandas_all()

    if not check_snow_fechas.empty:
        st.write('Problema: promos con distintas fechas INICIO o FIN')
        st.write('Cerrando el programa...')
        # Give a bit of delay for user experience
        time.sleep(1)
        # Terminate streamlit python process
        pid = os.getpid()
        p = psutil.Process(pid)
        p.terminate()
    st.write('')
    st.write('Termina check_fechas_ft_promos')