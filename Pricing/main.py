import keyboard
import os

if os.getcwd().upper() == 'C:\\USERS\\ARTURO.BOTATA12\\DOCUMENTS\\GITHUB\\PROYECTOS_STREAMLIT\\PRICING':
    keyboard.press_and_release('ctrl+w')        #Close the window

import streamlit as st
import time
import psutil

st.title('Pricing')

st.write('')
st.write('Bienvenido al programa de Pricing. Elegí en el menú a tu izquierda la función que necesites.')

exit_app = st.button("Cerrar el programa.")
if exit_app:
    st.write('Cerrando el programa...')
    # Give a bit of delay for user experience
    time.sleep(1)
    # Terminate streamlit python process
    pid = os.getpid()
    p = psutil.Process(pid)
    p.terminate()