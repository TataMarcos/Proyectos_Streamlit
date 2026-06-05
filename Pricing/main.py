import os
import keyboard
if os.getcwd().upper() == 'C:\\USERS\\ARTURO.BOTATA12\\DOCUMENTS\\GITHUB\\PROYECTOS_STREAMLIT\\PRICING':
    keyboard.press_and_release('ctrl+w')        #Close the window
import streamlit as st
import time
import psutil

st.set_page_config(
    page_title="Pricing · Tata Uruguay",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown(
    '<img src="https://grupovierci.uy/wp-content/uploads/2026/04/Agregar-un-titulo-6-scaled.png" style="width:50%;max-height:375px;object-fit:cover;border-radius:8px;">',
    unsafe_allow_html=True
)

col_logo, col_title, col_manual = st.columns([1, 4, 1])

with col_logo:
    st.image("https://grupovierci.uy/wp-content/uploads/2026/04/Tata-Gris.png", width=200)

with col_title:
    st.title("Sistema de Pricing")
    st.caption("Gestión de precios, costos y posicionamiento")

with col_manual:
    st.write("")
    with open("App Pricing.pdf", "rb") as f:
        pdf_bytes = f.read()
    st.download_button(
        label="📄 Descargar manual",
        data=pdf_bytes,
        file_name="App Pricing.pdf",
        mime="application/pdf",
        use_container_width=True
    )

st.divider()
st.write("Hacé clic en cualquier módulo para acceder.")
st.subheader("Módulos disponibles")
st.write("")

def card(href, icon, title, desc):
    return (
        f'<a href="{href}" target="_self" style="display:block;text-decoration:none;color:#1e293b;'
        f'background:#EEF4FF;border:1px solid #BFCFEE;border-radius:10px;padding:18px 14px;">'
        f'<strong style="font-size:1.05rem;">{icon} {title}</strong><br>'
        f'<span style="font-size:0.85rem;color:#475569;">{desc}</span>'
        f'</a>'
    )

r1c1, r1c2, r1c3, r1c4 = st.columns(4)
r2c1, r2c2, r2c3, r2c4 = st.columns(4)

with r1c1:
    st.markdown(card("/Carga_de_precios", "📥", "Carga de precios",
                     "Validá y cargá nuevos precios desde Excel."), unsafe_allow_html=True)
with r1c2:
    st.markdown(card("/Proveedores", "🏭", "Proveedores",
                     "Actualizá costos con ajuste por inflación."), unsafe_allow_html=True)
with r1c3:
    st.markdown(card("/Actualizacion_de_canastas", "🧺", "Canastas",
                     "Gestioná la categorización de productos."), unsafe_allow_html=True)
with r1c4:
    st.markdown(card("/Posicionamiento", "📊", "Posicionamiento",
                     "Configurar estrategia por canasta y local."), unsafe_allow_html=True)
with r2c1:
    st.markdown(card("/Carga_de_familias", "👥", "Familias",
                     "Asigná artículos a familias de precios."), unsafe_allow_html=True)
with r2c2:
    st.markdown(card("/Margenes_objetivo", "🎯", "Márgenes",
                     "Actualizá márgenes objetivo por artículo."), unsafe_allow_html=True)
with r2c3:
    st.markdown(card("/Consulta_de_precios", "🔍", "Consulta de precios",
                     "Consultá precios históricos por local e ítem."), unsafe_allow_html=True)
with r2c4:
    st.markdown(card("/Proceso_de_Pricing", "⚙️", "Proceso de Pricing",
                     "Ejecutá el motor de pricing externo."), unsafe_allow_html=True)

st.divider()
if st.button("Cerrar el programa"):
    st.warning("Cerrando el programa...")
    time.sleep(1)
    psutil.Process(os.getpid()).terminate()