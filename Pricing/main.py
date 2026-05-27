import streamlit as st
import time
import os
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
st.write("Seleccioná en el menú izquierdo la función que necesitás realizar.")
st.subheader("Módulos disponibles")
st.write("")

r1c1, r1c2, r1c3, r1c4 = st.columns(4)
r2c1, r2c2, r2c3, r2c4 = st.columns(4)

with r1c1:
    st.info("**📥 Carga de precios**\n\nValidá y cargá nuevos precios desde Excel.")
with r1c2:
    st.info("**🏭 Proveedores**\n\nActualizá costos de proveedores con ajuste por inflación.")
with r1c3:
    st.info("**🧺 Canastas**\n\nGestioná la categorización de productos.")
with r1c4:
    st.info("**📊 Posicionamiento**\n\nConfigurar estrategia de posicionamiento por canasta y local.")
with r2c1:
    st.info("**👥 Familias**\n\nAsigná artículos a familias de precios.")
with r2c2:
    st.info("**🎯 Márgenes**\n\nActualizá márgenes objetivo por artículo.")
with r2c3:
    st.info("**🔍 Consulta**\n\nConsultá precios históricos por local e ítem.")
with r2c4:
    st.info("**⚙️ Proceso**\n\nEjecutá el proceso de pricing en el motor externo.")

st.divider()
if st.button("Cerrar el programa"):
    st.warning("Cerrando el programa...")
    time.sleep(1)
    psutil.Process(os.getpid()).terminate()