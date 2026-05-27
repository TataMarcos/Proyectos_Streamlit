import streamlit as st

st.set_page_config(page_title="Proceso de Pricing", page_icon="⚙️", layout="wide")

st.title("⚙️ Proceso de Pricing")
st.divider()

st.write("Hacé clic en el siguiente botón para acceder al motor de pricing:")
st.link_button("Abrir motor de Pricing", "http://172.25.12.120:8081/")
