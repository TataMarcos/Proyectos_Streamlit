# %%
import pandas as pd
from utils import snowflake_login

# %%
user, cursor, snow = snowflake_login()

# %%
evento = pd.read_excel('Cotillon navideño Surtido local.xlsx')

# %% dSM300wpRLSS
evento.info()

# %%
evento.columns = ['GEOG_LOCL_COD', 'ORIN']
evento.info()

# %%
cursor.execute('SELECT ARTC_ARTC_COD, ORIN FROM MSTRDB.DWH.LU_ARTC_ARTICULO;')
art = cursor.fetch_pandas_all()
art

# %%
art.info()

# %%
art['ORIN'] = art['ORIN'].astype('str')
evento['ORIN'] = evento['ORIN'].astype('str')
evento = evento.merge(art)
evento

# %%
evento['PROM_FECHA_INICIO'] = '2025-09-29'
evento['PROM_FECHA_INICIO'] = pd.to_datetime(evento['PROM_FECHA_INICIO'])
evento['PROM_FECHA_FIN'] = '2025-10-03'
evento['PROM_FECHA_FIN'] = pd.to_datetime(evento['PROM_FECHA_FIN'])
evento['EVENTO_ID'] = 2547
evento['PRONOSTICO_VENTA'] = 0
evento['STOCK_INICIAL_PROMO'] = 0
evento['PROM_PVP_OFERTA'] = 0
evento['PROM_LOCAL_ACTIVO'] = 0
evento['PROM_ESTIBA'] = 0
evento

# %%
evento.info()

# %%
evento_final = evento[['PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'ARTC_ARTC_COD', 'EVENTO_ID', 'PRONOSTICO_VENTA',
                       'STOCK_INICIAL_PROMO', 'GEOG_LOCL_COD', 'PROM_PVP_OFERTA', 'PROM_LOCAL_ACTIVO', 'PROM_ESTIBA',
                       'ORIN']].astype({'ARTC_ARTC_COD':'str', 'GEOG_LOCL_COD':'str', 'ORIN':'str'})
evento_final

# %%
evento_final.info()

# %%
evento_final.to_csv('G:/Unidades compartidas/Inteligencia de Negocio/Promos/Eventos adhoc/Evento adhoc.csv')

# %%



