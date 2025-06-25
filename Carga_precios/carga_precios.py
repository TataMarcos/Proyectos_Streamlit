import pandas as pd
import tkinter as tk
from tkinter import filedialog
import getpass
import time
import pandas as pd
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime

try:
    print('Primero vamos a conectarnos a Snowflake, tené a mano tu celu')
    time.sleep(2)

    # Solicitar usuario y contraseña
    usuario = input("Introducir usuario de Snowflake: ")
    #contrasena = getpass.getpass("Introducir contraseña de Snowflake: ")  # Oculta la entrada de la contraseña
    contrasena = input("Introducir contraseña de Snowflake: ")

    ctx = snowflake.connector.connect(
        user= usuario, # data_pass['snow'][0]['user'],
        password= contrasena ,# data_pass['snow'][0]['pass'],
        account='XZ23267-dp32414',
        database='sandbox_plus',
        schema='dwh'
    )
    cursor = ctx.cursor()


    print("Ahora importá un archivo CSV con las siguientes columnas [CODIGO_TIENDA,ORIN,PVP_NUEVO,EFFECTIVE_DATE{2025-01-25}]. Aguarda unos segundos..")
    time.sleep(4)
    # Abrir ventana para seleccionar el archivo
    root = tk.Tk()
    root.withdraw()  # Oculta la ventana principal
    file_path = filedialog.askopenfilename(title="Selecciona un archivo CSV", filetypes=[("CSV files", "*.csv")])

    # Construir el nuevo nombre del archivo
    dir_name, file_name = os.path.split(file_path)  # Obtener directorio y nombre del archivo
    # print('dir' , dir_name)
    # print('file name',file_name)
    file_name_no_ext, ext = os.path.splitext(file_name)  # Separar nombre y extensión
    # print('file_name_no_ext',file_name_no_ext)
    # print('ext',ext)

    # new_file_path = os.path.join(dir_name, f"{file_name_no_ext}_MODIFICADO.csv")  # Nuevo nombre
    # print('new_file_path',new_file_path)



    if not file_path:
        print("\nNo se seleccionó archivo. Saliendo...")
        exit()

    print(f"\nArchivo seleccionado: {file_path}")

    # Leer el CSV
    df = pd.read_csv(file_path, sep=';')

    df.columns=df.columns.str.upper()

    try:
        df = df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE']]
        
    except KeyError:
        print("\n❌ Hay un error con las columnas. Verifica que todas existan en el archivo.")
        exit()

    # Validar que PVP_NUEVO contenga solo números enteros
    if not pd.api.types.is_integer_dtype(df['PVP_NUEVO']):
        print("\n❌ Error: La columna 'PVP_NUEVO' contiene valores que no son enteros.")
        exit()

    print("\n✅ Archivo validado correctamente. Puedes continuar.")


    print('\nel archivo seleccionado es asi:')
    print(df.head())

    print('\n')
    print('> El archivo tiene un total de',len(df), 'combinaciones para cambios de precio')

    if len(df[df.PVP_NUEVO==0])>0:

        print('> hay',len(df[df.PVP_NUEVO==0]), 'lineas con precio 0$. Voy a descartar estos datos')
    else:
        print('> hay',len(df[df.PVP_NUEVO==0]), 'lineas con precio 0$.')

    df=df[df.PVP_NUEVO!=0]

    f=datetime.now().strftime('%Y%m%d_%H%M%S')

    df['TABLA']=str(f)
    df.columns=df.columns.str.upper()
    df=df[['CODIGO_TIENDA', 'ORIN', 'PVP_NUEVO', 'EFFECTIVE_DATE', 'TABLA']]

    #cargo a snowflake las combinaciones a impactar
    try:
        success, nchunks, nrows, _ = write_pandas(ctx, df, table_name='INPUT_PRICING_ACUMULADO', database='SANDBOX_PLUS', schema='DWH')
        print(f"Éxito: {success}, Chunks: {nchunks}, Filas insertadas: {nrows}")

    except Exception as e:
        print(f"Error al escribir en Snowflake: {e}")
        exit()


    print('\n\n')


    # Preguntar si quiere aplicar todos los filtros
    todos_los_filtros = input("¿Desea aplicar todos los filtros? (si/no): ").strip().lower()

    # Variables de filtro
    filtro_excluidos = False
    filtro_promo = False
    filtro_activo = False
    filtro_precio_diferente = False

    # Si el usuario NO quiere todos los filtros, preguntar individualmente
    if todos_los_filtros == "no":
        filtro_excluidos = input("¿Desea aplicar el filtro de excluidos comercial? (si/no): ").strip().lower() == "si"
        filtro_promo = input("¿Desea aplicar el filtro items en promo? (si/no): ").strip().lower() == "si"
        filtro_activo = input("¿Desea aplicar el filtro de items no activos? (si/no): ").strip().lower() == "si"
        filtro_precio_diferente = input("¿Desea aplicar el filtro de precio diferente al actual? (si/no): ").strip().lower() == "si"
    else:
        filtro_excluidos = filtro_promo = filtro_activo = filtro_precio_diferente = True

    # Mostrar qué filtros se aplicarán
    print("\n✅ Filtros seleccionados:")
    print(f"- Excluidos Comercial: {filtro_excluidos}")
    print(f"- Promo: {filtro_promo}")
    print(f"- Activo: {filtro_activo}")
    print(f"- Precio Diferente: {filtro_precio_diferente}")

    time.sleep(1)





    # Lista de condiciones (se llenará con los filtros activados)
    where_conditions = [
        "fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM sandbox_plus.dwh.resultado_pricing_historico_bis)"
    ]  # Esta condición siempre estará

    # Aplicar los filtros seleccionados
    if filtro_promo:
        where_conditions.append("esta_en_promo = 0")
    if filtro_excluidos:
        where_conditions.append("excluido_comercial = 0")
    if filtro_precio_diferente:
        where_conditions.append("s.stck_precio_venta_dia_civa <> a.pvp_nuevo")
    if filtro_activo:
        where_conditions.append("artc_esta_id = 4")

    # Construir la cláusula WHERE
    where_clause = " AND ".join(where_conditions)



    #ejecuto chequeos
    query_D=f"""with pp as (select distinct a.codigo_tienda,b.familia,count(distinct a.pvp_nuevo) p
    from  sandbox_plus.dwh.INPUT_PRICING_ACUMULADO a
    left join sandbox_plus.dwh.resultado_pricing_historico_bis b on a.orin=b.orin and a.codigo_tienda=b.geog_locl_cod
    where fecha_ejecucion=(select max(fecha_ejecucion) from sandbox_plus.dwh.resultado_pricing_historico_bis)
    and tabla='{f}'
    group by all )

    select a.*, b.familia, b.artc_artc_desc,b.stck_precio_venta_dia_civa from 
    sandbox_plus.dwh.INPUT_PRICING_ACUMULADO  a
    inner  join sandbox_plus.dwh.resultado_pricing_historico_bis b on a.orin=b.orin and a.codigo_tienda=b.geog_locl_cod
    inner join pp on a.codigo_tienda=pp.codigo_tienda and b.familia=pp.familia and p>1
    where fecha_ejecucion=(select max(fecha_ejecucion) from sandbox_plus.dwh.resultado_pricing_historico_bis) and tabla='{f}'
    order by geog_locl_cod,b.familia;"""

    cursor.execute(query_D)
    duplicados = cursor.fetch_pandas_all()

    print('\n'*2)
    print('-------------------------------------------------------------------------')
    print('--------------------HAGO UNOS CHEQUEOS-----------------------------------')


    if len(duplicados)>0:
        duplicados.to_csv(f'{dir_name}\duplicados_{f}.csv')
        print('\n> Se encontraron',len(duplicados), 'duplicados:')
        print(duplicados)
        print(f'Descarto los duplicados. Guardo un archivo para que los revises y vuelvas a cargar. Archvio guardado en {dir_name}\duplicados_{f}')
    else:
        print('> No hay duplicados de items-local')


    #mas chequeos
    query_checks=f"""WITH LIMPIO AS (
    SELECT DISTINCT tabla,effective_date,codigo_tienda,FAMILIA,PVP_NUEVO FROM (
    select distinct tabla, codigo_tienda,a.effective_date ,A.orin,b.familia, pvp_nuevo, row_number() over (partition by geog_locl_cod,b.familia order by a.pvp_nuevo) rn_precio
    from  sandbox_plus.dwh.INPUT_PRICING_ACUMULADO   a 
    left join sandbox_plus.dwh.resultado_pricing_historico_bis b on a.orin=b.orin and a.codigo_tienda=b.geog_locl_cod 
    where fecha_ejecucion=(select max(fecha_ejecucion) from sandbox_plus.dwh.resultado_pricing_historico_bis) and tabla='{f}'
    qualify rn_precio=1
    order by 1,3,2))


    select DISTINCT  B.orin,b.geog_locl_cod, a.pvp_nuevo,
    null effective_date,
        b.familia,
        b.integrantes_familia,
        b.canasta,
        b.artc_artc_desc,
    artc_esta_id estado,
    esta_en_promo,
    excluido_comercial,
    case when s.stck_precio_venta_dia_civa<>a.pvp_nuevo then 1 else 0 end precio_Diferente

        from sandbox_plus.dwh.resultado_pricing_historico_bis b 
        inner join 
            LIMPIO A on a.FAMILIA=b.FAMILIA and a.codigo_tienda=b.geog_locl_cod
        inner join 
            mstrdb.dwh.ft_stock s on s.artc_artc_id=b.artc_artc_id and s.geog_locl_id=b.geog_locl_id and s.tiem_dia_id=current_date-1 
        
        where fecha_ejecucion=(select max(fecha_ejecucion) from sandbox_plus.dwh.resultado_pricing_historico_bis)"""



    cursor.execute(query_checks)
    checks = cursor.fetch_pandas_all()
    checks.columns=checks.columns.str.lower()
    checks.integrantes_familia=checks.integrantes_familia.astype(int)


    print('\n> Hay un total de ',len(checks),'combinaciones local sin aplicar filtros')

    familia=checks[checks.integrantes_familia>1]
    print('> Hay', len(familia[['familia','geog_locl_cod']].drop_duplicates()) ,'combinaciones familia-local')
    print('>>> Generan un total de ', len(familia[['orin','geog_locl_cod']]) ,'combinaciones item-local')

    ex=checks[checks.excluido_comercial==1]
    if len(ex[['orin','geog_locl_cod']])>0:
        print('> Hay ', len(ex[['orin','geog_locl_cod']]) ,'combinaciones item-local excluidos por comercial')
        print('>>> Hay ', len(ex[['orin']].drop_duplicates()) ,'items unicos excluidos por comercial')
    else: print('> No hay items excluidos por comercial para cargar')

    promo=checks[checks.esta_en_promo==1]
    if len(promo[['orin','geog_locl_cod']])>0:
        print('> Hay ', len(promo[['orin','geog_locl_cod']]) ,'combinaciones item-local en promo')
    else: print('> No hay items en promo')

    activo=checks[checks.estado!=4]
    if len(activo[['orin','geog_locl_cod']])>0:
        print('> Hay ', len(activo[['orin','geog_locl_cod']]) ,'combinaciones item-local no activas')
    else: print('Todas las combinaciones estan activas')

    precio=checks[checks.precio_diferente==0]
    print('> Hay ', len(precio[['orin','geog_locl_cod']]) ,'combinaciones item-local con ese mismo precio')



    #una vez validado inserto los precios en recopilacion modificaciones precio
    query_P=f"""
    insert into sandbox_plus.dwh.recopilacion_modificaciones_precios_bis
    WITH LIMPIO AS (
    SELECT DISTINCT tabla,effective_date,codigo_tienda,FAMILIA,PVP_NUEVO FROM (
    select distinct tabla, codigo_tienda,a.effective_date ,A.orin,b.familia, pvp_nuevo, row_number() over (partition by geog_locl_cod,b.familia order by a.pvp_nuevo) rn_precio
    from  sandbox_plus.dwh.INPUT_PRICING_ACUMULADO   a 
    left join sandbox_plus.dwh.resultado_pricing_historico_bis b on a.orin=b.orin and a.codigo_tienda=b.geog_locl_cod 
    where fecha_ejecucion=(select max(fecha_ejecucion) from sandbox_plus.dwh.resultado_pricing_historico_bis) and tabla='{f}'
    qualify rn_precio=1
    order by 1,3,2))

    , filtro as (
    select B.orin,b.geog_locl_cod, a.pvp_nuevo,
    a.tabla,
    null prioridad,
    a.effective_date,
        b.familia,
        b.integrantes_familia,
        b.canasta,
        b.artc_artc_desc,
        b.stck_ayer, 
        b.costo_unitario,
        b.precio_sugerido, 
        b.precio_competencia_utilizado_por_modelo,
        b.stck_precio_venta_dia_civa,
        b.fecha_ejecucion fecha_modelo,
        b.unidades_vendidas_treinta_dias,
        b.VNTA_TREINTA_DIAS,
        CASE WHEN b.CANASTA = 'referente' THEN 1
        WHEN b.CANASTA = 'mercado' THEN 2
        WHEN b.CANASTA = 'surtido' THEN 3
        WHEN b.CANASTA = 'marca propia' THEN 4
        WHEN b.CANASTA IS NULL THEN 5
        ELSE 6 -- Para cualquier otro valor que pueda aparecer
        END PRIORIDAD_CANASTA,
        null prioridad_fuente,
        current_date()  fecha_analisis
        from sandbox_plus.dwh.resultado_pricing_historico_bis b 
        inner join 
            LIMPIO A on a.FAMILIA=b.FAMILIA and a.codigo_tienda=b.geog_locl_cod
        inner join 
            mstrdb.dwh.ft_stock s on s.artc_artc_id=b.artc_artc_id and s.geog_locl_id=b.geog_locl_id and s.tiem_dia_id=current_date-1 
        
        where {where_clause}
        
        order by familia,b.geog_locl_cod) 

    select distinct CANASTA,
    FAMILIA,
    INTEGRANTES_FAMILIA,
    ORIN,
    GEOG_LOCL_COD,
    STCK_PRECIO_VENTA_DIA_CIVA,
    PRECIO_COMPETENCIA_UTILIZADO_POR_MODELO,
    PRECIO_SUGERIDO,
    pvp_nuevo PRECIO_NUEVO_AJUSTADO,
    COSTO_UNITARIO,
    UNIDADES_VENDIDAS_TREINTA_DIAS,
    VNTA_TREINTA_DIAS,
    STCK_AYER,
    PRIORIDAD_CANASTA,
    PRIORIDAD_FUENTE,
    FECHA_MODELO,
    row_number() over (partition by geog_locl_cod order by UNIDADES_VENDIDAS_TREINTA_DIAS desc) RN,	
    null LIMITE_CAMBIOS,
    current_date FECHA_ANALISIS,
    EFFECTIVE_DATe,
    tabla,'{usuario}' USUARIO


    from filtro 

    """

    cursor.execute(query_P)

    #ultimo chequeo
    query_final=f"""select orin,geog_locl_cod codigo_tienda,  precio_nuevo_ajustado PVP_NUEVO
    FROM SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS 
    where 
    fecha_analisis=CURRENT_DATE and tabla='{f}'"""

    cursor.execute(query_final)
    final = cursor.fetch_pandas_all()
    final.columns=final.columns.str.upper()
    final.to_csv(f'{dir_name}\Pricing_final_{f}.csv')

    print('')
    print('\nFinalmente quedaron ',len(final), 'combinaciones para impactar')


    #genero archivos para la web
    query_C=f"""SELECT 
    '0' AS ZONE_NODE_TYPE,
    NULL AS ZONE_GROUP_ID,
    NULL AS ZONE_ID,
    GEOG_LOCL_COD AS LOCATION,
    ORIN AS ITEM,
    NULL AS DIFF_ID,
    NULL AS SKULIST,
    NULL AS LINK_CODE,
    '2' AS CHANGE_TYPE,
    PRECIO_NUEVO_ajustado AS CHANGE_AMOUNT,
    NULL AS CHANGE_PERCENT,
    'EA' AS CHANGE_SELLING_UOM,
    NULL AS PRICE_GUIDE_ID,
    EFFECTIVE_DATE,
    '2' AS REASON_CODE,
    '0' AS VENDOR_FUNDED_IND,
    NULL AS FUNDING_TYPE,
    NULL AS FUNDING_AMOUNT,
    NULL AS FUNDING_AMOUNT_CURRENCY,
    NULL AS FUNDING_PERCENT,
    NULL AS DEAL_ID,
    NULL AS DEAL_DETAIL_ID
    FROM SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS 
    where 
    fecha_analisis=CURRENT_DATE and tabla='{f}'
    """

    cursor.execute(query_C)
    datos = cursor.fetch_pandas_all()

    datos['CHANGE_AMOUNT'] = datos['CHANGE_AMOUNT'].astype(int)
    datos['EFFECTIVE_DATE'] = pd.to_datetime(datos['EFFECTIVE_DATE'])

    # Cambiar el formato a día/mes/año
    datos['EFFECTIVE_DATE'] = datos['EFFECTIVE_DATE'].dt.strftime('%d/%m/%Y')

    d=datos[['LOCATION','EFFECTIVE_DATE','CHANGE_AMOUNT']].groupby(['LOCATION','EFFECTIVE_DATE']).count().reset_index().sort_values(by='CHANGE_AMOUNT', ascending=False)
    d.rename(columns={'CHANGE_AMOUNT':'COMBINACIONES'}, inplace=True)
    print('Combinaciones por local:')
    print(d)



    def dividir_y_guardar_csv(datos, carpeta_destino, filas_por_archivo=2999):
        total_filas = len(datos)
        num_archivos = total_filas // filas_por_archivo + (1 if total_filas % filas_por_archivo else 0)

        if not os.path.exists(carpeta_destino):
            os.makedirs(carpeta_destino)      

        for i in range(num_archivos):
            inicio = i * filas_por_archivo
            fin = inicio + filas_por_archivo
            segmento = datos.iloc[inicio:fin]
            # fecha_hora_actual = datetime.now().strftime("%d%m_%H%M%S")
            nombre_archivo = f"Pricing_WEB_{f}_file_{i+1}.csv"
            ruta_completa = os.path.join(carpeta_destino, nombre_archivo)
            segmento.to_csv(ruta_completa, index=False, sep= ";")

            print(f"Archivo guardado en:  {ruta_completa}")
    print('\n'*2)  
    dividir_y_guardar_csv(datos, dir_name)    


    borrado = input("Desea registrar estos precios como enviados? (si/no) ").strip().lower()
    if borrado=='no':
        cursor.execute(f"delete from SANDBOX_PLUS.DWH.RECOPILACION_MODIFICACIONES_PRECIOS_BIS  where  tabla='{f}'")
        print('Datos descartados')
    else: print('Datos grabados')

    time.sleep(3)

    print("\nPrograma finalizado. Manteniéndose abierto...")
    while True:
        pass  # El programa sigue corriendo hasta que lo cierren manualmente

except:
    print('-------------------------------------------------')
    print('\nAlgo falló')
    print("\nPrograma finalizado. Manteniéndose abierto...")
    while True:
        pass 