import sys
import numpy as np
import pandas as pd
import snowflake.connector
import json
from datetime import datetime, timedelta
import os
import re
from snowflake.connector.pandas_tools import write_pandas
from dateutil.relativedelta import relativedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from config import *

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

#credentials = Credentials.from_service_account_file('C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\leo_usuario_servicio_credenciales.json', scopes=scopes)
credentials = Credentials.from_service_account_file(jsons['credentials_mail_servicio'], scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

def execute_if_needed(log_path):
    '''
    Intends to retrieve the date from the execution_log.txt file.
    Returns True if:
        - the execution_log.txt file does not exist
        - the date stored in execution_log.txt < today
    '''
    try:
        # Open the log file in read mode

        with open(log_path, 'r') as log_file:
            # Read the content of the first line
            content = log_file.readline().strip()

            # Extract the date if the format is correct
            if content.startswith("Last date sent: "):
                log_date_str = content.replace("Last date sent: ", "")

                # Convert the extracted log date string to a datetime object
                log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()

                # Compare log date with today's date
                if log_date < datetime.today().date():
                    return True
                else:
                    print('Control Articulos Duplicados: el control ya fue enviado hoy')
                    return False
            else:
                print("Control Articulos Duplicados: Log format is incorrect")
                sys.exit()

    except FileNotFoundError:
        # If the log file doesn't exist, we allow the function to run
        return True

def descargar_articulos_duplicados(cursor):

    query = '''
    SELECT
        DISTINCT
        FP.PROM_FECHA_INICIO,
        FP.PROM_FECHA_FIN,
        FP.EVENTO_ID,
        LAA.ARTC_ARTC_COD,
        LAA.ORIN,
        LAA.ARTC_ARTC_DESC
    FROM
        MSTRDB.DWH.FT_PROMOS AS FP
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON FP.ARTC_ARTC_ID = LAA.ARTC_ARTC_ID
    WHERE
        FP.PROM_FECHA_FIN >= CURRENT_DATE
    '''

    cursor.execute(query)
    df = cursor.fetch_pandas_all()

    df['EVENTO_ID'] = df['EVENTO_ID'].astype(str)

    df2 = pd.DataFrame()
    rows_to_add = []

    for row in range(df.shape[0]):
        start_date = pd.to_datetime(df.iloc[row].iloc[0])
        end_date = pd.to_datetime(df.iloc[row].iloc[1])

        # DISTINTOS MESES
        if start_date.month != end_date.month:

            # PRIMER MES
            rows_to_add.append([start_date, start_date.replace(day=start_date.days_in_month)] + list(df.iloc[row, 2:]))

            # MESES ENTRE MEDIO (completos)
            meses = pd.date_range(
                start=start_date.replace(day=1) + pd.DateOffset(months=1),
                end=end_date.replace(day=1) - pd.DateOffset(days=1),
                freq='MS'
            )

            for month in meses:
                rows_to_add.append([month, month.replace(day=month.days_in_month)] + list(df.iloc[row, 2:]))

            # ULTIMO MES
            rows_to_add.append([end_date.replace(day=1), end_date] + list(df.iloc[row, 2:]))

        # MISMO MES
        else:
            rows_to_add.append([start_date, end_date] + list(df.iloc[row, 2:]))

    if rows_to_add:
        df2 = pd.concat([
            pd.DataFrame(rows_to_add, columns=df.columns)
        ],
            ignore_index=True)

    # Check
    df2['MES_INICIO'] = df2['PROM_FECHA_INICIO'].dt.to_period('M')
    df2['MES_FIN'] = df2['PROM_FECHA_FIN'].dt.to_period('M')

    if not df2[df2['MES_INICIO'] != df2['MES_FIN']].empty:
        print('Error: para una misma fila tenemos distintos meses')
        sys.exit()

    df2.drop(['MES_INICIO', 'MES_FIN'], axis=1, inplace=True)

    df3 = pd.DataFrame()

    for i in range(df2.shape[0]):
        # for i in range(1):
        df_aux = df2.iloc[[i]]

        dates = pd.date_range(start=pd.to_datetime(df_aux['PROM_FECHA_INICIO'].iloc[0]),
                              end=pd.to_datetime(df_aux['PROM_FECHA_FIN'].iloc[0]), freq='D')
        exploded_df = pd.DataFrame({'DATE': dates})

        # Merge the original DataFrame with the exploded DataFrame
        exploded_df_2 = pd.merge(exploded_df, df_aux, how='left', left_on='DATE', right_on='PROM_FECHA_INICIO')

        # Fill NaN values in the merged columns using ffill()
        exploded_df_2.ffill(inplace=True)
        df3 = pd.concat([df3, exploded_df_2])

    duplicados = df3.groupby(['ORIN', 'DATE'])['EVENTO_ID'].nunique().reset_index()
    duplicados = duplicados[duplicados['EVENTO_ID'] > 1]

    duplicados['K'] = duplicados['ORIN'] + '/' + duplicados['DATE'].astype(str)

    df3['K'] = df3['ORIN'] + '/' + df3['DATE'].astype(str)
    df4 = df3[
        df3['K'].isin(duplicados['K'])
    ]

    df4.drop(['DATE', 'K', 'PROM_FECHA_INICIO', 'PROM_FECHA_FIN'], axis=1, inplace=True)
    df4.drop_duplicates(inplace=True)
    df4['EVENTO_ID'] = df4['EVENTO_ID'].astype(int).astype(str)

    df4 = df4.merge(
        df[['EVENTO_ID', 'PROM_FECHA_INICIO', 'PROM_FECHA_FIN', 'ORIN']].drop_duplicates(),
        on=['EVENTO_ID', 'ORIN'],
        how='left'
    )

    df4.columns = ['Evento ID', 'Estadistico', 'ORIN', 'Descripcion Articulo', 'Inicio', 'Fin']

    excel = r'T:\BI\Comercial\ofertas\Base ofertas para BI.xlsx'
    eventos = pd.read_excel(excel, sheet_name='Eventos')
    eventos['evento_id'] = eventos['evento_id'].astype(str)
    eventos.rename({'evento_id': 'Evento ID', 'evento_desc': 'Promo'}, axis=1, inplace=True)

    df5 = df4.merge(
        eventos[['Evento ID', 'Promo']],
        on=['Evento ID'],
        how='left'
    )

    query = '''
    SELECT
        DISTINCT 
        GROUPS.GROUP_NAME,
        DEPS.DEPT_NAME,
        LAA.ORIN
    FROM
        MSTRDB.DWH.DIVISION AS DIVISION
        INNER JOIN MSTRDB.DWH.GROUPS AS GROUPS ON DIVISION.DIVISION = GROUPS.DIVISION
        INNER JOIN MSTRDB.DWH.DEPS AS DEPS ON DEPS.GROUP_NO = GROUPS.GROUP_NO
        INNER JOIN MSTRDB.DWH.ITEM_MASTER AS IM ON IM.DEPT = DEPS.DEPT
        INNER JOIN MSTRDB.DWH.LU_ARTC_ARTICULO AS LAA ON LAA.ORIN = IM.ITEM
    WHERE
        DIVISION.DIV_NAME ILIKE '%TATA%'
    '''

    cursor.execute(query)
    grupos = cursor.fetch_pandas_all()
    grupos['ORIN'] = grupos['ORIN'].astype(str)
    grupos.rename(
        {
            'GROUP_NAME': 'Grupo',
            'DEPT_NAME':'Departamento'
         }, axis=1, inplace=True)

    df6 = df5.merge(grupos, on = 'ORIN', how = 'inner')

    df_final = df6[[
        'Grupo',
        'Departamento',
        'ORIN',
        'Estadistico',
        'Descripcion Articulo',
        'Evento ID',
        'Promo',
        'Inicio',
        'Fin', ]]
    df_final.sort_values(by=['Grupo', 'Departamento', 'ORIN'], inplace=True)

    #check_un_orin_por_evento = df_final[['ORIN', 'Evento ID']].value_counts().reset_index()

    #path = 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio\\ft_promos_automatico\\Articulos duplicados'
    path = paths['articulos_duplicados']

    # Genero el excel general
    file_name = 'Articulos en multiples promos para una misma fecha.xlsx'
    file_path = os.path.join(path, file_name)

    df_final.to_excel(file_path, index=False)

def update_mail_log(log_path):
    '''
    Opens the mail_sent_log.txt file in w mode, deletes all content,
    and saves today's date
    '''

    try:
        today = datetime.today().date()
        with open(log_path, 'w') as log_file:
            log_file.write(f"Last date sent: {today}\n")
    except Exception as e:
        print(f"Error updating log: {e}")

def send_email(message: MIMEMultipart, sender_address: str, sender_pass: str) -> bool:
    try:
        # SMTP session
        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()  # Start TLS encryption
        session.login(sender_address, sender_pass)  # Login with credentials
        text = message.as_string()
        session.sendmail(sender_address, message["To"].split(","), text)  # Send email
        session.quit()  # Close the session

        return True
    except Exception as err:
        print(f"Error sending email: {err}")
        return False

def create_email(
    receiver_address: str,
    subject: str,
    file_path: str,
    message_type: str,
    automated_mail_credentials: dict) -> bool:

    '''
    Enviamos a Matias La Cava y Lucia Yanneo el reporte general de Articulos Duplicados
    El mismo queda guardado en el directorio "Articulos duplicados"
    '''

    sender_address = automated_mail_credentials['USER']
    sender_pass = automated_mail_credentials['PASSCODE']

    # Create the email object
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = subject

    if message_type not in ['GENERAL', 'ESPECIFICO']:
        raise ValueError(f"Invalid message type: {message_type}. Must be 'GENERAL' or 'ESPECIFICO'.")

    content = automated_mail_credentials['CONTENT'][message_type]

    # Attach the message content
    message.attach(MIMEText(content, 'plain'))

    # Attach the Excel file
    if file_path:
        attachment_name = os.path.basename(file_path)
        with open(file_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={attachment_name}')
            message.attach(part)

    # Send the email

    envio = send_email(message=message, sender_address=sender_address, sender_pass=sender_pass)

    return envio  # This returns True if successful, False otherwise

def articulos_duplicados_general(automated_mail_credentials, cursor):

    # path del adjunto

    path = paths['articulos_duplicados']
    file_name = 'Articulos en multiples promos para una misma fecha.xlsx'
    file_path = os.path.join(path, file_name)

    email_recipients = mails_articulos_duplicados_general

    # Create and send the email
    email = create_email(
        receiver_address = ", ".join(email_recipients),
        subject="Check Promos: articulos presentes en multiples promos",
        message_type='GENERAL',
        file_path=file_path,
        automated_mail_credentials=automated_mail_credentials
    )

    # Check if the email was sent successfully
    if email:
        print("Reporte general - Mail enviado correctamente")
    else:
        print("Reporte general - el envio del mail fallo")

def obtener_mails_a_enviar():

    #url = 'https://docs.google.com/spreadsheets/d/1FIUrtEfKU18KGfjQDroY-Sgadrke1NH8GYiA6wJ8DeI/edit?gid=0#gid=0'
    url = urls['mails_articulos_dupolicados']

    codigo_url = url.split('/')[-2]
    gs = gc.open_by_key(codigo_url)

    try:
        worksheet = gs.worksheet('Mails')
    except gspread.exceptions.WorksheetNotFound:
        print(f"{gs.title} - Mails worksheet not found")
        sys.exit()

    df = get_as_dataframe(worksheet, header=2)
    df = df.dropna(how='all')
    df = df.iloc[:, 0:3]
    df = df[~df['Mail/s'].isna()]

    if not df[['Grupo', 'Departamento']].drop_duplicates().shape[0] == df.shape[0]:
        print(f'{gs.title} Error: combinacion Grupo - Departamento duplicada')
        print('Ver drive Articulos duplicados - Mails')
        sys.exit()

    mails = pd.DataFrame(columns=['Email', 'Grupo', 'Departamento'])
    rows = []

    for index, row in df.iterrows():
        emails = row['Mail/s'].split(',')  # Split the emails
        for email in emails:
            rows.append({'Email': email.strip(), 'Grupo': row['Grupo'], 'Departamento': row['Departamento']})

    # Convert the list of dictionaries into a DataFrame
    mails = pd.DataFrame(rows)

    return mails

def articulos_duplicados_especifico(automated_mail_credentials):

    path = paths['articulos_duplicados']
    file_name = 'Articulos en multiples promos para una misma fecha.xlsx'
    file_path = os.path.join(path, file_name)

    df_final = pd.read_excel(file_path)

    mails = obtener_mails_a_enviar()

    consolidado = mails.merge(
        df_final,
        on=['Grupo', 'Departamento'],
        how='inner'
    )

    emails_sent = []
    failed_emails = []

    for correo in consolidado['Email'].unique():
        df_aux = consolidado[consolidado['Email'] == correo]

        if df_aux.empty:
            continue  # Skip to the next email if no data
        reporte_por_usuario = df_final[df_final['Grupo'].isin(df_aux['Grupo'])]
        reporte_por_usuario = reporte_por_usuario[reporte_por_usuario['Departamento'].isin(df_aux['Departamento'])]

        specific_file_path = os.path.join('Articulos duplicados', 'Reporte por responsable',
                                 f"Articulos en multiples promos para una misma fecha, {correo}.xlsx")
        reporte_por_usuario.to_excel(specific_file_path, index=False)

        # Create and send the email
        email = create_email(
            receiver_address=f"{correo}",
            subject="Check Promos: articulos presentes en multiples promos",
            message_type='ESPECIFICO',
            file_path=specific_file_path,
            automated_mail_credentials=automated_mail_credentials
        )

        if email:
            emails_sent.append(correo)
        else:
            failed_emails.append(correo)

    # Print the summary of sent and failed emails
    if not failed_emails:
        print("Reporte especifico - Mail enviado correctamente")
    else:
        print("Reporte especifico - el envio del mail fallo")

def articulos_duplicados(automated_mail_credentials, cursor):

    print('Inicia enviar_articulos_duplicados')

    # path del loggeo

    path = paths['articulos_duplicados']
    file_name = 'mail_sent_log.txt'
    log_path = os.path.join(path, file_name)

    if execute_if_needed(log_path):
        descargar_articulos_duplicados(cursor)
        articulos_duplicados_general(automated_mail_credentials, cursor)
        articulos_duplicados_especifico(automated_mail_credentials)
        update_mail_log(log_path)

    print('Termina enviar_articulos_duplicados')
    print('')











