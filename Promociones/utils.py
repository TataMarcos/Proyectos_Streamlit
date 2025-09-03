import os
import snowflake.connector
import json
import pandas as pd
import streamlit as st
from snowflake.connector.pandas_tools import write_pandas
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from config import *

def get_credentials(type: str) -> dict:

    if type == 'credentials_mail_servicio':
        with open(jsons['credentials_mail_servicio']) as f:
            credentials = json.load(f)#[type]

    else:
        with open(jsons['credentials']) as f:
            credentials = json.load(f)[type]

    return credentials

#Función para loguearse
def snowflake_login():
    counter = 0
    while True:
        if counter + 1 < 4:
            print(f"Intento {counter + 1}")

            try:
                user = st.text_input("INGRESAR USUARIO: ")
                psw = st.text_input("INGRESAR CONTRASEÑA: ")
                pass_ = st.text_input("INGRESAR PASSCODE: ")
                # user = input("INGRESAR USUARIO: ")
                # psw = input("INGRESAR CONTRASEÑA: ")
                # pass_ = input("INGRESAR PASSCODE: ")

                # Establish Snowflake connection
                snowflake_connection = snowflake.connector.connect(
                    user=user,
                    password=psw,
                    account="XZ23267-dp32414",
                    passcode=pass_,
                    database='SANDBOX_PLUS',
                    schema='DWH'
                )
                cursor = snowflake_connection.cursor()

                print('Correct Password - connected to SNOWFLAKE')
                break

            except FileNotFoundError:
                print("Error: 'credentials.json' file not found.")
                break
            except json.JSONDecodeError:
                print("Error: 'credentials.json' file is not valid JSON.")
                break
            except Exception as e:
                counter += 1
                print(f'Error: {e}')
                print('Incorrect Password - provide again')
        else:
            print('3 Intentos fallidos')
            break

    return user, cursor, snowflake_connection

def descargar_segmento(cursor: snowflake.connector.cursor.SnowflakeCursor,
                       query: str, cond = None) -> pd.DataFrame:

    # Obtengo json con directorio de queries y su orden
    query_path = 'Queries/' + query + '.sql'

    # Obtengo el texto de mi query
    with open(query_path, 'r', encoding="utf8") as file: command = file.read()

    # Ejecuto query para obtener clientes que superaron la promo
    if not(cond):
        cursor.execute(command)
        #command = command
    else:
        cursor.execute(command.format(cond=cond))
        #command = command.replace(';', cond)

    # Obtenemos el resultado de la consulta del cursor en una dataframe de pandas
    df = cursor.fetch_pandas_all()

    return df

def carga_snow_generic(df, ctx, table, database, schema):

    # Ingresamos la base de datos en Snow
    success = write_pandas(df = df, conn = ctx, table_name= table,
                           database= database, schema = schema)

    if success:
        print('SUCCESS')
        return success

def clean_table(cursor, table, cond=None):

    # SQL para obtener datos
    if cond is None:
        sql = f"DELETE FROM SANDBOX_PLUS.DWH.{table};"
    else:
        sql = f"DELETE FROM SANDBOX_PLUS.DWH.{table} WHERE {cond};"

    # Ejecuto select de Snow
    cursor.execute(sql)

    return True

def enviar_email(sender, receiver, subject, body, files:list):
    smtp_server = "fast.smtpok.com"  # Reemplaza con el servidor SMTP que uses
    smtp_port = 587  # Usualmente 587 para TLS o 465 para SSL
    sender_email = sender                       #Reemplaza con tu email
    credentials = get_credentials('correo_autom')
    sender_user = credentials['USER']
    sender_password = credentials['PASS']       #Reemplaza con tu contraseña
    recipient_email = receiver                  #Email del destinatario
 

    # Crear el mensaje
    mensaje = MIMEMultipart()
    mensaje["From"] = sender_email
    mensaje["To"] = ", ".join(recipient_email)
    mensaje["Subject"] = subject #"Error Maestro productos Hist"
      
    mensaje.attach(MIMEText(body, "plain"))

    # Attach the Excel file
    if len(files) > 0:
        for file_path in files:
            attachment_name = os.path.basename(file_path)
            with open(file_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={attachment_name}')
                mensaje.attach(part)
    try:
        # Conectar al servidor SMTP
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # Iniciar cifrado TLS
        server.login(sender_user, sender_password)  # Autenticación
        
        # Enviar el correo
        server.sendmail(sender_email, recipient_email, mensaje.as_string())
        print("Correo enviado exitosamente.")
        
        # Cerrar la conexión
        server.quit()
        return True
    except Exception as e:
        print(f"Error al enviar el correo: {e}")
        return False