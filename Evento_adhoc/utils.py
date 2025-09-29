import json
import snowflake.connector
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import os
import sys
import openpyxl
import shutil
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib
import streamlit as st

#Función para loguearse
def snowflake_login():
    counter = 0
    while True:
        if counter + 1 < 4:
            print(f"Intento {counter + 1}")

            try:
                # user = st.text_input("INGRESAR USUARIO: ")
                # psw = st.text_input("INGRESAR CONTRASEÑA: ")
                # pass_ = st.text_input("INGRESAR PASSCODE: ")
                user = input("INGRESAR USUARIO: ")
                psw = input("INGRESAR CONTRASEÑA: ")
                pass_ = input("INGRESAR PASSCODE: ")

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


def delete_old_files(directory, days_old=15):
    try:
        # Get the current time
        now = time.time()

        # Calculate the cutoff time
        cutoff_time = now - (days_old * 86400)  # 86400 seconds in a day

        # Iterate over files in the directory
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            # Check if it is a file
            if os.path.isfile(file_path):
                # Get the last modification time
                file_mtime = os.path.getmtime(file_path)
                # Check if the file is older than the cutoff time
                if file_mtime < cutoff_time:
                    os.remove(file_path)
    except Exception as e:
        print(f"Error deleting files: {e}")


def copy_excel_file(src_file):

    print('')
    print('Inicia copy_excel_file')
    try:
        # Get the directory of the source file
        src_directory = os.path.dirname(src_file)

        # Define the new file name with the current date
        respaldo_name = f"Base ofertas para BI - Respaldo {datetime.today().strftime('%y-%m-%d')}.xlsx"

        respaldo_directory = os.path.join(src_directory, 'Respaldo')
        if not os.path.exists(respaldo_directory):
            os.makedirs(respaldo_directory)

        dest_file = os.path.join(respaldo_directory, respaldo_name)

        # Copy the file
        try:
            shutil.copy(src_file, dest_file)
            print('Excel BI - Respaldo generado')

        except Exception as e:
            print(f'No se pudo generar una copia. Error: {e}')
            sys.exit()

        delete_old_files(respaldo_directory)

    except Exception as e:
        print(f"Error copying file: {e}")

    print('Termina copy_excel_file')

def try_open_excel(file_path):
    try:
        # Try to open the file in read-only mode
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        print("Excel file opened successfully.")
        workbook.close()  # Close the workbook after opening it
        return True
    except PermissionError:
        # Handle the error if the file is already opened by another user
        print("El Excel Base BI se encuentra abierto por un usuario")
        print("Se interrumple el flujo")
        return False

def lock_excel_file(file_path):
    print('')
    if try_open_excel(file_path):
        try:
            # Attempt to make the file read-only
            os.chmod(file_path, 0o444)  # Set file to read-only for all users
            print("El Excel Base BI esta bloqueado")
        except Exception as e:
            print(f"Error locking the file: {e}")
    else:
        sys.exit()

def unlock_excel_file(file_path):
    try:
        # Revert file back to writable mode
        os.chmod(file_path, 0o666)  # Set file back to writable
        print("El Excel Base BI esta desbloqueado")
    except Exception as e:
        print(f"Error unlocking the file: {e}")