import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def sheets_login():
    # Google Sheets credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive', "https://www.googleapis.com/auth/gmail.send"]
    gmailCreds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\Bryan Edman\Documents\Count Contact Tool\agentleadmailer-8cc577104ac3.json', scope)
    gc = gspread.authorize(gmailCreds)

    # Open the Google Sheet
    sheet = gc.open('Agent Contact Count').sheet1
    return sheet