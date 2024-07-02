from sf_login import sf_login
from sf_query import touched_accounts
from sheets_login import sheets_login
from dateutil import parser
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import base64
from googleapiclient.errors import HttpError
import pandas as pd
from datetime import datetime
import time

def first_empty_row(sheet):
    str_list = list(filter(None, sheet.col_values(1)))  # Get all values in column A
    return len(str_list) + 1



if __name__ == "__main__":
    start_time = time.time()

    sf = sf_login()

    # Load the agent_ids
    with open('agent_ids.json', 'r') as file:
        agents_data = json.load(file)

    sheet = sheets_login()
    # Get all the data from the sheet and group it up by agent
    # data = sheet.get_all_values()
    # sheet.update_cell(row_idx, 9, contact_count)
    # sheet.update_cell(row_idx, 10, str(last_attempt))
    # sheet.update_cell(row_idx, 11, sf_converted)

    login_time = time.time()


    agents_dict = {}
    for agent in agents_data:
        info = {
            "id": agent["id"],
            "email": agent["email"],
            "accountmanagers": agent["accountManagers"]
        }
        agents_dict[agent['name']] = info
        # headers = sheet.row_values(1) 
        # col = headers.index(agent["Name"]) + 1
        # sheet.update_cell(2, col, agent["id"])


    # Sales Season Start
    cutoff_date = parser.parse("6-1-24")
    today = str(datetime.today())
    agent_contact_counts = touched_accounts(sf, cutoff_date, agents_dict)

    get_contacts_time = time.time()

    row = first_empty_row(sheet)
    headers = sheet.row_values(1)
 
    for agent, info in agent_contact_counts.items():
        col = headers.index("Date") + 1
        sheet.update_cell(row, col, today)

        col = headers.index("Agent") + 1
        sheet.update_cell(row, col, agent)

        col = headers.index("Total Count") + 1
        sheet.update_cell(row, col, info["total_count"])

        col = headers.index("Customer Count") + 1
        sheet.update_cell(row, col, info["customer_count"])

        col = headers.index("Non Customer Count") + 1
        sheet.update_cell(row, col, info["non_customer_count"])

        col = headers.index("Links") + 1
        links_str = ", ".join(info["links"])
        sheet.update_cell(row, col, links_str)

        row = row + 1

    update_sheet_time = time.time()

    print("Sheet Updated with all contacts made by Agent or AM with Links.")
    print(f"Total execution Time: {update_sheet_time - start_time:.4f} seconds.")
    print(f"Login Time: {login_time - start_time:.4f} seconds.")
    print(f"Get Contacts Time: {get_contacts_time - login_time:.4f} seconds.")
    print(f"Update Sheet Time: {update_sheet_time - get_contacts_time:.4f} seconds.")