from sf_login import sf_login
from sf_query import touched_accounts
from sheets_login import sheets_login
from dateutil import parser
import json
from datetime import datetime
import time
from gspread.exceptions import APIError
from db_connect import connect_to_db, insert_data_to_db, get_agent_last_row


def first_empty_row(sheet):
    str_list = list(filter(None, sheet.col_values(1)))  # Get all values in column A
    return len(str_list) + 1


def exponential_backoff(attempt):
    if attempt == 0:
        return 1
    elif attempt == 1:
        return 2
    else:
        return (attempt ** 2)

def update_sheet_with_retry(sheet, values, start_row, end_row, max_attempts=5):
    attempt = 0
    while attempt < max_attempts:
        try:
            sheet.batch_update([{
                'range': f'A{start_row}:V{end_row}',
                'values': [[cell[2] for cell in values[i:i+22]] for i in range(0, len(values), 22)]
            }])
            return
        except APIError as e:
            if e.response.status_code == 429:  # Too Many Requests
                attempt += 1
                if attempt == max_attempts:
                    raise
                sleep_time = exponential_backoff(attempt)
                time.sleep(sleep_time)
            else:
                raise


if __name__ == "__main__":
    start_time = time.time()
    sf = sf_login()

    # Load the agent_ids
    with open('agent_ids.json', 'r') as file:
        agents_data = json.load(file)

    #get connected to everything
    sheet = sheets_login()
    conn = connect_to_db()

    login_time = time.time()


    agents_dict = {}
    for agent in agents_data:
        info = {
            "id": agent["id"],
            "email": agent["email"],
            "accountmanagers": agent["accountManagers"]
        }
        agents_dict[agent['name']] = info



    # Sales Season Start
    cutoff_date = parser.parse("2024-6-1")
    #today = str(datetime.today())
    today = datetime.today().strftime("%Y-%m-%d")
    agent_contact_counts = touched_accounts(sf, cutoff_date, agents_dict)

    get_contacts_time = time.time()
    
    row = first_empty_row(sheet)
    start_row = row
    headers = sheet.row_values(1)

    # Prepare batch update
    batch_update = []
    #going to db
    db_data = []
    agent_rows = []

    for agent, info in agent_contact_counts.items():
        if conn:
            agent_rows = get_agent_last_row(conn, agent)

        # Use a default value (e.g., 0) if agent_rows is empty
        default_value = 0
        last_row = agent_rows[0] if agent_rows else [default_value] * 12
        deltas = {
            "team_delta": info["total_count"] - last_row[3],
            "team_cust_delta": info["customer_count"] - last_row[4],
            "team_non_delta": info["non_customer_count"] - last_row[5],
            "agent_total_delta": info["agent_total_count"] - last_row[6],
            "agent_cust_delta": info["agent_count_cust"] - last_row[7],
            "agent_non_delta": info["agent_count_non"] - last_row[8],
            "am_total_delta": info["ams_total_count"] - last_row[9],
            "am_cust_delta": info["am_cust_count"] - last_row[10],
            "am_non_delta": info["am_non_count"] - last_row[11]
        }
        row_data = [
            (row, headers.index("Date") + 1, today),
            (row, headers.index("Agent") + 1, agent),

            (row, headers.index("Team Total") + 1, info["total_count"]),
            (row, headers.index("Team Total Delta") + 1, deltas["team_delta"]),

            (row, headers.index("Team Customer Count") + 1, info["customer_count"]),
            (row, headers.index("Team Cust Delta") + 1, deltas["team_cust_delta"]),

            (row, headers.index("Team Non Customer Count") + 1, info["non_customer_count"]),
            (row, headers.index("Team Non Delta") + 1, deltas["team_non_delta"]),

            (row, headers.index("Agent Total Count") + 1, info["agent_total_count"]),
            (row, headers.index("Agent Total Delta") + 1, deltas["agent_total_delta"]),

            (row, headers.index("Agent Cust Count") + 1, info["agent_count_cust"]),
            (row, headers.index("Agent Cust Delta") + 1, deltas["agent_cust_delta"]),

            (row, headers.index("Agent Non Count") + 1, info["agent_count_non"]),
            (row, headers.index("Agent Non Delta") + 1, deltas["agent_non_delta"]),

            (row, headers.index("AMs Total Count") + 1, info["ams_total_count"]),
            (row, headers.index("AMs Total Delta") + 1, deltas["am_total_delta"]),

            (row, headers.index("AMs Cust Count") + 1, info["am_cust_count"]),
            (row, headers.index("AMs Cust Delta") + 1, deltas["am_cust_delta"]),

            (row, headers.index("AMs Non Count") + 1, info["am_non_count"]),
            (row, headers.index("AMs Non Delta") + 1, deltas["am_non_delta"]),


            (row, headers.index("Customer Links") + 1, ", ".join(info["customer_links"])),
            (row, headers.index("Non-Customer Links") + 1, ", ".join(info["non_cust_links"]))
        ]


        batch_update.extend(row_data)
        
        db_row = (
            today, agent, info["total_count"], info["customer_count"], info["non_customer_count"],
            info["agent_total_count"], info["agent_count_cust"], info["agent_count_non"],
            info["ams_total_count"], info["am_cust_count"], info["am_non_count"], 
            deltas["team_delta"], deltas["team_cust_delta"], deltas["team_non_delta"], 
            deltas["agent_total_delta"], deltas["agent_cust_delta"], deltas["agent_non_delta"], 
            deltas["am_total_delta"], deltas["am_cust_delta"], deltas["am_non_delta"]
        )
        db_data.append(db_row)
        
        row += 1

    # Perform batch update with retry
    update_sheet_with_retry(sheet, batch_update, start_row, row)

    # Insert data into PostgreSQL database
    
    if conn:
        insert_data_to_db(conn, db_data)
        conn.close()

    update_sheet_time = time.time()

    print("Sheet Updated with all contacts made by Agent or AM with Links.")
    print(f"Total execution Time: {update_sheet_time - start_time:.4f} seconds.")
    print(f"Login Time: {login_time - start_time:.4f} seconds.")
    print(f"Get Contacts Time: {get_contacts_time - login_time:.4f} seconds.")
    print(f"Update Sheet and DB Time: {update_sheet_time - get_contacts_time:.4f} seconds.")