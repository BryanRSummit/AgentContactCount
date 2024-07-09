from dateutil import parser
from datetime import datetime
from dataclasses import dataclass
import json

@dataclass
class Agent:
    id: str
    username: str
    fname: str
    lname: str
    email: str

@dataclass
class Activity:
    id: str
    ownerId: str
    subject: str
    description: str
    activity_date: datetime

@dataclass
class Account:
    id: str
    name: str
    agentId: str
    agentName: str
    converted: bool
    account_type: str
    contacted: bool



def eligible_contact(act):
    if act.subject == "Call" or act.subject == "Prospecting":
        return True
    return False

def get_opp_activity(sf, opId, cutOff):
    contacted = False
    # Create a datetime object for October 1st, 2023
    #cutoff = datetime(2023, 9, 1)

    oppActivityQuery = f"""
                SELECT
                    Id, 
                    Subject,
                    Description, 
                    WhatId, 
                    ActivityDate
                FROM Task
                WHERE WhatId = '{opId}'
    """
    opp_activity_records = sf.query_all(oppActivityQuery)['records']

    for op in opp_activity_records:
        act_date = parser.parse(op["ActivityDate"])
        if act_date > cutOff:
            contacted = True
            break
        
    return contacted



    
# Because people can put calls on accounts OR on Opportunities we have to query all 
# kinds of stuff here, hopefully it doesn't slow things down too much
def had_activity(sf, account, cutOff):
    contacted = False
    # Create a datetime object for October 1st, 2023
    #cutoff = datetime(2023, 9, 1)

    # get any activity on the account
    accountActivityQuery = f"""
                SELECT 
                    What.Name,
                    What.Id,
                    Id,
                    OwnerId, 
                    Subject,
                    Description,
                    ActivityDate
                FROM Task 
                WHERE WhatId IN ('{account.id}')
          """
    
    #get 2024 opportunity for this account to check for activity
    oppQuery = f"""
                SELECT
                    Id, 
                    Name, 
                    StageName, 
                    CloseDate, 
                    Amount
                FROM Opportunity
                WHERE AccountId = '{account.id}' AND Name LIKE '%2024%' 
          """
    

    account_activity_records = sf.query_all(accountActivityQuery)['records']
    opp_records = sf.query_all(oppQuery)['records']


    # check activity on accounts 
    for rec in account_activity_records:
        act_date = parser.parse(rec["ActivityDate"])
        if act_date > cutOff:
            contacted = True
            account.contacted = True

    #don't do this part if we already found activity on account
    if contacted == False:
        #check if opportunity has recent activity
        for op in opp_records:
            contacted = get_opp_activity(sf, op["Id"], cutOff)
            if contacted == True:
                break

        
    return contacted

def agent_shares_am(agent, am_data):
    for am in am_data:
        for _agent in am["agents"]:
            if _agent["name"] == agent:
                if len(am["agents"]) > 1:
                    return True
                return False

def touched_accounts(sf, cutOff, agents_dict):
    # Used to see Object fields
    account_metadata = sf.Account.describe()
    # Extract field names
    field_names = [{"name": field['name'], "label": field["label"]} for field in account_metadata['fields']]

    # Load AM Info to check for multiple agents
    with open('am_ids.json', 'r') as file:
        am_data = json.load(file)

    formatted_date = cutOff.strftime("%Y-%m-%dT%H:%M:%SZ")


    agent_counts = {}
    for agent, info in agents_dict.items():
        agentId = info["id"]
        amIds = ",".join(f"'{x['id']}'" for x in info["accountmanagers"])

        ids = [agentId]
        ids.extend(x["id"] for x in info["accountmanagers"])
        formatted_ids = ','.join(f"'{id}'" for id in ids)

        shares_am = agent_shares_am(agent, am_data)
        # --------------------------------------START NON_CUSTOMERS-----------------------------
        if shares_am:
            # Query for agent's tasks
            agent_tasks_non = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE OwnerId = '{agentId}'
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type != 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)
            # Get the account IDs where the agent has tasks
            agent_account_ids_non = list({task['WhatId'] for task in agent_tasks_non['records']})

            agent_owns_non = sf.query(f"""
                SELECT Id, OwnerId, Assigned_Admin__c
                FROM Account
                WHERE OwnerId = '{agentId}'
                AND Type != 'Customer'
            """)

            # Query for account managers' tasks
            am_tasks_non = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE (OwnerId IN ({amIds}) OR OwnerId = '')
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type != 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)


            agent_task_count_non = agent_tasks_non["totalSize"]

            # If agent is assigned to the account then we will count the AM's activity on the account, 
            # Otherwise only agent activity will be counted. That's the only relationship we can use 
            # to link AMs with multiple agents to a specific agent and give credit for the activity. 
            am_task_count_non = 0
            for task in am_tasks_non['records']:
                for rec in agent_owns_non["records"]:
                    if task['WhatId'] == rec["Id"] and rec["OwnerId"] == agentId:
                        am_task_count_non += 1


            total_non_count = agent_task_count_non + am_task_count_non
        else:
            # Query for agent's tasks
            agent_tasks_non = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE OwnerId IN ({formatted_ids})
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type != 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)
            # Get the account IDs where the agent has tasks
            agent_account_ids_non = list({task['WhatId'] for task in agent_tasks_non['records']})
            total_non_count = agent_tasks_non["totalSize"]
        # --------------------------------------END NON_CUSTOMERS-------------------------------------------------

        #---------------------------------------------------START CUSTOMERS----------------------------------------------------------------
        if shares_am:
            # Query for agent's tasks
            agent_tasks_cust = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE OwnerId = '{agentId}'
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type = 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)
                        # Get the account IDs where the agent has tasks
            agent_account_ids_cust = list({task['WhatId'] for task in agent_tasks_cust['records']})

            agent_owns_cust = sf.query(f"""
                SELECT Id, OwnerId, Assigned_Admin__c
                FROM Account
                WHERE OwnerId = '{agentId}'
                AND Type = 'Customer'
            """)

            # Query for account managers' tasks
            am_tasks_cust = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE (OwnerId IN ({amIds}) OR OwnerId = '')
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type = 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)


            agent_task_count_cust = agent_tasks_cust["totalSize"]

            am_task_count_cust = 0

            # If agent is assigned to the account then we will count the AM's activity on the account, 
            # Otherwise only agent activity will be counted. That's the only relationship we can use 
            # to link AMs with multiple agents to a specific agent and give credit for the activity. 
            am_task_count_cust = 0
            for task in am_tasks_cust['records']:
                for rec in agent_owns_cust["records"]:
                    if task['WhatId'] == rec["Id"] and rec["OwnerId"] == agentId:
                        am_task_count_cust += 1



            total_cust_count = agent_task_count_cust + am_task_count_cust
        else:
            # Query for agent's tasks
            agent_tasks_cust = sf.query(f"""
                SELECT Id, Description, WhatId
                FROM Task
                WHERE OwnerId IN ({formatted_ids})
                AND CreatedDate >= {formatted_date}
                AND Status = 'Completed'
                AND Account.Type = 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """)
            # Get the account IDs where the agent has tasks
            agent_account_ids_cust = list({task['WhatId'] for task in agent_tasks_cust['records']})
            total_cust_count = agent_tasks_cust["totalSize"]

        #---------------------------------------------------END CUSTOMERS----------------------------------------------------------------

        all_links = agent_account_ids_non + agent_account_ids_cust

        agent_counts[agent] = {
            "total_count": total_cust_count + total_non_count,
            "customer_count": total_cust_count,
            "non_customer_count": total_non_count,
            "ams_count": (total_cust_count + total_non_count) - (agent_task_count_non + agent_task_count_cust),
            "agent_count_non": agent_task_count_non,
            "agent_count_cust": agent_task_count_cust,
            "links": [f"https://reddsummit.lightning.force.com/lightning/r/Account/{x}/view" for x in all_links]
        }


    return agent_counts


#-------------------Gives list of accounts or Opportunities where the contact was made
# SELECT 
#     What.Id,
#     What.Name,
#     What.Type
# FROM Task  
# WHERE OwnerId = '0054U00000DtldhQAB'
# AND CreatedDate >= 2024-06-01T00:00:00Z
# AND (What.Type = 'Account' OR What.Type = 'Opportunity')