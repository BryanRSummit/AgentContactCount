from dateutil import parser
from datetime import datetime, timedelta
from dataclasses import dataclass
import re
import time

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



def touched_accounts(sf, cutOff, agents_dict):
    # # Used to see Object fields
    # account_metadata = sf.Task.describe()
    # # Extract field names
    # field_names = [field['name'] for field in account_metadata['fields']]

    formatted_date = cutOff.strftime("%Y-%m-%dT%H:%M:%SZ")


    agent_counts = {}
    for agent, info in agents_dict.items():
        ids = [info["id"]]
        ids.extend(x["id"] for x in info["accountmanagers"])
        formatted_ids = ','.join(f"'{id}'" for id in ids)
        countNonCustQuery = f"""
                SELECT COUNT()
                FROM Task  
                WHERE OwnerId IN ({formatted_ids})
                AND CreatedDate >= {formatted_date}
                AND Account.Type != 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
                """
        
        countCustQuery = f"""
                SELECT COUNT()
                FROM Task  
                WHERE OwnerId IN ({formatted_ids})
                AND CreatedDate >= {formatted_date}
                AND Account.Type = 'Customer'
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
                """
        
        linkQuery = f"""
                SELECT 
                    What.Id,
                    What.Name,
                    What.Type
                FROM Task  
                WHERE OwnerId IN ({formatted_ids})
                AND CreatedDate >= {formatted_date}
                AND (What.Type = 'Account' OR What.Type = 'Opportunity')
            """

        link_records = sf.query(linkQuery)["records"]
        time.sleep(0.1)
        non_cust_record_count = sf.query(countNonCustQuery)['totalSize']
        time.sleep(0.1)
        cust_record_count = sf.query(countCustQuery)['totalSize']
        time.sleep(0.1)
        agent_counts[agent] = {
            "total_count": non_cust_record_count + cust_record_count,
            "customer_count": cust_record_count,
            "non_customer_count": non_cust_record_count,
            "links": [f"https://reddsummit.lightning.force.com/lightning/r/Account/{x['What']['Id']}/view" for x in link_records]
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