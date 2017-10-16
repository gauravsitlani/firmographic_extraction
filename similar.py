import csv
import clearbit
from collections import defaultdict
from pymongo import MongoClient
import json

client = MongoClient('localhost',27017)
db = client.test

clearbit.key='sk_e34ad80bdb6baf1ed89a7c7e9df02b4f'

columns = defaultdict(list) # each value in each column is appended to a list

with open('similar_domains_from_2300Companies.csv - Updated Uniques.csv') as f:
    reader = csv.DictReader(f) # read rows into a dictionary format
    for row in reader: # read a row as {column1: value1, column2: value2,...}
        for (k,v) in row.items(): # go over each column name and value
            columns[k].append(v) # append the value into the appropriate list
                                 # based on column name k

#print(columns['firm_data.similarDomains'])



dom= columns['Domains']
dom_dat = [i.split('\t', 1)[0] for i in dom] #domain list

cid = columns['cid']
cid_dat = [i.split('\t', 1)[0] for i in cid] #cids

for i in range(0,40):
    company_id = cid_dat[i]
    company_d = dom_dat[i]
    with open('new_companies.txt','a') as f:
        f.write(str(company_id) + " " + str(company_d) + "\n")
    try:
        company_dat = clearbit.Company.find(domain=company_d, stream=True)
        json_dat = {"cid": company_id, "domain": company_d, "firm_data": company_dat}
        db.company_data.insert(json_dat)
        print("downloaded for " + str(company_id) + " " + str(i))
    except:
        with open("notdownloaded.txt", "a") as f:
            print("unable to download for " + str(i) + " " + str(company_id))
            f.write("unable to download " + str(i) + " " + str(company_id) + " " + str(company_d) + "\n")






