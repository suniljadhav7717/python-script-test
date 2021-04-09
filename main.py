
import os
import psycopg2
import csv

#Establishing a connection with a Heroku postgres database

os.environ['DATABASE_URL'] = 'postgres://uc2p2236val9kp:paf8df72800d4c1fb3da645deb692e613ca6aacc50dd166ce9f7a415fc5fd4629@ec2-18-198-198-182.eu-central-1.compute.amazonaws.com:5432/d7dp84irs13fqf'
DATABASE_URL = os.environ['DATABASE_URL']

conn = psycopg2.connect(DATABASE_URL, sslmode='require')
print("conn=", conn)


cur = conn.cursor()
print(" Cursor", cur)

#Get all tables
cur.execute("select table_name from information_schema.tables where table_schema = 'salesforce' order by table_name")
Htables = cur.fetchall()
objectList = [''.join(i) for i in Htables]
for char in objectList[:]:
    if char.startswith('_'):
        objectList.remove(char)
print(objectList)
print(len(objectList))




for obj in objectList:
    # Get columns here
    s2 = "select column_name from information_schema.columns where table_schema = 'salesforce' and table_name = "+"'"+obj+"'"+" order by column_name"
    #print(s2)
    cur.execute(s2)
    Hfields = cur.fetchall()
    fields = [''.join(i) for i in Hfields]
    #print(Hfields)

    s3 = ",".join(fields)
    s1 = "select "+s3+" from salesforce."+obj+" LIMIT 10"

    cur.execute(s1)
    rows = cur.fetchall()

    #for i in rows:
     #   print(i[0], "\t", i[1], "\n")

    filename = obj+".csv"

    # writing to csv file
    with open(filename, 'w') as csvfile:
    # creating a csv writer object
        csvwriter = csv.writer(csvfile)

    # writing the fields
        csvwriter.writerow(fields)
    # writing the data rows
        csvwriter.writerows(rows)

