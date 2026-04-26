import pymysql
conn = pymysql.connect(host='zidepeople-dev.mysql.database.azure.com', user='zide_admin', password='Suggestpassword56', database='zidepeople_db', ssl={'ssl': {}})
cur = conn.cursor()
cur.execute("SELECT id, email, username, role, verified, active, verification_complete, documents_verified, account_status FROM accounts WHERE id = 1225")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
print(cols)
for r in rows:
    print(r)
conn.close()
