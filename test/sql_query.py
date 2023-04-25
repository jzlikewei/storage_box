import http.client
import json

conn = http.client.HTTPConnection("0.0.0.0", 4243)

sql = '''
 select * from tasks where task_status = 0; 
'''
payload = json.dumps({
  "auth_key": "auth",
  "sql": sql,
})
headers = {
  'Content-Type': 'application/json'
}
conn.request("POST", "/sql/query", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))