import http.client
import json

conn = http.client.HTTPConnection("0.0.0.0", 4243)

sql = '''
 insert into tasks ( name, args) values ( 'testjob', '[1,2,3,4,5]')
'''
payload = json.dumps({
  "auth_key": "auth",
  "sql": sql,
})
headers = {
  'Content-Type': 'application/json'
}
conn.request("POST", "/sql/exec", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))