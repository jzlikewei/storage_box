import http.client
import json

conn = http.client.HTTPConnection("0.0.0.0", 4243)

sql = '''
create table if not exists tasks (
  'id' integer not null  primary key autoincrement,
	'name' varchar(255) not null,
  'kind' varchar(255) not null ,
	'args' varchar(4096) not null,
	'task_status' bool default 0,
  'created_at' datetime default current_timestamp,
  'updated_at' datetime default current_timestamp,
  CONSTRAINT 'name_idx' UNIQUE ('name'),
)
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