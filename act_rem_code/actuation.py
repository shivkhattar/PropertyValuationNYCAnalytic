import json, pprint, requests, textwrap
host = 'http://login-2-1.local:9238'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}

#Create a new session
r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
pprint.pprint(r.json())

#Check if session is created, and is in idle state
session_url = host + r.headers['location']
r = requests.get(session_url, headers=headers)
pprint.pprint(r.json())

#Take the zipcode as input
userInput = str(input("Enter zipcode: "))

#Create a new POST statement request with spark code in data
statements_url = session_url + '/statements'

data = {'code': textwrap.dedent("""
    val input = sc.textFile("/user/sk8325/project/output/processed/zipcode");
    val data = input.map(_.split(","));
    val filtered = data.filter(x => x(0).equalsIgnoreCase("{0}"))
    val mapped = filtered.map(x =>"{{ zipcode: " + x(0) + ", averagePropertyPrice : " + x(4) + ", crimeScore: " + x(1) + ",subwayScore: " + x(2) + ", educationScore: " + x(3) + "}}");
    val result = mapped.collect();
    if(result.length != 1) println("Response: {{ status : 400: BAD REQUEST }}"); else println("Response: {{ status : 200 OK, out : " + result(0) + " }}");
""".format(userInput)
)}
r = requests.post(statements_url, data=json.dumps(data), headers=headers)
pprint.pprint(r.json())

#Poll until response is ready
statement_url = host + r.headers['location']
r = requests.get(statement_url, headers=headers)
pprint.pprint(r.json())

#Close the session
session_url = host + '/sessions/0'
requests.delete(session_url, headers=headers)