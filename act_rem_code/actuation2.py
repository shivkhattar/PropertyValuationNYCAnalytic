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

#Take the latitude and longitude as input
latitudeInput = str(input("Enter latitude: "))
longitudeInput = str(input("Enter longitude: "))

#Create a new POST statement request with spark code in data
statements_url = session_url + '/statements'

data = {'code': textwrap.dedent("""
    def calculateDistance(location1: (String, String), location2: (Double, Double)): Double = {{
      val location1InRadians = (location1._1.toDouble.toRadians, location1._2.toDouble.toRadians);
      val location2InRadians = (location2._1.toRadians, location2._2.toRadians);
      val difference = (location2InRadians._1 - location1InRadians._1, location2InRadians._2 - location1InRadians._2);
      6371 * 2 * math.asin(math.sqrt(math.pow(math.sin(difference._1 / 2), 2) + math.cos(location1InRadians._1) * math.cos(location2InRadians._1) * math.pow(math.sin(difference._2 / 2), 2)));
    }}
    def inRange(distance: Double) = distance <= 1.5;
    val input = sc.textFile("/user/sk8325/project/output/processed/boroughBlock").map(_.split(","));
    val filtered = input.filter(x => inRange(calculateDistance((x(5),x(6)), ({0}, {1}))));
    val values = filtered.collect();
    if(values.isEmpty) println("Response: {{ status : 400: BAD REQUEST }}"); else {{
      val x = values.minBy(x => calculateDistance((x(5), x(6)), ({0}, {1})));
      val result = "{{ boroughBlock: " + x(0) + ", averagePropertyPrice : " + x(4) + ", crimeScore: " + x(1) + ",subwayScore: " + x(2) + ", educationScore: " + x(3) + "}}";
      println("Response: {{ status : 200 OK, out : " + result + " }}");
    }}
""".format(latitudeInput, longitudeInput)
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
