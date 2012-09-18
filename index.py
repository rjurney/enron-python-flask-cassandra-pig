from flask import Flask
import pycassa
import json

pool = pycassa.ConnectionPool('enron')
cf = pycassa.ColumnFamily(pool, 'email_topics')

app = Flask(__name__)

@app.route("/message/topics/<message_id>")
def topics(message_id):
  return json.dumps(cf.get(message_id))

if __name__ == "__main__":
  app.run(debug=True)