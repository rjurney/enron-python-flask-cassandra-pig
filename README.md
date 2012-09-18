Enron-Python-Flask-Cassandra-Pig
================================

This Hortonworks example post extracts topics via TF-IDF from the Enron emails and serves them via Cassandra and Flask with help from the Pygmalion project, CassandraStorage and pycassa. It accompanies the blog post at <>.

### Environment Setup

Edit and run env.sh to inform CassandraStorage about your local Cassandra instance.

### Cassandra Setup

Install Cassandra according to the instructions in the post, and then create our schema by running cassandra.txt in the cassandra-cli.

### Test Pycassa

Run test_pycassa.py to verify it works.

### Get the Enron Emails

Grab the Enron emails at https://s3.amazonaws.com/rjurney_public_web/hadoop/enron.avro

### Run our Pig Script

Run cassandra_enron.pig to extract topics from the email bodies and store them in Cassandra. Note: you may want to adjust the limit statement to run the example on fewer emails if you are running this example in local mode. The entire corpus on one machine will take a LONG time to finish. This is where the utility of Hadoop comes in :)

### Serve up our data in our app

Run index.py and plug in a message_id (which you can get via SAMPLE/LIMIT in Pig) to the url in your favorite browser and you can see the top 20 topics, as determined by Tf*idf, published in a web service. Wallah!