/* AvroStorage */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

import 'tfidf.macro';

emails = load '/me/Data/enron.avro' using AvroStorage();
emails = filter emails by message_id is not null;
/* Limit to 100 documents for local mode, or go bake a cake in the meanwhile */
emails = limit emails 100;
id_body = foreach emails generate message_id, body;

rmf /tmp/macro_test
my_tf_idf_scores = tf_idf(id_body, 'message_id', 'body');
store my_tf_idf_scores into '/tmp/macro_test';
