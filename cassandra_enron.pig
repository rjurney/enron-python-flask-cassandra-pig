/* CassandraStorage and utilities */
register /me/Software/apache-cassandra-1.1.5-src/build/apache-cassandra*.jar
register /me/Software/apache-cassandra-1.1.5-src/lib/*.jar
register /me/Software/apache-cassandra-1.1.5-src/build/lib/jars/*.jar /* */
register /me/Software/pygmalion/udf/target/pygmalion-1.1.0-SNAPSHOT.jar

define CassandraStorage org.apache.cassandra.hadoop.pig.CassandraStorage();
define FromCassandraBag org.pygmalion.udf.FromCassandraBag();
define ToCassandraBag org.pygmalion.udf.ToCassandraBag();

/* AvroStorage */
register /me/Software/pig/build/ivy/lib/Pig/avro-1.5.3.jar
register /me/Software/pig/build/ivy/lib/Pig/json-simple-1.1.jar
register /me/Software/pig/contrib/piggybank/java/piggybank.jar

define AvroStorage org.apache.pig.piggybank.storage.avro.AvroStorage();

emails = load '/me/Data/enron.avro' using AvroStorage();
emails = filter emails by message_id is not null;
emails = limit emails 10;
id_body = foreach emails generate message_id, body;

define test_stream `token_extractor.py` SHIP ('token_extractor.py');
cleaned_words = stream id_body through test_stream as (message_id:chararray, token_strings:chararray);
token_records = foreach cleaned_words generate message_id, FLATTEN(TOKENIZE(token_strings)) as tokens;

all_words = foreach token_records generate tokens;
word_totals = foreach (group all_words by tokens) generate group as token, COUNT_STAR(all_words) as doc_total;

dump word_totals

/*raw =  LOAD 'cassandra://pygmalion/account' USING CassandraStorage();
rows = FOREACH raw GENERATE key, FLATTEN(FromCassandraBag('first_name, last_name, birth_place', columns)) AS (
    first_name:chararray,
    last_name:chararray,
    birth_place:chararray
);
dump rows
*/