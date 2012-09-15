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
id_body = foreach emails generate message_id, body;

raw =  LOAD 'cassandra://pygmalion/account' USING CassandraStorage();
rows = FOREACH raw GENERATE key, FLATTEN(FromCassandraBag('first_name, last_name, birth_place', columns)) AS (
    first_name:chararray,
    last_name:chararray,
    birth_place:chararray
);
dump rows
