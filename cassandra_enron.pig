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
doc_word_totals = foreach (group token_records by (message_id, tokens)) generate 
                    flatten(group) as (message_id, token), 
                    COUNT_STAR(token_records) as doc_total;

pre_term_counts = foreach (group doc_word_totals by message_id) generate
                    group AS message_id,
                    FLATTEN(doc_word_totals.(token, doc_total)) as (token, doc_total), 
                    SUM(doc_word_totals.doc_total) as doc_size;

/* Calculate the TF */
term_freqs = foreach pre_term_counts generate message_id as message_id,
               token as token,
               ((double)doc_total / (double)doc_size) AS term_freq;

/* Get count of documents using each token, for idf */
token_usages = foreach (group term_freqs by token) generate
                 FLATTEN(term_freqs) as (message_id, token, term_freq),
                 COUNT_STAR(term_freqs) as num_docs_with_token;

/* Get document count */
just_ids = foreach emails generate message_id;
ndocs = foreach (group just_ids all) generate COUNT_STAR(just_ids) as total_docs;

/* Note the use of Pig Scalars to calculate idf */
tfidf_all = foreach token_usages {
             idf    = LOG((double)ndocs.total_docs/(double)num_docs_with_token);
             tf_idf = (double)term_freq*idf;
               generate message_id as message_id,
                  token as token,
                  tf_idf as tf_idf;
            };

per_message = foreach (group tfidf_all by message_id) {
  sorted = order tfidf_all by tf_idf desc;
  top_20_topics = limit sorted 20;
  generate group as message_id, top_20_topics.(token, tf_idf) as topic_score;
}

store per_message into '/tmp/test_per_message';

/*raw =  LOAD 'cassandra://pygmalion/account' USING CassandraStorage();
rows = FOREACH raw GENERATE key, FLATTEN(FromCassandraBag('first_name, last_name, birth_place', columns)) AS (
    first_name:chararray,
    last_name:chararray,
    birth_place:chararray
);
dump rows
*/