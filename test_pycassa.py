import pycassa

pool = pycassa.ConnectionPool('enron')
cf = pycassa.ColumnFamily(pool, 'email_topics')

