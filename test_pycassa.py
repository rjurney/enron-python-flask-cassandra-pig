import pycassa

pool = pycassa.ConnectionPool('enron')
cf = pycassa.ColumnFamily(pool, 'email_topics')

cf.get('<431.1075859137859.JavaMail.evans@thyme>') # Replace me
# OrderedDict([(u'bankruptcy', u'0.02577520626485872'), (u'end', u'0.018016096034710077'), (u'left', u'0.024021461379613435'), (u'palmer', u'0.017183470843239148'), (u'party', u'0.05155041252971744'), (u'phillip', u'0.018016096034710077'), (u'pl', u'0.018016096034710077'), (u'plove', u'0.02577520626485872'), (u'tonight', u'0.018016096034710077'), (u'your', u'0.017969743348148298')])