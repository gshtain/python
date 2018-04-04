from pyhive import presto
from pyhive.exc import DatabaseError
from time import sleep

PRESTO_HOST = 'presto.wixpress.com'
PRESTO_PORT = 8181
USER = 'georges@wix.com'

c = presto.connect(host=PRESTO_HOST, port=PRESTO_PORT, username= USER).cursor()

fd = open('query.sql', 'r')  # reading text of query
RAW_QUERY = fd.read()
fd.close()
fd = open('wt_delta.sql', 'r')  # reading text of query
WT_DELTA = fd.read()
fd.close()

c = presto.connect(host=PRESTO_HOST, port=PRESTO_PORT, username=USER).cursor()
for i in range(-10, 0):
    raw = RAW_QUERY.format(i)
    delta = WT_DELTA
    c = presto.connect(host=PRESTO_HOST, port=PRESTO_PORT, username=USER).cursor()
    c.execute(itr)
    c.close()



c = presto.connect(host=PRESTO_HOST , port=PREST_PORT, username=USER).cursor()
for i in range(0,10):
    qr = QUERY.format(i)
    c.execute(qr)