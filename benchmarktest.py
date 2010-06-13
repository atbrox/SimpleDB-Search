from nltk.corpus import inaugural, treebank

from sdbsearch import *
from sdbindex import *
import cProfile
import time

def f():
    t0 = time.time()
    term = "dallakdkdkd"
    t1 = time.time()
    putvector = "x"*1000
    t2 = time.time()
    sdb = SimpleDBIndex()
    t3 = time.time()
    sdb.addInvertedFileEntry(term, putvector)
    t4 = time.time()
    getvector = sdb.getInvertedFileEntry(term)
    t5 = time.time()
    print  "l gv = ", len(getvector)
    print "t5-t4 - get milliseconds = ", (t5-t4)*1000.0
    print "t4-t3 - put milliseconds = ", (t4-t3)*1000.0
    print "t3-t2 - constructor", (t3-t2)*1000.0
    print "t2-t1 - creating vector", (t2-t1)*1000.0
    print "t1-t0" , (t1-t0)*1000.0


def g():
    #t = inaugural.raw("1789-Washington.txt")
    #print(len(t))
    sdbsearch = SimpleDBSearch()
    for url in nltk.corpus.inaugural.fileids()[:10]:
        data = inaugural.raw(url)
        print "indexing url ", url, len(data), type(data)
        sdbsearch.index(url, data)

    print "-- writing index to sdb"
    sdbsearch.writeIndexToSDB()


def q(query):
    print "-------------------------"
    print "-------------------------"
    sdbsearch = SimpleDBSearch()
    t0 = time.time()
    sdbsearch.query(query)
    t1 = time.time()
    print "query '%s' took %0.3f s" % (query, (t1-t0) )

def benchmark():
    data = inaugural.raw(nltk.corpus.inaugural.fileids())
    firsterms = " ".join(data.split()[:1000]).split()

    for term in firsterms:
        q(term.lower())

#g()
benchmark()



