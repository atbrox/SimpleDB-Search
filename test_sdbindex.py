import time
import unittest
import mockito
from mockito import *
import sys

from sdbindex import SimpleDBIndex

class TestSimpleDBIndex(unittest.TestCase):
    def setUp(self):
        self.sdbindex = SimpleDBIndex(domainprefix="atbroxtest")
    
    def testGetTermLine(self):
        i = 1+13*self.sdbindex.MAXITEMSIZE
        origterm = "gettermline"
        term, termLine = self.sdbindex._getTermLine(origterm,i)
        self.assertEquals(origterm + self.sdbindex.ITEMINDEXSEPARATOR + str(13), term)
        
    def testTermLineSize(self):
        termLine = {"termlinesize":"456"}
        self.assertEquals(15, self.sdbindex._termLineSize(termLine))
        
    def testFlushCache(self):
        when(self.sdbindex.sdb).batch_put_attributes(any(str),any()).thenReturn("")

        cacheentry = ("testflushcache", {"1":"abc", "2":"def"})
        self.sdbindex.batchcache.append(cacheentry)
        self.sdbindex._flushcache()
        self.assertEquals(None, verify(self.sdbindex.sdb, times=1).batch_put_attributes(any(str),any()))
        
    def testStore(self):
        when(self.sdbindex.sdb).batch_put_attributes(any(str),any()).thenReturn("")
        term = "teststoreterm"
        # trigger autoflushing
        termLine = {"0":"a"*(self.sdbindex.MAXITEMSIZE/10)} 
        # and a forced flush with flush=True
        self.sdbindex._store(term,termLine, flush = True)
        # check that it was flushed twice
        self.assertEquals(None, 
                          verify(self.sdbindex.sdb, times=1).batch_put_attributes(any(str),any()))
        
    def testAddInvertedFileEntry(self):
        self.sdbindex._warningthisdeletesallsimpledbdomains()
        self.sdbindex._createsimpledbdomains()
        term = "termtoput"
        vector = "thistermvectorshouldbeputandstored"
        self.sdbindex.addInvertedFileEntry(term, vector)
        self.assertTrue("termtoput" in self.sdbindex.domain.keys())
        self.assertEquals(1, len(self.sdbindex.domain.keys()))
        #print >> sys.stderr, self.sdbindex.domain.keys()
        
    def testGetInvertedFileEntry(self):
        self.sdbindex._warningthisdeletesallsimpledbdomains()
        self.sdbindex._createsimpledbdomains()
        term = "termtoget"
        putvector = "valuetoget"
        self.sdbindex.addInvertedFileEntry(term, putvector)
        self.sdbindex._flushcache()
        time.sleep(4)
        getvector = self.sdbindex.getInvertedFileEntry(term)
        #print >> sys.stderr, "getvector = ", getvector
        self.assertEquals(getvector, putvector)
        
        
    def testAddAndHashUrl(self):
        self.sdbindex._warningthisdeletesallsimpledbdomains()
        self.sdbindex._createsimpledbdomains()
        url = "http://atbrox.com"
        urlhash = self.sdbindex.addAndHashUrl(url)
        self.assertEquals(str(url.__hash__()), urlhash)
        
    def testGetUrl(self):
        self.sdbindex._warningthisdeletesallsimpledbdomains()
        self.sdbindex._createsimpledbdomains()
        url = "http://www.atbrox.com"
        urlhash = self.sdbindex.addAndHashUrl(url)
        self.assertEquals(url, self.sdbindex.getUrl(urlhash))
        
if __name__ == '__main__':
    unittest.main()

    
    
        
