import unittest
import mockito
from mockito import *

import sys

from sdbsearch import SimpleDBSearch

class TestSimpleDBSearch(unittest.TestCase):
    def setUp(self):
        self.sdbsearch = SimpleDBSearch()
        self.document = """This is a sentence. And this is another one! But is this the third?"""
    
    def testGetSentences(self):
        sentences = self.sdbsearch._getSentences(self.document)
        self.assertEquals(3, len(sentences))
    
    def testGetAllTermsInOrder(self):
        termsinorder = self.sdbsearch._getAllTermsInOrder(self.document)
        self.assertEquals("This", termsinorder[0])
        self.assertEquals("is", termsinorder[1])
        self.assertEquals("third?", termsinorder[len(termsinorder)-1])
    
    def testGetTermsWithPositions(self):
        termswithpositions = self.sdbsearch._getTermsWithPositions(self.document)
        self.assertEquals(["1","6","10"], termswithpositions["is"])
    
   
    def testIndex(self):
        url = "http://www.foo.com"
        when(self.sdbsearch.sdbindex).addAndHashUrl(any(str)).thenReturn(url)
        termindex = self.sdbsearch.index(url, self.document)
        termindex = self.sdbsearch.index(url, self.document)
        print >> sys.stderr, termindex
        self.assertTrue("1|6|10" in termindex["is"])
            
    def testWriteIndexToSDB(self):
        url = "http://www.foo.com"
        when(self.sdbsearch.sdbindex).addAndHashUrl(any(str)).thenReturn(url)
        termindex = self.sdbsearch.index(url, self.document)
        when(self.sdbsearch.sdbindex).addInvertedFileEntry(any(str),any(str)).thenReturn(True)
        self.sdbsearch.writeIndexToSDB()
        # 10 unique terms in input document, called once per term
        self.assertEquals(None, 
                          verify(self.sdbsearch.sdbindex, 
                                 times=10).addInvertedFileEntry(any(str),any(str)))
            
    def testExtractUrlHashListFromInvertedFileEntry(self):
        url = "http://www.foo.com"
        # simulate hash with reversing url -> url[::-1]
        when(self.sdbsearch.sdbindex).addAndHashUrl(any(str)).thenReturn(url[::-1])
        termindex = self.sdbsearch.index(url, self.document)
        invertedFileEntry = termindex["this"]
        urlhashlist = self.sdbsearch.extractUrlHashListFromInvertedFileEntry(invertedFileEntry)
        self.assertEquals([url[::-1]], urlhashlist)
        
    def testQuery(self):
        pass
    
if __name__ == '__main__':
    unittest.main()
    