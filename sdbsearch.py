import sys
import time

import re
#from nilsimsa import *
from sdbindex import SimpleDBIndex
import logging
import nltk
from nltk import word_tokenize
from nltk.corpus import wordnet as wn

class SimpleDBSearch:
    def __init__(self):
        self.sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
        self.termindex = {}
        self.sdbindex = SimpleDBIndex()
        self.PAGESEPARATOR = """&""" # safe since only numbers used
        self.POSITIONSEPARATOR="""|""" # safe since only numbers separating
        self.CLEANREGEXP = re.compile(r"(\(|\)|\,|\:|\;)")
        
    def _getSentences(self, document):
        return self.sent_detector.tokenize(document)
    
    def _getAllTermsInOrder(self, document):
        allterms = []
        sentences = self._getSentences(document)
        for sentence in sentences:
            allterms += sentence.split()
        return allterms       
    
    def _getTermsWithPositions(self, document):
        allterms = self._getAllTermsInOrder(document)
        termswithpos = {}
        for pos, term in enumerate(allterms):
            indexterm = self.CLEANREGEXP.sub("", term).lower().strip()
            indexterm.lower().strip()
            if indexterm != "":
                termswithpos[indexterm] = termswithpos.get(indexterm,[]) + [str(pos)]
        return termswithpos
                      
    def index(self, url, document):
        urlhash = self.sdbindex.addAndHashUrl(url)
        termswithpositions = self._getTermsWithPositions(document)
        for term in termswithpositions:
            # first item is the hash of the url, rest is 
            self.termindex[term] = self.termindex.get(term,"")
            self.termindex[term] += self.PAGESEPARATOR
            self.termindex[term] += self.POSITIONSEPARATOR.join([urlhash] + termswithpositions[term])
        return self.termindex
            
    def writeIndexToSDB(self):
        i = 0
        numterms = len(self.termindex.keys())
        for term in self.termindex:
            print i, " of ", numterms,  " adding tv for term: '%s' to SDB" % (term)
            self.sdbindex.addInvertedFileEntry(term, self.termindex[term])
            i += 1
        print "flushing cache"
        self.sdbindex._flushcache()
            
    def extractUrlHashListFromInvertedFileEntry(self, invertedFileEntry):
        pages = invertedFileEntry.split(self.PAGESEPARATOR)
        # TODO: extract positions
        urlhashlist = []
        for page in pages:
            if page == "":
                continue
            urlhashlist += [page.split(self.POSITIONSEPARATOR)[0]]
        urlhashlist.sort()
        return urlhashlist
        
    def query(self, query):
        urlhashforterms = {}
        terms = query.split()
        for term in terms:
            t0 = time.time()
            invertedFileEntry = self.sdbindex.getInvertedFileEntry(term)
            t1 = time.time()
            print "fetchtime|%f" % (t1-t0)
            #print invertedFileEntry
            urlhashes = self.extractUrlHashListFromInvertedFileEntry(invertedFileEntry)
            #print urlhashes
            for urlhash in urlhashes:
                urlhashforterms[urlhash] = urlhashforterms.get(urlhash, []) + [term]
                
        results = ((len(urlhashforterms[urlhash]), urlhash) for urlhash in urlhashforterms)
        #print "raw inverted file vector results: "
        #print results
        #print "urls with matches"
        results = []
        for matches, urlhash in results:
            # TODO: look up url with urlhash
            #print self.sdbindex.getUrl(urlhash)
            results.append(self.sdbindx.getUrl(urlhash))
        return results

