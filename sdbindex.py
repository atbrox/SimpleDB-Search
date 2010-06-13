import time
from simpledb import *
import sys

class SimpleDBIndex:
    def __init__(self, domainprefix="atbrox"):
        self.NUMKEYVALUEPAIRS = 250 # keep 6 for other stuff
        self.MAXVALUESIZE = 1000
        self.MAXITEMSIZE = self.NUMKEYVALUEPAIRS*self.MAXVALUESIZE
        self.MAXATTRIBUTESPERDOMAIN = 1000000000
        self.MAXDOMAINSIZE = self.MAXATTRIBUTESPERDOMAIN*self.MAXVALUESIZE #
        self.ITEMINDEXSEPARATOR = """@#$@"""
        self.MAXBATCHSIZE = 500000
        self.domainprefix = domainprefix
        self.domain = {}
        self.DOMAINFORURLS = self.domainprefix + "url"
        self.DOMAINFORTERMS = self.domainprefix + "term"
        self.YOURAWSPUBLICKEY = ""
        self.YOURAWSPRIVATEKEY = ""
        assert self.YOURAWSPUBLICKEY == "", "add your aws public key"
        assert self.YOURAWSPRIVATEKEY == "", "add your aws public key"
        self.sdb = SimpleDB(self.YOURAWSPUBLICKEY, self.YOURAWSPRIVATEKEY)
        self._createsimpledbdomains()
        self.batchcache = []
        self.batchsize = 0

    def _createsimpledbdomains(self):
        if not self.sdb.has_domain(self.DOMAINFORTERMS):
            self.sdb.create_domain(self.DOMAINFORTERMS)
        if not self.sdb.has_domain(self.DOMAINFORURLS):
            self.sdb.create_domain(self.DOMAINFORURLS)

    def _warningthisdeletesallsimpledbdomains(self):
        if self.sdb.has_domain(self.DOMAINFORTERMS):
            del self.sdb[self.DOMAINFORTERMS]
        if self.sdb.has_domain(self.DOMAINFORURLS):
            self.sdb[self.DOMAINFORURLS]
        
    def addAndHashUrl(self, url):
        # TODO: perhaps also add batched write for urls
        urlhash = str(url.__hash__())
        self.sdb[self.DOMAINFORURLS][url] = {"urlhash":urlhash}
        self.sdb[self.DOMAINFORURLS][urlhash] = {"url":url}
        return urlhash
        
    def getUrl(self, urlhash):
        return self.sdb[self.DOMAINFORURLS][str(urlhash)]["url"]
        
    def _getTermLine(self, term, i):
        termlineidx = i / self.MAXITEMSIZE # line operating on
        if termlineidx > 0: # special or default case
            term += self.ITEMINDEXSEPARATOR + str(termlineidx)
        termLine = self.domain[term] = self.domain.get(term, {})
        return term, termLine
    
    def _termLineSize(self, termLine):
        return len("".join(termLine.keys() + termLine.values()))
    
    def _flushcache(self):
        print "flushcache - len(bc), bs", len(self.batchcache), self.batchsize
        #print "batchcache content = ", self.batchcache
        if len(self.batchcache) > 0:
            self.sdb.batch_put_attributes(self.DOMAINFORTERMS,self.batchcache)
            self.batchcache = []
            self.batchsize = 0
    
    def _store(self, term, termLine, flush=False):
        termLineSize = self._termLineSize(termLine)

        if termLineSize + self.batchsize > self.MAXBATCHSIZE:
            self._flushcache()
        if len(self.batchcache) > 24: # max 25 attributes per flush
            self._flushcache()
        self.batchsize += termLineSize
        self.batchcache.append( (term, termLine) )
        # don't store duplicates in memory
        self.domain[term] = {}
        if flush:
            self._flushcache()
            
    def addInvertedFileEntry(self, term, vector):
        print >> sys.stderr, "aife - term, v.len = ", term, len(vector)
        currentTerm = ""
        currentTermline = {}
        batchcache = []
        lastSavedTerm = None

        # iterates on character level
        for i, v in enumerate(vector):
            indexTerm, termLine = self._getTermLine(term, i)
            bucket = str((i/self.MAXVALUESIZE)%self.NUMKEYVALUEPAIRS)
            if i%989 == 0:
                print i, "indexterm, bucket = ", indexTerm, bucket, currentTerm

            if currentTerm != indexTerm and currentTerm != "":
                print "storing ct, ctl = ", currentTerm, len(currentTermline)
                self._store(currentTerm, currentTermline)
                lastSavedTerm = currentTerm
            currentTermline = termLine
            currentTerm = indexTerm
            currentTermline[bucket] = currentTermline.get(bucket, "") + v

        if currentTerm != lastSavedTerm:
            print "2. storing ct, ctl = ", currentTerm, currentTermline
            self._store(currentTerm, currentTermline, flush=True)
            lastSavedTerm = currentTerm

    def _getInOrderValues(self,term):
        t0 = time.time()
        result = self.sdb[self.DOMAINFORTERMS][term]
        t1 = time.time()
        print "rawfetchtime|%f" % (t1-t0)
        intkeys = [int(k) for k in result.keys()]
        sortedkeys = sorted(intkeys)
        return "".join([result[str(k)] for k in sortedkeys])

                        
    def getInvertedFileEntry(self, term):
        # only return first one for now
        # get first domain
        termvector = self._getInOrderValues(term)
        morekeys = True
        i = 1
        while morekeys:
            newterm = term + self.ITEMINDEXSEPARATOR + str(i)
            if self.sdb[self.DOMAINFORTERMS][newterm] != {}:
                termvector += self._getInOrderValues(newterm)
            else:
                #print >> sys.stderr, "didn't find key = ", newterm
                morekeys = False
            i += 1
        return termvector
 


        

