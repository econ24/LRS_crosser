# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 11:13:24 2016

@author: econ24
"""

import threading, time

class ThreadChecker(threading.Thread):
    
    def __init__(self, threadList, linkIdList):
        threading.Thread.__init__(self)
        self.linkIdList = linkIdList
        self.totalLinkIds = len(linkIdList)
        self.startTime = None
        self.threadList = threadList
        self.numThreads = len(threadList)
        self.sleepTime = min(self.totalLinkIds // 5, 15)
        
    def run(self):
        self.startTime = time.time()
        
        self.firstMessage()
        
        while len(self.linkIdList):       
            
            self.updateMessage()
            
            time.sleep(self.sleepTime)
            
        self.lastMessage()
            
            
    def firstMessage(self):
        print "<ThreadChecker> Start time:", time.ctime(self.startTime)
        print "<ThreadChecker> Number of working threads:", self.numThreads
        print "<ThreadChecker> Total link IDs:", self.totalLinkIds, "\n"
        
    def updateMessage(self):
        numRemaining = len(self.linkIdList)
        numProcessed = self.totalLinkIds - numRemaining
        
        print "<ThreadChecker> Number of links processed:", numProcessed
        print "<ThreadChecker> Number of links remaining:", numRemaining
        numAlive = reduce(lambda a, c: a+1 if c.isAlive() else a, self.threadList, 0)
        print "<ThreadChecker> Number of active threads:", numAlive, "\n"
        
    def lastMessage(self):
        now = time.time()
        
        print "<ThreadChecker> End time:", time.ctime(now)
        print "<ThreadChecker> Total time:", (now-self.startTime)
        print "<ThreadChecker> Number of links processed:", self.totalLinkIds
        print "<ThreadChecker> Exiting..."