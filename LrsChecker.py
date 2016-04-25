# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 11:13:24 2016

@author: econ24
"""

import threading, time

from LrsThread import LrsThread

class LrsChecker(threading.Thread):
    
    def __init__(self, num):
        threading.Thread.__init__(self)
        self.totalLinkIds = len(LrsThread.linkIdList)
        self.startTime = None
        self.numThreads = num
        self.sleepTime = min(self.totalLinkIds // 5, 15)
        
    def run(self):
        self.startTime = time.time()
        
        self.firstMessage()
        
        while len(LrsThread.linkIdList):       
            
            self.updateMessage()
            
            time.sleep(self.sleepTime)
            
        self.lastMessage()
            
            
    def firstMessage(self):
        print "<LrsChecker> Start time:", time.ctime(self.startTime)
        print "<LrsChecker> Number working threads:", self.numThreads
        print "<LrsChecker> Total link IDs:", self.totalLinkIds, "\n"
        
    def updateMessage(self):
        numRemaining = len(LrsThread.linkIdList)
        numProcessed = self.totalLinkIds - numRemaining
        
        print "<LrsChecker> Number links processed:", numProcessed
        print "<LrsChecker> Number links remaining:", numRemaining, "\n"
        
    def lastMessage(self):
        now = time.time()
        
        print "<LrsChecker> End time:", time.ctime(now)
        print "<LrsChecker> Total time:", (now-self.startTime)
        print "<LrsChecker> Number links processed:", self.totalLinkIds
        print "<LrsChecker> Exiting..."