# -*- coding: utf-8 -*-
"""
Created on Wed Apr 20 18:17:53 2016

@author: econ24
"""

import math

class Vector2d():
    def __init__(self, x=0.0, y=0.0):
        self.x = x
        self.y = y
        
    def magnitude(self):
        return math.sqrt(self.x**2+self.y**2)
        
    def normalize(self):
        mag = self.magnitude()
        if mag != 0:
            self.x /= mag
            self.y /= mag
        return self
        
    def unitVec(self):
        mag = self.magnitude()
        if mag != 0:
            return Vector2d(self.x/mag, self.y/mag)
        return Vector2d(self.x, self.y)
        
    def dotProduct(self, vec):
        return self.x * vec.x + self.y * vec.y
        
    def __str__(self):
        return str(self.x) + ", " + str(self.y)
        
    def __repr__(self):
        return "x=" + str(self.x) + ", y=" + str(self.y)
        
    def __add__(self, vec):
        return Vector2d(self.x + vec.x, self.y + vec.y)
    def __sub__(self, vec):
        return Vector2d(self.x - vec.x, self.y - vec.y)
        
    def __iadd__(self, vec):
        self.x += vec.x
        self.y += vec.y
        return self
    def __isub__(self, vec):
       self.x -= vec.x
       self.y -= vec.y
       return self
       
    def __abs__(self):
        return Vector2d(abs(self.x), abs(self.y))
          
        
def test():
    vec1 = Vector2d(1,1)
    vec2 = Vector2d(-1, -1)
    
    print vec1.magnitude()
    print vec1.unitVec()
    print vec1.unitVec().magnitude()
    print vec1.unitVec().dotProduct(vec2.unitVec())
    print vec1 + vec2
    vec1 += Vector2d(1,1)
    print vec1
    print abs(vec2)
        
if __name__ == "__main__":
    test()