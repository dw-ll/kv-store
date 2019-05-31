import logging
import os


class ViewList:
    def __init__(self, startString, ownSocket):
        s = startString.split(',')
        for a in s:
            if a.split(":")[0] == ownSocket.split(":")[0]:
                s.remove(a)
                self.ownSocket = a
                break
        self.viewArray = s

    def array(self):
        return self.viewArray

    def remove(self, delAddress):
        self.viewArray.remove(delAddress)

    def add(self, addAddress):
        self.viewArray.append(addAddress)

    def __repr__(self):
        x = ","
        x = x.join(self.viewArray)
        if x:
            x = self.ownSocket + "," + x
        else:
            x = self.ownSocket

        return x

    def __getitem__(self, key):
        return self.viewArray[key]




