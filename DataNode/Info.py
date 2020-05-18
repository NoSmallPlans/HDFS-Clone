class Info:
    def __init__(self, blockID, address=None, stored=None, data=None):
        self.blockID = blockID
        if address is None:
            self.address = ["", "", ""]
        else:
            self.address = address

        if stored is None:
            self.stored = [False, False, False]
        else:
            self.stored = stored
        self.data = data

    def getBlockID(self):
        return self.blockID

    def getAddress(self):
        return self.address

    def getStored(self):
        return self.stored

    def getData(self):
        return self.data

    def setData(self, data):
        self.data = data

    def setStoredIndex(self, i, val):
        self.stored[i] = val

    def setAddressIndex(self, i, address):
        self.address[i] = address
