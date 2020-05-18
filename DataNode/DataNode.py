import os
import requests
import time
import threading

WAIT_TIME = 30

class DataNode:
    dataList = [b'line 1 I have a pen\nline 2 I have an apple\nline 3 Ah\nline 4 Apple pen\nline 5 ',
                b'I have a pen\nline 6 I have pineapple\nline 7 Ah\nline 8 Pineapple pen\nline 9 Ap',
                b'ple pen\nline 10 Pineapple pen\nline 11 Ah\nline 12 Pen Pie Pineapple Apple Pen']

    def __init__(self, IP=None, Name_Node_IP=None):
        self.IP = IP
        self.Name_Node_IP = Name_Node_IP
        self.block_list = set([])
        #self.startThread()

    # def startThread(self):
    #     t = threading.Thread(name='sendBlockList', target=self.sendBlockList())
    #     t.start()
    #
    # def sendBlockList(self):
    #     # To-do needs to be done every 30 seconds
    #     while True:
    #         print(self.getIP())
    #         report = self.getBlockReport()
    #         response = requests.put(self.Name_Node_IP + '/DeliverBlockReport',
    #                                 json={'dn_ip': self.getIP(), 'dn_block_report': report})
    #         time.sleep(WAIT_TIME)


    # def testData(self):
    #     self.block_list['block_0'] = b'line 1 I have a pen\nline 2 I have an apple\nline 3 Ah\nline 4 Apple pen\nline 5 '
    #     self.block_list[
    #         'block_1'] = b'I have a pen\nline 6 I have pineapple\nline 7 Ah\nline 8 Pineapple pen\nline 9 Ap'
    #     self.block_list[
    #         'block_2'] = b'ple pen\nline 10 Pineapple pen\nline 11 Ah\nline 12 Pen Pie Pineapple Apple Pen'

    def getIP(self):
        return self.IP

    def received(self, data):
        # Todo get data with Info class
        # Info class contains block_ID, address[] addr, bool[] stored, Data data
        # store data for current Node
        found = False
        myself = 0
        size = len(data.address)
        while (myself < size) and not found:
            print("myself: " + str(myself))
            print(", " + data.address[myself])
            if data.address[myself] == self.IP:
                found = True
            else:
                myself = myself+1

        data.stored[myself] = self.storeData(data.blockID, data.data)
        # send data to next Data node
        sent = True
        next_Data_Node = 0
        while (next_Data_Node < size) & sent:
            print(next_Data_Node)
            if not data.stored[next_Data_Node]:
                sent = False
                return data.address[next_Data_Node], data.stored
            next_Data_Node = next_Data_Node+1
        return None, None

    def duplicate(self, data):
        # send data to false stored data node
        found = False
        new_Data_Node = 0
        data_block = self.retrieveData(data.blockID)
        if data_block is not False:
            size = len(data.address)
            while (new_Data_Node<size) and not found:
                if data.stored[new_Data_Node] == False:
                    return data.address[new_Data_Node] , data_block
                    #self.send(data.address[new_Data_Node], data)
                    found = True
                else:
                    new_Data_Node = new_Data_Node + 1
        return None, None

    def getBlockReport(self):
        #return list(self.block_list.keys())
        return list(self.block_list)

    def storeData(self, blockId, block_data):
        # Todo find a way to store data besides dictionary
        #self.block_list[block_Id] = block_data
        file_name = blockId
        #add block name to blocklist
        self.block_list.add(blockId)
        with open(file_name, 'w') as file:
            file.write(block_data)
        return True

    def retrieveData(self, blockId):
        # retrieve data address from block_list dictionary
        #return self.block_list[blockID]
        file_name = blockId
        exist = os.path.isfile(file_name)
        if exist:
            with open(file_name, 'r') as file:
                output = file.read()
            return output
        else:
            return False

    def remove(self, info_data):
        # Info class contains block_ID, address[] addr, bool[] stored, Data None
        # store data for current Node
        found = False
        myself = 0
        size = len(info_data.address)
        while (myself < size) and not found:
            print("myself: " + str(myself))
            print(", " + info_data.address[myself])
            if info_data.address[myself] == self.IP:
                found = True
            else:
                myself = myself+1

        info_data.stored[myself] = self.removeData(info_data.blockID)
        # send data to next Data node
        sent = True
        next_Data_Node = 0
        while (next_Data_Node < size) & sent:
            print(next_Data_Node)
            if info_data.stored[next_Data_Node]:
                sent = False
                return info_data.address[next_Data_Node], info_data.stored
            next_Data_Node = next_Data_Node+1
        return None, None

    def removeData(self, blockId):
        file_name = blockId
        # removes the file from os
        if os.path.exists(file_name):
            os.remove(file_name)
            #remove block name from block list
            self.block_list.discard(blockId)
            return False
        else:
            return True




