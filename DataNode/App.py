from flask import Flask, url_for
from flask import json
import requests
from flask import request
from DataNode import *
from Info import *
from os import environ
import time
import threading

app = Flask(__name__)
# To-do need to get the hard coded Name Node IP
PORT = 5001
MY_IP = 'http://127.0.0.1:' + str(PORT)
NAME_NODE_IP = 'http://localhost:5000'
AWS_IP_REQUEST = 'http://checkip.amazonaws.com'
WAIT_TIME = 30
dataNode = DataNode(MY_IP, NAME_NODE_IP)
#Uncomment for EC2
NAME_NODE_IP = "http://34.219.37.232:5000"
response = requests.get(AWS_IP_REQUEST)
MY_IP = "http://" + response.text.strip() + ":" + str(PORT)
dataNode = DataNode(MY_IP, NAME_NODE_IP)


#Move this part bellow the start runner function
#Once its moved below it will run on startup with errors (Says there's some issue with a GET method...)
#@app.before_first_request
def sendBlockList():        
    # To-do needs to be done every 30 seconds
    time.sleep(2)
    while True:
        print(dataNode.getIP())
        report = dataNode.getBlockReport()
        requests.put(dataNode.Name_Node_IP + '/BlockReport',
                                json={'dn_ip': dataNode.getIP(), 'dn_block_report': report})
        time.sleep(WAIT_TIME)

thread = threading.Thread(target=sendBlockList)
thread.start()


@app.route('/', methods=['GET'])
def home():
    """
    Display the home page
    """
    return "Hello World!"


# @app.route('/TestPath/', methods=['GET'])
# def testMethod():
#     # request IP from amazon aws
#     global NAME_NODE_IP
#     NAME_NODE_IP = environ.get('NAME_NODE_IP')
#     response = requests.get(AWS_IP_REQUEST)
#     global MY_IP
#     MY_IP = "http://" + response.text + ":5000"
#     global dataNode
#     dataNode = DataNode(MY_IP)
#     # t = threading.Thread(name='sendBlockList', target=sendBlockList)
#     #t.start()
#     return MY_IP

# @app.route('/DeliverBlockList', methods=['GET']

@app.route('/Block', methods=['PUT'])
def storeBlock():
    print('---- TEST Add -----')
    Block_ID = request.json['Block_ID']
    Address = request.json['Address']
    Stored = request.json['Stored']
    Data = request.json['Data']
    print(Block_ID)
    print(Address)
    print(Stored)
    #print(Data)
    # create Info class input
    input = Info(Block_ID, Address, Stored, Data)
    # call the dataNode received method to store data.
    # This should return another data node to send data.
    otherDataNode, stored_data = dataNode.received(input)
    report = dataNode.getBlockReport()
    if otherDataNode is not None:
        requests.put(otherDataNode + '/Block', json={'Block_ID': Block_ID, 'Address': Address, 'Stored': stored_data, 'Data': Data})
        requests.put(NAME_NODE_IP + '/BlockReport', json={'dn_ip': MY_IP, 'dn_block_report': report})
    return json.dumps({'success':True}), 200, {'ContentType':'application/json'}


@app.route('/Block', methods=['DELETE'])
def removeBlock():
    # Duplicate the info.
    print('---- TEST Delete -----')
    Block_ID = request.json['Block_ID']
    Address = request.json['Address']
    Stored = request.json['Stored']
    print(Block_ID)
    print(Address)
    print(Stored)
    input = Info(Block_ID, Address, Stored, None)
    newDataNode, updated_Stored = dataNode.remove(input)
    if newDataNode is not None:
        requests.delete(newDataNode + '/Block', json={'Block_ID': Block_ID, 'Address': Address, 'Stored': updated_Stored})
    return json.dumps({'success':True}), 200, {'ContentType':'application/json'}


@app.route('/Block/', methods=['GET'])
def sendDataBlock():
    # To-do needs to be done every 30 seconds
    Block_Id = request.args.get("Block_ID")
    data = dataNode.retrieveData(Block_Id)
    if data is not False:
        return data
    else:
        return json.dumps({'success':False, 'Error_Message': 'Block Not Found'}), 404, {'ContentType':'application/json'}

@app.route('/DuplicateBlock', methods=['PUT'])
def duplicateBlock():
    # Duplicate the info.
    print('---- TEST Duplicate -----')
    Block_ID = request.json['Block_ID']
    Address = request.json['Address']
    Stored = request.json['Stored']
    print(Block_ID)
    print(Address)
    print(Stored)
    input = Info(Block_ID, Address, Stored, None)
    newDataNode, block_data = dataNode.duplicate(input)
    if newDataNode is not None:
        requests.put(newDataNode + '/Block', json={'Block_ID': Block_ID, 'Address': Address, 'Stored': Stored, 'Data': block_data})
        return json.dumps({'success':True}), 200, {'ContentType':'application/json'}
    if newDataNode is None:
        return json.dumps({'success':False, 'Error_Message': 'Block Not Found'}), 404, {'ContentType':'application/json'}

if __name__ == '__main__':
    #start_runner()
    app.run(port=PORT, threaded=True)