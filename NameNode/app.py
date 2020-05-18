"""
This script runs the application for the NameNode server.
It contains the definition of routes and route logic for NameNode
"""

from flask import Flask
from flask import json
from flask import request
from flask import jsonify
from NameNode import *
app = Flask(__name__)
name_node = NameNode()



V1_ROUTES = dict()
V1_ROUTES["NewFile"] = '/NewFile'
V1_ROUTES["CreateDir"] = '/CreateDirectory'
V1_ROUTES["DirectoryContents_w_path"] = '/DirectoryContents/<file_path>'
V1_ROUTES["DirectoryContents_root"] = '/DirectoryContents/'
V1_ROUTES["DeliverBlockReport"] = '/DeliverBlockReport'
V1_ROUTES["GetBlockList_root"] = '/GetBlockList/<file_name>'
V1_ROUTES["GetBlockList_w_path"] = '/GetBlockList/<file_path>/<file_name>'
V1_ROUTES["GetDataNodes"] = '/GetDataNodes'
V1_ROUTES["DeleteFile_root"] = '/DeleteFile/<file_name>'
V1_ROUTES["DeleteFile_w_path"] = '/DeleteFile/<file_path>/<file_name>'
V1_ROUTES["DeleteDirectory"] = '/DeleteDirectory/<file_path>'
V1_ROUTES["ForceHeartbeatInventory"] = '/ForceHeartbeatInventory'

V2_ROUTES = dict()
V2_ROUTES["NewFile"] = '/File'
V2_ROUTES["CreateDir"] = '/Folders'
V2_ROUTES["DirectoryContents_w_path"] = '/Folder/<file_path>'
V2_ROUTES["DirectoryContents_root"] = '/Folder/'
V2_ROUTES["DeliverBlockReport"] = '/BlockReport'
V2_ROUTES["GetBlockList_root"] = '/File/<file_name>'
V2_ROUTES["GetBlockList_w_path"] = '/File/<file_path>/<file_name>'
V2_ROUTES["GetDataNodes"] = '/DataNodes'
V2_ROUTES["DeleteFile_root"] = '/File/<file_name>'
V2_ROUTES["DeleteFile_w_path"] = '/File/<file_path>/<file_name>'
V2_ROUTES["DeleteDirectory"] = '/Folder/<file_path>'
V2_ROUTES["ForceHeartbeatInventory"] = '/ForceHeartbeatInventory'

ROUTES = V2_ROUTES

def json_response(data):
    response = app.response_class(
        response=json.dumps(data),
        status=200,
        mimetype='application/json')
    return response


@app.route(ROUTES["CreateDir"], methods = ['POST'])
def create_directory():
    file_path = request.json["file_path"]
    file_path = name_node.cleanse_file_path(file_path)
    #print(request.json)
    if name_node.path_exists(file_path):
        return "Directory already exists", 500
    is_success = name_node.create_directory(file_path)
    if is_success is not None:
        return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 
    else:
        return json.dumps({'success':False}), 500, {'ContentType':'application/json'} 


@app.route(ROUTES["DirectoryContents_root"], defaults={'file_path': None}, methods = ['GET'])
@app.route(ROUTES["DirectoryContents_w_path"], methods = ['GET'])
def directory_contents(file_path):
    if file_path is not None:
        file_path = name_node.cleanse_file_path(file_path)
    if not name_node.path_exists(file_path):
        return "Directory does not exist", 400
    content_list = name_node.get_directory_for_client(file_path)
    if content_list is not None:
        return json_response(content_list)
    else:
        return 500


#Receive updated block information
@app.route(ROUTES["DeliverBlockReport"], methods = ['PUT'])
def deliver_block_report():
    #print(request.json)
    dn_ip = request.json["dn_ip"]
    dn_block_report = request.json["dn_block_report"]
    dn_blocks_saved = len(dn_block_report)
    if not name_node.is_registered_data_node(dn_ip):
        name_node.register_dn(dn_ip, dn_blocks_saved)
    if name_node.is_valid_block_report(dn_block_report):
        name_node.consume_block_report(dn_ip, dn_block_report)
        name_node.update_dn_space(dn_ip, dn_blocks_saved)
        return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 
    else:
        return "Invalid block report", 422
        #Todo - include example block report format in response
    return "Internal Server Error", 500


#Receive new file request from client
@app.route(ROUTES["NewFile"], methods = ['POST'])
def new_file():
    file_path = request.json["file_path"]
    file_name = request.json["file_name"]
    file_size = request.json["file_size"]
    if file_path is not None:
        file_path = name_node.cleanse_file_path(file_path)
    if name_node.file_exists(file_name, file_path):
        return "File already exists", 400
    if name_node.has_min_num_datanodes():
        block_list = name_node.add_file(file_name, file_size, file_path)
        return json_response(block_list)
    else:
        return "Not enough data nodes to perform operation", 500


#Retrieve block list for client
@app.route(ROUTES["GetBlockList_root"], defaults={'file_path': None}, methods = ['GET'])
@app.route(ROUTES["GetBlockList_w_path"], methods = ['GET'])
def get_block_list(file_path,file_name):
    if file_path is not None:
        file_path = name_node.cleanse_file_path(file_path)
    if not name_node.file_exists(file_name, file_path):
        return "File does not exist", 400
    block_list = name_node.get_block_list(file_name, file_path)
    return json_response(block_list)


#Diagnostic readout
@app.route(ROUTES["GetDataNodes"], methods = ['GET'])
def get_data_nodes():
    return json_response(name_node.data_nodes)


@app.route(ROUTES["DeleteFile_root"], defaults={'file_path': None}, methods = ['DELETE'])
@app.route(ROUTES["DeleteFile_w_path"], methods = ['DELETE'])
def delete_file(file_path, file_name):
    if file_path is not None:
        file_path = name_node.cleanse_file_path(file_path)
    if not name_node.file_exists(file_name,file_path):
        return 400, "File does not exist", 400
    block_list = name_node.delete_file(file_name,file_path)
    if block_list is not None:
        return json_response(block_list)
    else:
        return "Internal Server Error", 500


@app.route(ROUTES["DeleteDirectory"], methods = ['DELETE'])
def delete_folder(file_path):
    
    if file_path is not None:
        file_path = name_node.cleanse_file_path(file_path)
    if file_path is None or file_path == "":
        return "Cannnot remove the root directory", 500
    if not name_node.path_exists(file_path):
        return "path does not exist", 400
    list_of_block_lists = name_node.delete_folder(file_path)
    if list_of_block_lists is not None:
        return json_response(list_of_block_lists)
    else:
        return "Internal Server Error", 500


@app.route(ROUTES["ForceHeartbeatInventory"], methods = ['GET'])
def force_heartbeat_inventory():
    name_node.heartbeat_inventory()
    return "Inventory Done", 200


@app.route('/AdminDirectoryContents/', defaults={'file_path': None}, methods = ['GET'])
@app.route('/AdminDirectoryContents/<file_path>', methods = ['GET'])
def admin_directory_contents(file_path):
    if not name_node.path_exists(file_path):
        return "Directory does not exist", 400
    content_list = name_node.get_directory(file_path)
    if content_list is not None:
        return json_response(content_list)
    else:
        return 500


@app.route('/Reset/', methods = ['GET'])
def reset_namenode():
    name_node.reset()
    return "Reset NameNode Done", 200


if __name__ == '__main__':
    import os
    HOST = os.environ.get('SERVER_HOST', 'localhost')
    try:
        PORT = int(os.environ.get('SERVER_PORT', '5000'))
    except ValueError:
        PORT = 5000
    app.run(HOST, PORT)