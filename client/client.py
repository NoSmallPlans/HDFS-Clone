from flask import Flask, jsonify, request, render_template, flash, redirect, url_for
#from werkzeug import secure_filename
import requests
import os, sys
import time
import json
import boto3
from os import environ


# create app instance
app = Flask(__name__, template_folder="templates")
app.secret_key = "super secret key"
ACCESS_KEY = environ['ACCESS_KEY']
SECRET_KEY = environ['SECRET_KEY']
NUM_REPLICA = 3
ROOT_DIR_PATH = '' 
NAMENODE_ADDR = environ['NAME_NODE_IP']


@app.route('/', methods=['GET'])
def home():
	"""
	Display the home page
	"""
	return render_template("home.html")


@app.route('/directory', methods=['GET'])
def show_directory():
	"""
	Display the directory page with directory contents received from NameNode
	"""
	directory = []

	try:
		url = NAMENODE_ADDR + '/Folder/' + ROOT_DIR_PATH
		response = requests.get(url)
		contents = response.json()	

		if contents is not None:
			convert_directory(contents, ROOT_DIR_PATH, directory)

		for i in directory:
			print(i)

	except requests.exceptions.RequestException as err:
		print(err)

	# Get the list of files in S3 bucket for file uploading
	s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
	my_bucket = s3.Bucket('finalprojectcloud1')
	s3_items = []

	for item in my_bucket.objects.all():
		s3_items.append(item.key)

	return render_template("directory.html", contents=directory, objs=s3_items)


@app.route('/directory/filestorage/', methods=['POST'])
def store_file():
	"""
	Store a file uploaded by client
	"""
	filename = request.form['filename']
	filepath = request.form['filepath']

	s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
	obj = s3.Object('finalprojectcloud1', filename)

	filedata = obj.get()['Body'].read() # should be in bytes
	filesize = len(filedata)

	try:
		# Call NameNode to get the blocklist 
		filepath_toNN = convert_path(filepath, filename)
		response = requests.post(NAMENODE_ADDR + '/File', json={'file_path': filepath_toNN, 'file_name': filename, 'file_size': filesize})
		blocklist = response.json()
		print(blocklist)

	except requests.exceptions.RequestException as err:
		print(err)

	# generate data blocks
	data_blocks = split_file(filedata, filesize, len(blocklist.keys()))

	i = 0

	for blockID in blocklist:
		print('--- blockID -- ' + str(blockID))
		# [address1, address2, address3]
		for address in blocklist.get(blockID):
			print('* ip -- ' + str(address))

		addrs = blocklist.get(blockID)
		
		# Call DataNode to store file in Info format
		try:
			requests.put(addrs[0] + '/Block', json={'Block_ID': blockID, 'Address': addrs, 'Stored': [False, False, False], 'Data': data_blocks[i].decode('utf-8')})

		except requests.exceptions.RequestException as err:
			print(err)

		i += 1

	flash('File stored successfully!', 'success')
	return jsonify({'filename' : filename})


@app.route('/directory/file/', methods=['POST'])
def download_file():
	"""
	Download a file given name
	"""
	filepath = request.form['filepath']
	filename = filepath.split('/')[-1]
	
	try:
		# Call NameNode to get the blocklist
		filepath_toNN = convert_path(filepath, '/' + filename)
		response = requests.get(NAMENODE_ADDR + '/File/' + filepath_toNN + '/' + filename)
		blocklist = response.json()

		dataList = []

		for blockID in blocklist:	
			print('--- blockID -- ' + str(blockID))
			# [address1, address2, address3]
			for address in blocklist.get(blockID):
				print('* ip -- ' + str(address))

			addrs = blocklist.get(blockID)

			# Call DataNode to get file block

			dataBlock = ''

			for addr in addrs:
				response = requests.get(addr + '/Block/', params={'Block_ID':blockID})
				dataBlock = response.text
				if dataBlock is not None:
					break			

			dataList.append(dataBlock.encode())

		joinSplit(dataList, filename)

	except requests.exceptions.RequestException as err:
		print(err)

	return jsonify({'filename' : filename})


@app.route('/directory/filetrashcan/', methods=['DELETE'])
def delete_file():	
	"""
	Delete a file with a given name
	"""
	filepath = request.form['filepath']
	filename = filepath.split('/')[-1]

	try:
		# Call NameNode to get the blocklist 
		filepath_toNN = convert_path(filepath, '/' + filename)
		response = requests.delete(NAMENODE_ADDR + '/File/' + filepath_toNN + '/' + filename)
		blocklist = response.json()

		for blockID in blocklist:	
			print('--- blockID -- ' + str(blockID))
			# [address1, address2, address3]
			for address in blocklist.get(blockID):
				print('* ip -- ' + str(address))

			addrs = blocklist.get(blockID)

			# Call DataNode to delete file block
			requests.delete(addrs[0] + '/Block', json={'Block_ID': blockID, 'Address': addrs, 'Stored': [True, True, True]})

	except requests.exceptions.RequestException as err:
		print(err)

	return jsonify({'filename' : filename})


@app.route('/directory/folder/', methods=['POST'])
def create_folder():
	"""
	Create a new folder given path
	"""
	foldername = request.form['foldername']
	folderpath = request.form['folderpath']

	# Call NameNode to create a folder
	try:
		filepath_toNN = convert_path(folderpath)
		filepath_toNN += '-'
		filepath_toNN += foldername

		response = requests.post(NAMENODE_ADDR + '/Folders', json={'file_path': filepath_toNN})

	except requests.exceptions.RequestException as err:
		print(err)

	return jsonify({'newfolder' : foldername})


@app.route('/directory/folder/', methods=['DELETE'])
def delete_folder():
	"""
	Delete an existing folder given path
	"""
	folderpath = request.form['folderpath']
	foldername = folderpath.split('/')[-1]

	DN_addrs = []

	try:
		# Call NameNode to get the blocklist 
		folderpath_toNN = convert_path(folderpath)
		response = requests.delete(NAMENODE_ADDR + '/Folder/' + folderpath_toNN)
		files = response.json()

		print('------list of files------')
		print(files)

		for blocklist in files:	
			print('--- blocklist -- ' + str(blocklist))

			for blockID in blocklist:
				# [address1, address2, address3]
				for address in blocklist.get(blockID):
					print('* ip -- ' + str(address))

				addrs = blocklist.get(blockID)

				# Call DataNode to delete file block
				requests.delete(addrs[0] + '/Block', json={'Block_ID': blockID, 'Address': addrs, 'Stored': [True, True, True]})

	except requests.exceptions.RequestException as err:
		print(err)

	return jsonify({'foldername' : foldername})


### Helper Functions ###

def convert_path(filepath, filename=''):
	'''
	Convert the file path to a format required by NameNode
	'''
	return filepath.replace(filename, '').replace('/', '-')


def convert_directory(contents, path, directory):
	"""
	Converts the directory structure for UI display
	"""
	for item in contents.keys():
		path += '/' 
		path += item
		properties = contents.get(item)

		if item == '' and properties['type'] == 'folder':
			convert_directory(properties['content'], '', directory)
		elif properties['type'] == 'folder':
			directory.append({'Name' : item, 'Type' : 'folder', 'Path' : path})
			convert_directory(properties['content'], path, directory)
		elif properties['type'] == 'file':
			directory.append({'Name' : item, 'Type' : 'file', 'Path' : path})

		i = len(item) + 1
		path = path[: -i]


def split_file(data, size, numSplit=NUM_REPLICA):
	"""
	Split a file into multiple splits
	"""
	splitSize = int(size / numSplit)

	# ensure the last split has the smallest size
	if type(size / numSplit) is float:
		splitSize += 1

	splits = []

	for i in range(0, size + 1, splitSize):
		splits.append(data[i : i + splitSize])

	return splits

def joinSplit(dataList, outputFile, numSplit=NUM_REPLICA):
	"""
	Join multiple splits into one file and store locally
	"""
	newFile = open(outputFile, 'wb')	

	for data in dataList:	
		newFile.write(data)

	newFile.close()


if __name__ == '__main__':
	global directory
	directory = []

	app.run(debug=True, port=5555)












