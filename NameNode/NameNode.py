from heapq import *
from flask import json
import requests
import threading
import time
import copy
from BlockList import *
import math

class NameNode(object):
    DEBUG = False

    data_nodes = []
    missing_nodes = dict()
    file_dictionary = dict()
    block_report_results = dict() # block_report_resutls format is {block_id: [node_ip, node_ip, node_ip]}
    CONST_BLOCK_SIZE = 128000000.0 #size in Bytes
    
    CONST_HEARTBEAT_TIMER = 30
    CONST_REPLICATION_FACTOR = 3
    CONST_MIN_DATANODE_COUNT = 1
    CONST_AWS_IP_REQUEST = 'http://checkip.amazonaws.com'
    CONST_TARGET_PORT = ':5000'
    test_id = 0
    data_node_count = 0
    my_ip = 'localhost'
    hash_seed = 1


    def __init__(self):
        if self.DEBUG:
            self.CONST_BLOCK_SIZE = 120.0 #size in Bytes

        response = requests.get(self.CONST_AWS_IP_REQUEST)
        self.my_ip = "http://" + response.text.strip() + self.CONST_TARGET_PORT
        t = threading.Thread(name='loop_heartbeat_inventory', target=self.loop_heartbeat_inventory)
        t.start()
        return


    def loop_heartbeat_inventory(self):
        inventory_url = self.my_ip + '/ForceHeartbeatInventory'
        if self.DEBUG:
            inventory_url = "http://localhost:5000/ForceHeartbeatInventory"
        while True:
            time.sleep(self.CONST_HEARTBEAT_TIMER)
            requests.get(inventory_url)


    # 1 - trigger replication of any underrepresented nodes
    # 2 - check if any valid IPs in the heap are also in the list of missing nodes, if missing invalidate that heap entry
    # 3 - add any nodes that are still present to the list of potential misses for next round (removed by dn heartbeat)
    # 4 - clear the contents of last rounds block reports
    def heartbeat_inventory(self):
            self.update_blocks_and_replicate(self.file_dictionary)
            next_missing_list = []
            for i, node in enumerate(self.data_nodes):
                node_num_blocks_saved = node[0]
                node_ip = node[1]
                node_valid = node[2]
                if node_valid:
                    if node_ip in self.missing_nodes:
                        self.data_nodes[i] = (node_num_blocks_saved, node_ip, False)
                        self.data_node_count = self.data_node_count-1 
                    else:
                        next_missing_list.append(node_ip)
            self.block_report_results.clear()
            self.missing_nodes = next_missing_list
    

    # update directory's block lists with the results of the the block report
    # if any block is underrepresented send replication request
    # work recursively on sub-folders
    def update_blocks_and_replicate(self, directory):
        for key in directory.keys():
            next = directory[key]
            if isinstance(next, BlockList):
                file = next
                # file format is dict<K,V>, with <K=block_id, v=IP[]>
                for block_id in file:
                    # block_report_results format is dict<K,V>, with <K=block_id, v=IP[]>
                    if block_id in self.block_report_results:
                        updated_list = self.block_report_results[block_id]
                    else: 
                        updated_list = []
                    file[block_id] = updated_list
                    if len(updated_list) < self.CONST_REPLICATION_FACTOR and self.data_node_count > 1:
                        self.replicate_block(block_id, updated_list)
                    
            elif isinstance(next, dict):
                next_dir = next
                self.update_blocks_and_replicate(next_dir)


    def replicate_block(self, block_id, target_ips):
        rep_count = len(target_ips)
        replicated_array = []
        redundant = []

        desired_replications = self.CONST_REPLICATION_FACTOR
        if self.data_node_count < self.CONST_REPLICATION_FACTOR:
            desired_replications = self.data_node_count

        while rep_count > 0:
            replicated_array.append(True)
            rep_count = rep_count-1
        while len(target_ips) < desired_replications:
            node = heappop(self.data_nodes)
            node_num_blocks_saved = node[0]
            node_ip = node[1]
            node_valid = node[2]
            redundant = []
            if node_valid and (node_ip not in target_ips):
                if self.DEBUG: 
                    print("--------Start Add Report-----------")
                    print("block id: " + str(block_id))
                    print("dups needed to fill: " + str(len(target_ips)))
                    print("valid is " + str(node_valid))
                    print("node ip is " + str(node_ip))
                    print("is ip in target_ips? " + str(node_ip in target_ips))
                    print(str(target_ips))
                    print("--------End Add Report----------- \n \n")

                updated_num_blocks_saved = node_num_blocks_saved + 1
                heappush(self.data_nodes,(updated_num_blocks_saved,node_ip, True))
                target_ips.append(node_ip)
                replicated_array.append(False)
            elif node_valid:
                redundant.append(node)
        for skipped in redundant:
            heappush(self.data_nodes, skipped)

        if desired_replications > 0:
            first_ip = target_ips[0]
            url = first_ip + "/DuplicateBlock"
            replication_data = {"Block_ID":block_id, "Stored":replicated_array, "Address":target_ips}

            if self.DEBUG:
                print("--------Sending Duplicates -----------")
                print(replication_data)
                print("--------End send duplicates----------- \n \n")
            try:
                r = requests.put(url, json=replication_data)
            except:
                if self.DEBUG:
                    print("Node unable to duplicate")
                pass
            


    def has_min_num_datanodes(self):
        return self.data_node_count >= self.CONST_MIN_DATANODE_COUNT


    def register_dn(self, dn_ip, dn_blocks_saved):
        #treat space size as negative to use minqueue
        heappush(self.data_nodes,(dn_blocks_saved, dn_ip, True))
        self.data_node_count = self.data_node_count+1
        return


    def is_registered_data_node(self, target_ip):
        for node in self.data_nodes:
            num_blocks_saved = node[0]
            node_ip = node[1]
            valid_flag = node[2]
            if node_ip == target_ip and valid_flag == True:
                return True
        return False


    def is_valid_block_report(self, dn_block_report):
        return True


    #remove ip from missing list
    #and add the ip adress to the map of <block_id : [ips]>
    def consume_block_report(self, sender_ip, dn_block_list):
        if sender_ip in self.missing_nodes:
            self.missing_nodes.remove(sender_ip)
        #dn_block_list format is [block_id_0, block_id_1, block_id_2]
        for block_id in dn_block_list:
            if block_id in self.block_report_results.keys():
                stored_list = self.block_report_results.pop(block_id)
            else:
                stored_list = []
            if not sender_ip in stored_list:
                stored_list.append(sender_ip)
            # block_report_results format is {block_id: [node_ip, node_ip, node_ip]}
            self.block_report_results[block_id] = stored_list


    def update_dn_space(self, dn_ip, dn_blocks_saved):
        for i, node in enumerate(self.data_nodes):
            orig_blocks_saved = node[0]
            node_ip = node[1]
            node_valid = node[2]
            if node_ip == dn_ip and orig_blocks_saved != dn_blocks_saved and node_valid:
                self.data_nodes[i] = (orig_blocks_saved, node_ip, False)
                heappush(self.data_nodes,(dn_blocks_saved, node_ip, True))
                return

    
    def gen_block_id(self, file_name, file_path = None):
        self.test_id
        if file_path is not None:
            self.test_id = str(file_path)
        self.test_id = self.test_id + file_name + "blocknum" + str(self.hash_seed)
        self.hash_seed = self.hash_seed + 1
        return self.test_id


    def file_exists(self, file_name, file_path = None):
        if file_path is None or file_path == "":
            tgt_dir = self.file_dictionary
        else:
            tgt_dir = self.get_directory(file_path)
        if self.DEBUG:
            print("checking if " + str(file_name) + " exists")
            print("checking name in dict yields " + str(file_name in tgt_dir.keys()))
        return file_name in tgt_dir.keys()


    def add_file(self, file_name, file_size, file_path = None):
            if self.DEBUG:
                print("received file size is: " + str(file_size))
            block_list = BlockList()
            block_count = int(math.ceil(file_size/self.CONST_BLOCK_SIZE))
            if self.DEBUG:
                print("blocks to save: " + str(block_count))
            block_num = block_count
            while block_num > 0:
                block_id = self.gen_block_id(file_name,file_path)
                if self.DEBUG:
                    print("generating block " + str(block_num) + " info")
                    print("block id is " + str(block_num) + " info")
                targeted = []
                redundant = []
                dups_needed = self.CONST_REPLICATION_FACTOR
                if self.data_node_count < dups_needed:
                    dups_needed = self.data_node_count
                while dups_needed > 0:
                    node = heappop(self.data_nodes)
                    node_num_blocks_saved = node[0]
                    node_ip = node[1]
                    node_valid = node[2]
                    if node_valid and (node_ip not in targeted):
                        if self.DEBUG:
                            print("--------Start Add Report-----------")
                            print("block id: " + str(block_id))
                            print("blocks remaining to fill: " + str(block_num))
                            print("dups needed to fill: " + str(dups_needed))
                            print("valid is " + str(node_valid))
                            print("node ip is " + str(node_ip))
                            print("is ip in targeted? " + str(node_ip in targeted))
                            print(str(targeted))
                            print("--------End Add Report----------- \n \n")

                        targeted.append(node_ip)
                        block_list.add(block_id,node_ip)
                        if self.DEBUG:
                            print("block list now contains: ")
                            print(block_list)
                        updated_num_blocks_saved = node_num_blocks_saved + 1
                        heappush(self.data_nodes,(updated_num_blocks_saved,node_ip, True))
                        dups_needed = dups_needed-1
                    elif node_valid:
                        redundant.append(node)
                for skipped in redundant:
                    heap.push(self.data_nodes, skipped)
                block_num = block_num-1
            if self.path_exists(file_path):
                tgt_directory = self.get_directory(file_path)
            else:
                tgt_directory = self.create_directory(file_path)
            tgt_directory[file_name] = block_list
            ret = self.get_block_list(file_name, file_path)
            if self.DEBUG:
                print("--------Sending client blocklist -----------")
                print(str(ret))
                print("--------End send blocklist----------- \n \n")
            return ret


    def get_block_list(self, file_name, file_path = None):
        tgt_dir = self.get_directory(file_path)
        if tgt_dir: 
            return tgt_dir.get(file_name)


    def delete_file(self, file_name, file_path = None):
        block_list = None
        if self.file_exists(file_name, file_path):
            tgt_dir = self.get_directory(file_path)
            block_list = tgt_dir.pop(file_name)
        return block_list


    def cleanse_file_path(self,file_path):
        return file_path.lstrip("-")


    def get_directory_by_path_list(self, file_path_list = None):
        if ((file_path_list is None or len(file_path_list)) == 0 or (len(file_path_list) == 1 and file_path_list[0] == "")):
            return self.file_dictionary
        prev_dir = self.file_dictionary
        for next_dir_name in file_path_list:
            if(next_dir_name in prev_dir):
                prev_dir = prev_dir.get(next_dir_name)
            else:
                return dict()
        curr_dir = prev_dir
        return curr_dir


    def get_directory(self, file_path_string = None):
        if file_path_string is None or file_path_string == "":
            return self.file_dictionary
        if not self.is_valid_path(file_path_string):
            return dict()
        file_path_array = self.parse_path(file_path_string)
        prev_dir = self.file_dictionary
        for next_dir_name in file_path_array:
            if(next_dir_name in prev_dir):
                prev_dir = prev_dir.get(next_dir_name)
            else:
                return dict()
        curr_dir = prev_dir
        return curr_dir


    def get_directory_for_client(self, file_path_string = None):
        if file_path_string is None:
            return self.assign_types_to_content(self.file_dictionary)
        if not self.is_valid_path(file_path_string):
            return self.assign_types_to_content(dict())
        file_path_array = self.parse_path(file_path_string)
        prev_dir = self.file_dictionary
        for next_dir_name in file_path_array:
            if(next_dir_name in prev_dir):
                prev_dir = prev_dir.get(next_dir_name)
            else:
                return self.assign_types_to_content(dict())
        curr_dir = prev_dir
        return self.assign_types_to_content(curr_dir)


    def assign_types_to_content(self, input_content):
        content = copy.deepcopy(input_content)
        for key in content.keys():
            next = content[key]
            if isinstance(next, BlockList):
                file = next
                outer_dir = dict()
                outer_dir["type"] = "file"
                outer_dir["content"] = file
                content[key] = outer_dir
            elif isinstance(next, dict):
                outer_dir = dict()
                outer_dir["type"] = "folder"
                outer_dir["content"] = self.assign_types_to_content(next)
                content[key] = outer_dir
                next_dir = next
        return content


    def delete_folder(self, file_path):
        if self.DEBUG:
            print(file_path)
        path_and_folder = self.seperate_path_and_folder(file_path)
        target_dir = self.get_directory(file_path)
        del_blocks = self.delete_folder_contents(target_dir)
        target_folder_name = path_and_folder[0]
        adjusted_path_list = path_and_folder[1]
        parent_dir = self.get_directory_by_path_list(adjusted_path_list)
        parent_dir.pop(target_folder_name, None)
        return del_blocks


    def delete_folder_contents(self, target_dir):
        list_of_blocks = []
        for curr in target_dir:
            curr = target_dir[curr]
            if isinstance(curr, BlockList):
                list_of_blocks.append(curr)
            else:
                addl_list = self.delete_folder_contents(curr)
                for item in addl_list:
                    list_of_blocks.append(item)
        return list_of_blocks


    def path_exists(self, file_path_string = None):
        if file_path_string is None or file_path_string == "":
            return True
        if not self.is_valid_path(file_path_string):
            return False
        file_path_array = self.parse_path(file_path_string)
        prev_dir = self.file_dictionary
        for next_dir_name in file_path_array:
            if self.DEBUG:
                print("checking if directory named " + str(next_dir_name) + "is in " + str(prev_dir))
            if(next_dir_name in prev_dir):
                prev_dir = prev_dir.get(next_dir_name)
            else:
                if self.DEBUG:
                    print("Directory named " + str(next_dir_name) + " was not in " + str(prev_dir))
                return False
        return True


    def create_directory(self, file_path_string = None):
        file_path_array = self.parse_path(file_path_string)
        prev_dir = self.file_dictionary
        final_dir = None
        for next_dir_name in file_path_array:
            if(next_dir_name in prev_dir):
                prev_dir = prev_dir.get(next_dir_name)
            else:
                prev_dir[next_dir_name] = dict()
                prev_dir =  prev_dir[next_dir_name]
        final_dir = prev_dir
        return final_dir


    def is_valid_path(self, file_path_string):
        return True


    def seperate_path_and_folder(self, file_path_string):
        if self.DEBUG:
            print("path is" + file_path_string)
        parse_char = "-"
        path_list = file_path_string.split(parse_char)
        tgt_folder = path_list.pop()
        return (tgt_folder,path_list)


    def parse_path(self, file_path_string):
        parse_char = "-"
        return file_path_string.split(parse_char)

    
    def reset(self):
        self.data_nodes = []
        self.missing_nodes = dict()
        self.file_dictionary = dict()
        self.block_report_results = dict() # block_report_resutls format is {block_id: [node_ip, node_ip, node_ip]}
        self.test_id = 0
        self.data_node_count = 0
        self.hash_seed = 1