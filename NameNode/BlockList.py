class BlockList(dict):

    def add(self, block_id, ip_addr):
        if block_id in self:
            curr_list = self.pop(block_id)
        else:
            curr_list = []
        curr_list.append(ip_addr)
        self[block_id] = curr_list

