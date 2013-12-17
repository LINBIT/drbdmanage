#!/usr/bin/python

import drbdmanage.drbd.persistence
import drbdmanage.utils
import sys

__author__="raltnoeder"
__date__ ="$Dec 12, 2013 4:51:07 PM$"

init_blks = 4
blksz     = drbdmanage.drbd.persistence.PersistenceImpl.BLKSZ


index_name = drbdmanage.drbd.persistence.PersistenceImpl.IDX_NAME
index_off  = drbdmanage.drbd.persistence.PersistenceImpl.IDX_OFFSET
hash_name  = drbdmanage.drbd.persistence.PersistenceImpl.HASH_NAME
hash_off   = drbdmanage.drbd.persistence.PersistenceImpl.HASH_OFFSET
data_off   = drbdmanage.drbd.persistence.PersistenceImpl.DATA_OFFSET

assg_len_name  = drbdmanage.drbd.persistence.PersistenceImpl.ASSG_LEN_NAME
assg_off_name  = drbdmanage.drbd.persistence.PersistenceImpl.ASSG_OFF_NAME
nodes_len_name = drbdmanage.drbd.persistence.PersistenceImpl.NODES_LEN_NAME
nodes_off_name = drbdmanage.drbd.persistence.PersistenceImpl.NODES_OFF_NAME
res_len_name   = drbdmanage.drbd.persistence.PersistenceImpl.RES_LEN_NAME
res_off_name   = drbdmanage.drbd.persistence.PersistenceImpl.RES_OFF_NAME

def main():
    global index_name
    global index_off
    global hash_name
    global hash_off
    global data_off
    global assg_len_name
    global assg_off_name
    global nodes_len_name
    global nodes_off_name
    global res_len_name
    global res_off_name
    
    myname = "drbdctrl_init"
    
    if len(sys.argv) >= 1:
        myname = sys.argv[0]
    
    if len(sys.argv) >= 2:
        drbdctrl = None
        try:
            hash = drbdmanage.utils.DataHash()
            
            index_str = (
                    "{\n"
                    "    \"" + index_name + "\": {\n"
                    "        \"" + assg_len_name + "\": 3,\n"
                    "        \"" + assg_off_name + "\": " 
                    + str(data_off) + ",\n"
                    "        \"" + nodes_len_name + "\": 3,\n"
                    "        \"" + nodes_off_name + "\": "
                    + str(data_off) + ",\n"
                    "        \"" + res_len_name + "\": 3,\n"
                    "        \"" + res_off_name + "\": "
                    + str(data_off) + "\n"
                    "    }\n"
                    "}\n"
            )
            data_str = "{}\n"
            
            pos = 0
            while pos < 3:
                hash.update(data_str)
                pos += 1
            
            drbdctrl = open(sys.argv[1], "rb+")
            zeroblk  = bytearray('\0' * blksz)
            pos      = 0
            while pos < init_blks:
                drbdctrl.write(zeroblk)
                pos += 1
            drbdctrl.seek(index_off)
            drbdctrl.write(index_str)
            drbdctrl.seek(data_off)
            drbdctrl.write(data_str)
            drbdctrl.seek(hash_off)
            drbdctrl.write(
                "{\n"
                "    \"hash\": \"" + hash.get_hex_hash() + "\"\n"
                "}\n"
            )
        except IOError as ioexc:
            sys.stderr.write("Initialization failed: " + str(ioexc) + "\n")
        finally:
            if drbdctrl is not None:
                try:
                    drbdctrl.close()
                except IOError:
                    pass
        sys.stdout.write("empty drbdmanage control volume initialized.\n")
    else:
        sys.stderr.write("Syntax: " + myname + " <device>\n")

if __name__ == "__main__":
    main()
