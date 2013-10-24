#!/usr/bin/python

__author__="raltnoeder"
__date__ ="$Oct 10, 2013 9:57:03 AM$"

import sys
from drbdmanage.exceptions import *

class ConfFile(object):
    _input = None
    
    
    def __init__(self, stream):
        self._input = stream
    
    
    def get_conf(self):
        input        = self._input
        split_idx    = self._split_idx
        unescape     = self._unescape
        extend_line  = self._extend_line
        comment_line = self._comment_line
        
        key  = None
        val  = None
        conf = dict()
        
        while True:
            line = input.readline()
            if not len(line) > 0:
                break
            
            if line.endswith("\n"):
                line = line[:len(line) - 1]
            if key is None:
                # new key/val line
                # check for comment lines
                if comment_line(line):
                    continue
                idx = split_idx(line, '=')
                if idx != -1:
                    raw_key = line[:idx]
                    raw_val = line[idx + 1:]
                    key = unescape(raw_key)
                    val = unescape(raw_val)
                else:
                    # TODO: bad line, no key/val pair
                    continue
            else:
                # val continuation line
                val += unescape(line)
            if not extend_line(raw_val):
                conf[key] = val
                key = None
                val = None
        if key is not None:
            conf[key] = val
        return conf
    
    
    """
    Override values from the default configuration with values from a
    configuration loaded from a configuration file, without importing
    any keys that had not been defined in the default configuration.
    The new configuration contains only keys from the default configuration.
    """
    @classmethod
    def conf_defaults_merge(cls, conf_defaults, conf_loaded):
        conf = dict()
        for key in conf_defaults.iterkeys():
            val = conf_loaded.get(key)
            if val is not None:
                conf[key] = val
            else:
                conf[key] = conf_defaults.get(key)
        return conf
    
    
    """
    Override values from the default configuration with values from a
    configuration loaded from a configuration file, and also load new
    key/value pairs into the new configuration.
    The new configuration contains all keys from the default configuration
    plus any keys defined by the configuration loaded from the configuration
    file.
    """
    @classmethod
    def conf_defaults_union(cls, conf_defaults, conf_loaded):
        conf = dict()
        conf = self.conf_defaults_merge(conf_defaults, conf_loaded)
        for key in conf_loaded.iterkeys():
            val = conf.get(key)
            if val is None:
                conf[key] = conf_loaded.get(key)
        return conf
    
    
    def _split_idx(self, line, s_char):
        lidx = 0
        idx  = 0
        split_idx = -1
        midx = len(line) - 1
        while idx != -1:
            bidx = line.find('\\', lidx)
            sidx = line.find(s_char, lidx)
            idx = self._min_idx(bidx, sidx)
            if idx != -1:
                fchar = line[idx]
                if fchar == '\\':
                    lidx = idx + 2
                elif fchar == s_char:
                    split_idx = sidx
                    break
                if lidx > midx:
                    break
        return split_idx
    
    
    def _comment_line(self, line):
        rc = False
        idx  = 0
        midx = len(line)
        while idx < midx:
            c = line[idx]
            if not (c == ' ' or c == '\t'):
                if c == '#':
                    rc = True
                break
            idx += 1
        return rc
    
    
    def _min_idx(self, x, y):
        if x < y:
            idx = x if x != -1 else y
        else:
            idx = y if y != -1 else x
        return idx
    
    
    def _unescape(self, line):
        u_line = ""
        lidx = 0
        idx  = 0
        midx = len(line)
        # remove leading tabs and spaces
        while idx < midx:
            c = line[idx]
            if not (c == ' ' or c == '\t'):
                line = line[idx:]
                break
            idx += 1
        # replace escape sequences
        midx = len(line) - 1
        while idx != -1:
            idx = line.find('\\', lidx)
            if idx != -1:
                u_line += line[lidx:idx]
                if idx < midx:
                    cchar = line[idx + 1]
                    if cchar == 'n':
                        u_line += '\n'
                    elif cchar == 't':
                        u_line += '\t'
                    else:
                        u_line += cchar
                    lidx = idx + 2
                else:
                    # line ends with backslash, remove the backslash
                    lidx = len(line)
        # remove trailing spaces
        idx = lidx
        spaces = True
        while idx <= midx:
            c = line[idx]
            if spaces:
                if not (c == ' ' or c == '\t'):
                    spaces = False
                    u_line += line[lidx:idx]
                    lidx = idx
            else:
                if (c == ' ' or c == '\t'):
                    spaces = True
                    u_line += line[lidx:idx]
                    lidx = idx
            idx += 1
        if not spaces:
            u_line += line[lidx:]
        return u_line
    
    
    def _extend_line(self, line):
        rc   = False
        lidx = 0
        idx  = 0
        midx = len(line) - 1
        while idx != -1:
            idx = line.find("\\", lidx)
            if idx != -1:
                if idx >= midx:
                    rc = True
                    break
                else:
                    lidx = idx + 2
            if lidx > midx:
                break
        return rc


class DrbdAdmConf(object):
    def __init__(self):
        pass
    
    
    def write(self, stream, assignment, undeployed_flag):
        try:
            resource = assignment.get_resource()
            secret   = resource.get_secret()
            if secret is None:
                secret = ""
            
            # begin resource
            stream.write("resource %s {\n"
              "    net {\n"
              "        cram-hmac-alg sha1;\n"
              "        shared-secret \"%s\";\n"
              "    }\n"
              % (resource.get_name(), secret)
              )
            
            # begin resource/volumes
            for volume in resource.iterate_volumes():
                vol_state = assignment.get_volume_state(volume.get_id())
                if vol_state is None:
                    raise ValueError
                bd_path = vol_state.get_bd_path()
                if bd_path is None:
                    bd_path = ""
                minor = volume.get_minor()
                if minor is None:
                    raise InvalidMinorNrException
                stream.write("    volume %d {\n"
                  "        device /dev/drbd%d minor %d;\n"
                  "        disk %s;\n"
                  "        meta-disk internal;\n"
                  "    }\n"
                  % (volume.get_id(), minor.get_value(), minor.get_value(),
                    bd_path)
                  )
            # end resource/volumes
            
            # begin resource/nodes
            for assignment in resource.iterate_assignments():
                if ((assignment.get_tstate() & assignment.FLAG_DEPLOY != 0)
                  or undeployed_flag):
                    node = assignment.get_node()
                    stream.write("    on %s {\n"
                      "        node-id %s;\n"
                      "        address %s:%d;\n"
                      "    }\n"
                      % (node.get_name(), assignment.get_node_id(),
                        node.get_ip(), resource.get_port())
                      )
            # end resource/nodes
            
            # begin resource/connection
            stream.write("    connection-mesh {\n"
              "        hosts"
              )
            for assignment in resource.iterate_assignments():
                if ((assignment.get_tstate() & assignment.FLAG_DEPLOY != 0)
                  or undeployed_flag):
                    node = assignment.get_node()
                    stream.write(" %s" % (node.get_name()))
            stream.write(";\n")
            stream.write("        net {\n"
              "            protocol C;\n"
              "        }\n"
              )
            stream.write("    }\n")
            # end resource/connection
            
            stream.write("}\n")
            # end resource
        except InvalidMinorNrException as min_exc:
            sys.stderr.write("DEBUG: DrbdAdmConf: No minor number\n")
        except Exception as exc:
            # TODO: handle errors (stream I/O?)
            print exc
