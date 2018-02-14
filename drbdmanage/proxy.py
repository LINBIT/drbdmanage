#!/usr/bin/env python2
"""
    drbdmanage - management of distributed DRBD9 resources
    Copyright (C) 2015 - 2017 LINBIT HA-Solutions GmbH
    Author: Roland Kammerer <roland.kammerer@linbit.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import base64
import os
import socket
import SocketServer
import struct
import threading
import pickle


from drbdmanage.consts import (
    FAKE_LEADER_NAME,
    KEY_S_CMD_INIT,
    KEY_S_CMD_UPDATE,
    KEY_S_CMD_SHUTDOWN,
    KEY_S_CMD_PING,
    KEY_S_CMD_RELAY,
    KEY_S_CMD_REQCTRL,
    KEY_S_CMD_UPPOOL,
    KEY_S_INT_SHUTDOWN,
    KEY_S_ANS_OK,
    KEY_S_ANS_E_LOCKING,
    KEY_S_ANS_CHANGED,
    KEY_S_ANS_CHANGED_FAILED,
    KEY_S_ANS_UNCHANGED,
    KEY_S_ANS_E_OP_INVALID,
    KEY_S_ANS_E_TOO_LONG,
    KEY_S_ANS_E_COMM,
    KEY_SAT_CFG_TCP_KEEPIDLE,
    KEY_SAT_CFG_TCP_KEEPINTVL,
    KEY_SAT_CFG_TCP_KEEPCNT,
    KEY_SAT_CFG_TCP_SHORTTIMEOUT,
    KEY_SAT_CFG_TCP_LONGTIMEOUT,
    DEFAULT_SAT_CFG_TCP_KEEPIDLE,
    DEFAULT_SAT_CFG_TCP_KEEPINTVL,
    DEFAULT_SAT_CFG_TCP_KEEPCNT,
    DEFAULT_SAT_CFG_TCP_SHORTTIMEOUT,
    DEFAULT_SAT_CFG_TCP_LONGTIMEOUT,
)


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        blocking = self.server.blocking

        acquired = self.server.lock.acquire(blocking)

        idle = DEFAULT_SAT_CFG_TCP_KEEPIDLE
        intvl = DEFAULT_SAT_CFG_TCP_KEEPINTVL
        cnt = DEFAULT_SAT_CFG_TCP_KEEPCNT

        self.server.set_current_server_socket(self.request)
        self.server.set_peer_sockopts(self.request, 1, idle, intvl, cnt)

        opcodes = self.server.opcodes

        try:
            # do not return from following loop, use break
            while True:
                cmd = KEY_S_ANS_E_OP_INVALID
                answer_payload = ''

                opcode, length, payload = self.server.recv_msg(self.request)

                if opcode == opcodes[KEY_S_CMD_INIT]:
                    self.server.dmserver._persist.set_json_data(payload)
                    self.server.dmserver.run_config()
                    cmd = KEY_S_ANS_OK
                elif opcode == opcodes[KEY_S_CMD_UPDATE]:
                    if not self.server.dmserver._sat_lock.acquire(False):
                        cmd = KEY_S_ANS_E_LOCKING
                    else:
                        self.server.dmserver._persist.set_json_data(payload)
                        updated, failed_actions = self.server.dmserver._drbd_mgr.run(False, False, True)
                        if updated:
                            answer_payload = self.server.dmserver._persist.get_json_data()
                            if failed_actions:
                                cmd = KEY_S_ANS_CHANGED_FAILED
                            else:
                                cmd = KEY_S_ANS_CHANGED
                        else:
                            cmd = KEY_S_ANS_UNCHANGED
                        self.server.dmserver._sat_lock.release()
                elif opcode == opcodes[KEY_S_CMD_UPPOOL]:
                    self.server.dmserver._persist.set_json_data(payload)
                    self.server.dmserver._persist.load(self.server.dmserver._objects_root)
                    self.server.dmserver.update_pool_data(force=True)
                    self.server.dmserver._persist.save(self.server.dmserver._objects_root)
                    answer_payload = self.server.dmserver._persist.get_json_data()
                    cmd = KEY_S_ANS_OK
                elif opcode == opcodes[KEY_S_CMD_PING]:
                    answer_payload = payload
                    addr = self.request.getpeername()[0]
                    self.server.dmserver.set_current_leader(addr)
                    cmd = KEY_S_ANS_OK
                elif opcode == opcodes[KEY_S_CMD_RELAY]:
                    fn_rc = self.server.dmserver.add_cmd_queue(payload, via_queue=False)
                    cmd = KEY_S_ANS_OK
                    answer_payload = pickle.dumps(fn_rc)
                elif opcode == opcodes[KEY_S_CMD_REQCTRL]:
                    success = self.server.dmserver._sat_lock.acquire(False)
                    if success:
                        self.server.dmserver._persist.json_export(self.server.dmserver._objects_root)
                        answer_payload = self.server.dmserver._persist.get_json_data()
                        self.server.dmserver._sat_lock.release()
                        cmd = KEY_S_ANS_OK
                    else:
                        cmd = KEY_S_ANS_E_LOCKING
                elif opcode == opcodes[KEY_S_INT_SHUTDOWN]:
                    self.server.event_shutdown_done.set()
                    break
                elif opcode == opcodes[KEY_S_CMD_SHUTDOWN]:
                    # kill myself
                    self.server.shutdown_and_close(self.request)
                    # set current socket to None, otherwise proxy.shutdown() hangs in wait
                    self.server.set_current_server_socket(None)
                    self.server.dmserver.schedule_shutdown()
                    break
                elif opcode == opcodes[KEY_S_ANS_E_OP_INVALID]:
                    cmd = KEY_S_ANS_E_OP_INVALID
                elif opcode == opcodes[KEY_S_ANS_E_COMM]:  # recv_msg failed
                    # break out of handler, which automatically starts a new server thread
                    break
                else:
                    cmd = KEY_S_ANS_E_OP_INVALID

                if opcode == opcodes[KEY_S_CMD_INIT] or opcode == opcodes[KEY_S_CMD_UPDATE]:
                    # set sockopts if changed
                    conf = self.server.dmserver._conf
                    idle_old, intvl_old, cnt_old = idle, intvl, cnt

                    idle = int(conf.get(KEY_SAT_CFG_TCP_KEEPIDLE, DEFAULT_SAT_CFG_TCP_KEEPIDLE))
                    intvl = int(conf.get(KEY_SAT_CFG_TCP_KEEPINTVL, DEFAULT_SAT_CFG_TCP_KEEPINTVL))
                    cnt = int(conf.get(KEY_SAT_CFG_TCP_KEEPCNT, DEFAULT_SAT_CFG_TCP_KEEPCNT))

                    if idle != idle_old or intvl != intvl_old or cnt != cnt_old:
                        self.server.set_peer_sockopts(self.request, 1, idle, intvl, cnt)

                # send back and handle error
                opcode, length, payload = self.server.send_msg(self.request,
                                                               self.server.encode_msg(cmd, answer_payload))
                if opcode == opcodes[KEY_S_ANS_E_COMM] or not blocking:
                    # break out of handler, which automatically starts a new server thread
                    break
        except Exception:
            pass
        finally:
            self.server.set_current_server_socket(None)
            if acquired:
                self.server.lock.release()


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    # traditional server, manually started or by dbus activation
    pass


class TCPServerSystemdSockActivation(SocketServer.TCPServer):
    def __init__(self, server_address, handler_class, bind_and_activate=True):
        # we init the SocketServer.TCPServer always with bind_and_activate False
        SocketServer.TCPServer.__init__(self, server_address, handler_class,
                                        bind_and_activate=False)


class ThreadedTCPServerSystemdSockActivation(SocketServer.ThreadingMixIn, TCPServerSystemdSockActivation):
    # server started by systemd socket activation
    def __init__(self, server_address, handler_class, bind_and_activate=True):
        SYSTEMD_FIRST_SOCKET_FD = 3
        socket_type = SocketServer.TCPServer.socket_type
        address_family = SocketServer.TCPServer.address_family

        TCPServerSystemdSockActivation.__init__(self, server_address, handler_class, bind_and_activate)

        self.socket = socket.fromfd(SYSTEMD_FIRST_SOCKET_FD, address_family, socket_type)

        # rest of TCPServer.__init__ (if bind_and_activate) is in our case always False
        # no further action required


class DrbdManageProxy(object):
    # default DRBD ports 7000 - 7999
    # default DRBD control volume port 6999
    # this is the next lower one easy to remember
    _DEFAULT_PORT_NR = 6996

    # protcol:
    # | opcode | len | payload |
    # opcode: 2 byte
    # len: 4 byte, length of payload in bytes
    # payload: variable length (len bytes), always bas64 encoded, can be encrypted

    OP_LEN = 2
    LEN_LEN = 4

    # opcodes sent over the network are positive 2 bytes unsigned values
    # negative values are used for internal signaling

    opcodes = {
        KEY_S_CMD_INIT: 11,
        KEY_S_CMD_UPDATE: 12,
        KEY_S_CMD_SHUTDOWN: 13,
        KEY_S_CMD_PING: 14,
        KEY_S_CMD_RELAY: 15,
        KEY_S_CMD_REQCTRL: 16,
        KEY_S_CMD_UPPOOL: 17,
        KEY_S_INT_SHUTDOWN: 21,
        KEY_S_ANS_OK: 31,
        KEY_S_ANS_E_OP_INVALID: 32,
        KEY_S_ANS_E_TOO_LONG: 33,
        KEY_S_ANS_E_COMM: 34,
        KEY_S_ANS_CHANGED: 35,
        KEY_S_ANS_UNCHANGED: 36,
        KEY_S_ANS_CHANGED_FAILED: 37,
    }

    def __init__(self, dmserver, host='', port=_DEFAULT_PORT_NR, blocking=True):
        self._tcp_server = None
        self._dmserver = dmserver
        self._host = host
        self._port = port
        self._peersockets = {}
        self._current_server_socket = None
        self._blocking = blocking
        # currently we depend on blocking behavior
        # which is perfectly fine in our 1:1 mapping
        # probably we remove this choice in the future
        self._blocking = True
        self.lock = threading.Lock()
        self.event_shutdown_init = threading.Event()
        self.event_shutdown_done = threading.Event()

    def start(self):
        if self._tcp_server:
            for s in self._peersockets.values():
                self._shutdown_and_close(s)
            return

        # server type selection
        if os.environ.get('LISTEN_PID', None) == str(os.getpid()):
            ThreadedTCPServerSystemdSockActivation.allow_reuse_address = True
            ThreadedTCPServerSystemdSockActivation.daemon_threads = True
            self._tcp_server = ThreadedTCPServerSystemdSockActivation((self._host, self._port),
                                                                      ThreadedTCPRequestHandler)
        else:
            ThreadedTCPServer.allow_reuse_address = True
            ThreadedTCPServer.daemon_threads = True
            self._tcp_server = ThreadedTCPServer((self._host, self._port), ThreadedTCPRequestHandler)

        # forward some refs to the TCPServer
        # while we could add a ref to 'self', I like to keep it selective
        # TCP handler should be able to call dmserver functions
        self._tcp_server.dmserver = self._dmserver

        # TCP handler should be able to synchronize requests
        self._tcp_server.lock = self.lock

        # Handle server shutdown
        # self._tcp_server.event_shutdown_init = self.event_shutdown_init
        self._tcp_server.event_shutdown_done = self.event_shutdown_done

        self._tcp_server.blocking = self._blocking
        self._tcp_server.opcodes = self.opcodes

        self._tcp_server.encode_msg = self._encode_msg
        self._tcp_server.decode_msg = self._decode_msg
        self._tcp_server.recv_msg = self.recv_msg
        self._tcp_server.send_msg = self.send_msg

        self._tcp_server.set_current_server_socket = self.set_current_server_socket
        self._tcp_server.set_peer_sockopts = self.set_peer_sockopts
        self._tcp_server.set_want_shutdown = self.set_want_shutdown
        self._tcp_server.shutdown_and_close = self._shutdown_and_close

        tcp_server_thread = threading.Thread(target=self._tcp_server.serve_forever)
        tcp_server_thread.daemon = True
        tcp_server_thread.start()

    def _shutdown_and_close(self, sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
        except Exception:
            pass

    def shutdown(self):
        if self._tcp_server:
            self.event_shutdown_init.clear()
            self.event_shutdown_done.clear()

        if self._current_server_socket:
            self.event_shutdown_init.set()
            # close the socket, which triggers the server to shutdown
            self._shutdown_and_close(self._current_server_socket)
            self.event_shutdown_done.wait()

        if self._tcp_server:
            self._tcp_server.shutdown()
            self._tcp_server.server_close()

    def set_current_server_socket(self, sock):
        self._current_server_socket = sock

    def set_peer_sockopts(self, sock, alive, idle, intvl, cnt):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, alive)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, idle)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, intvl)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, cnt)

    def set_want_shutdown(self, want):
        self._want_shutdown = True if want else False

    # sets _l_ocking to blocking/non-blocking
    # returns new state
    def set_locking(self, blocking):
        # always set this value, even if server not running
        self._blocking = blocking
        if self._tcp_server:
            self._tcp_server.blocking = blocking
        return self._blocking

    def get_blocking(self, blocking):
        return self._blocking

    def send_msg(self, sock, data):
        header = self.OP_LEN + self.LEN_LEN
        try:
            # unfortunately python's send/sendall has a bug when executed on a
            # non-connected/shutdowned/closed socket.
            # first send succeeds and returns all bytes sent
            # only the subsequent sends then fail with an exception
            # http://bugs.python.org/issue10644
            # our sends always follows out protocol (header + payload), therefore
            # the easy solution is to split one send into two.
            sock.sendall(data[:header])
            sock.sendall(data[header:])
        except Exception:
            return self.opcodes[KEY_S_ANS_E_COMM], 0, ''

        return self.opcodes[KEY_S_ANS_OK], 0, ''

    def recv_msg(self, sock):
        # recv does _not_ set/close sockets to invalid, it propagates the error
        # the higher level caller is responsible to take care of it
        # get fixed length header
        bytes_recvd = 0
        chunks = []
        header = self.OP_LEN + self.LEN_LEN
        while bytes_recvd < header:
            try:
                chunk = sock.recv(header - bytes_recvd)
            except Exception:
                return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
            if chunk == '':
                if self.event_shutdown_init.is_set():
                    return self.opcodes[KEY_S_INT_SHUTDOWN], 0, ''
                else:
                    return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
            chunks.append(chunk)
            bytes_recvd += len(chunk)

        data = bytearray(''.join(chunks))

        opcode = struct.unpack("!H", str(data[0:self.OP_LEN]))
        length = struct.unpack("!I", str(data[self.OP_LEN:self.OP_LEN + self.LEN_LEN]))
        opcode = opcode[0]
        length = length[0]

        if opcode > max(self.opcodes.values()):
            return self.opcodes[KEY_S_ANS_E_OP_INVALID], 0, ''

        # get variable length payload
        bytes_recvd = 0
        chunks = []
        while bytes_recvd < length:
            try:
                chunk = sock.recv(min(length - bytes_recvd, 4096))
            except Exception:
                return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
            if chunk == '':
                if self.event_shutdown_init.is_set():
                    return self.opcodes[KEY_S_INT_SHUTDOWN], 0, ''
                else:
                    return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
            chunks.append(chunk)
            bytes_recvd += len(chunk)

        payload = ''.join(chunks)
        payload = self._decode_msg(payload)

        return opcode, length, payload

    def send_recv_msg(self, sock, data):
        opcode, length, payload = self.send_msg(sock, data)
        if opcode != self.opcodes[KEY_S_ANS_E_COMM]:
            opcode, length, payload = self.recv_msg(sock)

        return opcode, length, payload

    def get_established(self):
        return self._peersockets.keys()

    def set_sockettimeout(self, s, t):
        try:
            s.settimeout(t)
        except:
            pass

    # This is the function that should be used by control nodes to send commands to satellites
    # and by satellites to request the ctrlvol, forward requests, ...
    # semantics: if there isn't an established connection, connect to the peer (which is the TCP server)
    # if the communiction fails (E_COMM) reset the client socket and the function tries to establish
    # it on the next call.
    # important, the E_COMM is returned to the caller, it is up to the caller if he immediately tries
    # to resend if the first attempt failed.
    def send_cmd(self, peer_name, cmd, port=_DEFAULT_PORT_NR, override_data='', override_ip=''):
        payload = override_data

        conf = self._dmserver._conf
        short_timeout = float(conf.get(KEY_SAT_CFG_TCP_SHORTTIMEOUT, DEFAULT_SAT_CFG_TCP_SHORTTIMEOUT))
        long_timeout = float(conf.get(KEY_SAT_CFG_TCP_LONGTIMEOUT, DEFAULT_SAT_CFG_TCP_LONGTIMEOUT))

        if peer_name not in self._peersockets:
            if peer_name == FAKE_LEADER_NAME:
                if override_ip:
                    peer_ip = override_ip
                else:
                    return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
            else:
                peer_node = self._dmserver.get_node(peer_name)
                if peer_node is None:
                    return self.opcodes[KEY_S_ANS_E_COMM], 0, ''
                peer_ip = peer_node.get_addr()

            try:
                sock = socket.create_connection((peer_ip, port), timeout=short_timeout)
            except Exception:
                return self.opcodes[KEY_S_ANS_E_COMM], 0, ''

            idle = int(conf.get(KEY_SAT_CFG_TCP_KEEPIDLE, DEFAULT_SAT_CFG_TCP_KEEPIDLE))
            intvl = int(conf.get(KEY_SAT_CFG_TCP_KEEPINTVL, DEFAULT_SAT_CFG_TCP_KEEPINTVL))
            cnt = int(conf.get(KEY_SAT_CFG_TCP_KEEPCNT, DEFAULT_SAT_CFG_TCP_KEEPCNT))
            self.set_peer_sockopts(sock, 1, idle, intvl, cnt)

            self._peersockets[peer_name] = sock

        needs_json_data = cmd == KEY_S_CMD_INIT or cmd == KEY_S_CMD_UPDATE or cmd == KEY_S_CMD_UPPOOL
        needs_long_delay = needs_json_data or cmd == KEY_S_CMD_RELAY

        if needs_long_delay:
            self.set_sockettimeout(self._peersockets[peer_name], long_timeout)

        if needs_json_data:
            # self._dmserver._persist.json_export(self._dmserver._objects_root)
            # ^^ done on call site, because needs to be done only once per "transaction"
            payload = self._dmserver._persist.get_json_data()

        data = self._encode_msg(cmd, payload)

        if cmd == KEY_S_CMD_SHUTDOWN:
            # send cmd, but don't expect to get anything back...
            self.send_msg(self._peersockets[peer_name], data)
            self._shutdown_and_close(self._peersockets[peer_name])
            del self._peersockets[peer_name]
            return self.opcodes[KEY_S_ANS_OK], 0, ''

        opcode, length, payload = self.send_recv_msg(self._peersockets[peer_name], data)
        self.set_sockettimeout(self._peersockets[peer_name], short_timeout)

        # cleanup if communication failed.
        if opcode == self.opcodes[KEY_S_ANS_E_COMM]:
            self._shutdown_and_close(self._peersockets[peer_name])
            self._peersockets.pop(peer_name, None)

        return opcode, length, payload

    def shutdown_connection(self, satellite_name):
        if satellite_name in self._peersockets:
            self._shutdown_and_close(self._peersockets[satellite_name])
            self._peersockets.pop(satellite_name, None)
        return KEY_S_ANS_OK, 0, ''

    # used to encode/encrypt
    def _encode_msg(self, cmd, payload):
        payload = payload.encode('bz2')
        payload = base64.b64encode(payload)
        payload = bytearray(payload)
        opcode = bytearray(struct.pack("!H", self.opcodes[cmd]))
        length = len(payload)
        length = bytearray(struct.pack("!I", length))

        return opcode + length + payload

    def _decode_msg(self, data):
        data = base64.b64decode(data)
        data = data.decode('bz2')
        return data
