#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  zmqpushout.py
#
#  Copyright 2016 Jelle Smet <development@smetj.net>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

import zmq.green as zmq
from wishbone.module import OutputModule
from wishbone.event import extractBulkItems
from wishbone.error import ModuleInitFailure


class ZMQPushOut(OutputModule):

    '''**Push events to one or more ZeroMQ pull sockets.**

    Expects to connect with one or more wishbone.input.push modules.  This
    module can be started in client or server mode.  In server mode, it waits
    for incoming connections.  In client mode it connects to the defined
    clients.  Events are spread in a round robin pattern over all connections.

    Parameters:

        - selection(str)("@data")
           |  The part of the event to submit externally.
           |  Use an empty string to refer to the complete event.

        - mode(str)("server")
           |  The mode to run in.  Possible options are:
           |  - server: Binds to a port and listens.
           |  - client: Connects to a port.

        - interface(string)("0.0.0.0")
           |  The interface to bind to in server <mode>.

        - port(int)(19283)
           |  The port to bind to in server <mode>.

        - clients(list)([])
           |  A list of hostname:port entries to connect to.
           |  Only valid when running in "client" <mode>.


    Queues:

        - inbox
           |  Incoming events submitted to the outside.

    '''

    def __init__(
            self,
            actor_config,
            selection="data",
            native_events=False,
            parallel_streams=1,
            mode="server",
            payload=None,
            interface="0.0.0.0",
            port=19283):
        OutputModule.__init__(self, actor_config)
        self.pool.createQueue("inbox")
        self.registerConsumer(self.consume, "inbox")

    def preHook(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUSH)
        uri = "tcp://{}:{}".format(self.kwargs.interface, self.kwargs.port)
        if self.kwargs.mode == "server":
            self.socket.bind(uri)
            self.logging.info("Listening on port %s" % (self.kwargs.port))
        elif self.kwargs.mode == 'client':
            self.socket.connect(uri)
        else:
            raise ModuleInitFailure("Invalid mode {}. Choose one of ('server', 'client')")

    def consume(self, event):
        if event.isBulk():
            data = [
                e.get(self.kwargs.selection)
                for e in extractBulkItems(event)
            ]
            self.socket.send_json(data)
        else:
            self.socket.send_json(event.get(self.kwargs.selection))
