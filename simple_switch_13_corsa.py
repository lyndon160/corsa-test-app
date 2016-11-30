# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# version for corsa swithc (DP2100)
# corsa requires modification of the original design for the following reasons
#
# the Corsa pipeline does not support logical ports which allow packet replication, for e.g. multicast/broadcast
# additionally the Corsa pipeline does not support group actions in conjunction with PACKET-OUT operations
#
# therefore a multicast or broadcast capable switch may need to utilise
# group actions containing multiple port out actions for dataplane forwarding
# and additionally utilise packet out command with multiple port out actions and/or multiple packet out operations
#
import time
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, HANDSHAKE_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types


class SimpleSwitch13(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]
    LEARN_GROUP_ID = 0
    FLOOD_GROUP_ID = 1

    def __init__(self, *args, **kwargs):
        super(SimpleSwitch13, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.port_table = set([])

    def del_all_flows(self, datapath):
        '''Delete all flows on switch'''
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        print "Deleting all flows on table 0"
        match = parser.OFPMatch()
        instructions = []
        flow_mod = datapath.ofproto_parser.OFPFlowMod(datapath, 0, 0, 0, ofproto.OFPFC_DELETE, 0, 0, 1, ofproto.OFPCML_NO_BUFFER, ofproto.OFPP_ANY, ofproto.OFPP_ANY, 0, match, instructions)
        datapath.send_msg(flow_mod)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.del_all_flows(datapath)

        # create a table miss entry for discovery of unhandled traffic
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        # lowest possible priority rule, = table miss
        self.add_flow(datapath, 0, match, actions)

        # create a group to perform flood operations
        # delete any pre-existing group of this type/index
        datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_DELETE, ofproto.OFPGT_ALL, self.FLOOD_GROUP_ID))
        # before creating it
        # initially the group is empty, but later we will add some buckets.....
        datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD, ofproto.OFPGT_ALL, self.FLOOD_GROUP_ID, []))
	'''print "Starting group creation"
	for x in range(100000):
        	datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_DELETE, ofproto.OFPGT_ALL, x))
        	datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD, ofproto.OFPGT_ALL, x, []))
		time.sleep(0.01)
		print "Group: " + str(x) + " created"'''
        # the following group also performs flood operations and additionally forwards a copy of the received broadcast message to the controller
        ###bucket_list = [ parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,ofproto.OFPCML_NO_BUFFER)])]
        print "starting bucket list"
        #bucket_list = [ parser.OFPActionGroup(self.FLOOD_GROUP_ID)]
        bucket_list = []
        datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_DELETE, ofproto.OFPGT_ALL, self.LEARN_GROUP_ID))
        bucket_list.append ( parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(17)]))
        datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_ADD,    ofproto.OFPGT_ALL, self.LEARN_GROUP_ID, bucket_list))
        for j in range(1000):
            bucket_list = []
	    time.sleep(1)
            for i in range(1000+j):
                bucket_list.append ( parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(17)]))
        #bucket_list.append ( parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(17)]))
            print "current bucket value: i="+str(i)+" j="+str(j)
            datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_MODIFY,    ofproto.OFPGT_ALL, self.LEARN_GROUP_ID, bucket_list))
            time.sleep(1)
            print "current bucket value: i="+str(i)+" j="+str(j)
        # create another 'table miss' entry for discovery of unhandled broadcast traffic
        # add a rule which uses the previously defined learn+flood group to broadcast messages to all ports and also forwards to controller
        # this allows the controller to learn host source addresses and thus install rules when it can....
	#time.sleep(5)
        match = parser.OFPMatch(eth_dst='ff:ff:ff:ff:ff:ff')
        actions = [parser.OFPActionGroup(self.LEARN_GROUP_ID)]
        # higher priority (1>0) rule to ensure that it fires for all broadcast traffic rather than the general case table miss rule
        ###self.add_flow(datapath, 1, match, actions)
        # install a rule to ignore LLDP - this is a high priority rule - we really don't want to forward this
        self.add_flow(datapath, 100, parser.OFPMatch(eth_type=ether_types.ETH_TYPE_LLDP), [])



    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, [HANDSHAKE_DISPATCHER, CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def port_desc_stats_reply_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # build a port table for use in constructiong flood group actions and packet_out action lists
        # Note: in _this_ version the port list is not updated once created, however a more robust implmentation would listed for port status events and react accordingly
        for p in ev.msg.body:
            if p.port_no > 0xffffff00:
                # self.logger.debug('special port found: %x', p.port_no)
                # typically at least the 'LOCAL' port will be returned in this list
                pass
            else:
                self.port_table.add(p.port_no)

        self.logger.debug('%d ports configured',len(self.port_table))
        # now build both the group action set for this set of ports....
        # and also the action list for use in any broadcast packet_out
        bucket_list = []
        # bucket_list = [ parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(ofproto.OFPP_CONTROLLER)]))]
        for port_no in self.port_table:
            bucket_list.append ( parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(port_no)]))
        #bucket_list.append ( parser.OFPBucket (0, ofproto.OFPP_ANY, ofproto.OFPG_ANY, [ parser.OFPActionOutput(17)]))

        # add the list of port out buckets to both groups...
        datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_MODIFY,    ofproto.OFPGT_ALL, self.FLOOD_GROUP_ID, bucket_list))
        ###datapath.send_msg( parser.OFPGroupMod(datapath, ofproto.OFPGC_MODIFY,    ofproto.OFPGT_ALL, self.LEARN_GROUP_ID, bucket_list))

    def flood_from_port(self, datapath, port, data):
        # this is the crude, multi message version......
        self.logger.debug('**Warning - using simple flood mechanism...')
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        for port_no in self.port_table:
            if port_no != port:
                actions = [ parser.OFPActionOutput(port_no) ]
                datapath.send_msg( parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_ANY, actions=actions, data=data))

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        # makesure that we don't try and send a message which doesn't exist
        if buffer_id == ofproto.OFP_NO_BUFFER:
            buffer_id=None

        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # If you hit this you might want to increase
        # the "miss_send_length" of your switch
        if ev.msg.msg_len < ev.msg.total_len:
            self.logger.debug("packet truncated: only %s of %s bytes",
                              ev.msg.msg_len, ev.msg.total_len)
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return
        dst = eth.dst
        src = eth.src

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.logger.info("packet in %s %s %s %s", dpid, src, dst, in_port)

        # learn the mac address
        learned_host = src in self.mac_to_port[dpid] and self.mac_to_port[dpid][src] == in_port

        if not learned_host:
            self.mac_to_port[dpid][src] = in_port
       
        if dst == 'ff:ff:ff:ff:ff:ff':
            self.flood_from_port(datapath, in_port, msg.data)
            if not learned_host:
                self.logger.debug('installing broadcast rule for port: %d src MAC: %s',in_port, eth.src)
                match = parser.OFPMatch(in_port=in_port, eth_src=eth.src, eth_dst='ff:ff:ff:ff:ff:ff')
                actions = [ parser.OFPActionGroup(self.FLOOD_GROUP_ID) ]
                print "adding broadcast flow"
                self.add_flow(datapath, 10, match, actions)
                print "finished adding broadcast flow"
            else:
                self.logger.debug('** WARNING! Ignoring existing broadcast rule for port: %d src MAC: %s',in_port, eth.src)
        # check if we know the destination port for this MAC destination
        # if so we can route rather than flood, and install a flow to optimise the action next time
        elif dst not in self.mac_to_port[dpid]:
            # we don't expect to get messages in this state because we should have seen the earlier ARP by which the origin host acquired the dst ether addr
            self.logger.debug('**Warning: received unicast message for unknown MAC(%s)',dst)
            # this action is not sensible, but what else to do?
            self.flood_from_port(datapath, in_port, msg.data)
        else:
            # we know where the packet should go.....
            # install a flow to avoid packet_in next time
            # the same actions will work for both a packet-out and a flow_mod...
            out_port = self.mac_to_port[dpid][dst]
            #actions = [parser.OFPActionOutput(out_port), parser.OFPActionGroup(self.LEARN_GROUP_ID), parser.OFPActionGroup(self.FLOOD_GROUP_ID)]
            actions = [parser.OFPActionGroup(self.LEARN_GROUP_ID)]
            #actions = [parser.OFPActionOutput(21), parser.OFPActionOutput(17)]

            # if we  don't have a valid buffer_id then need to send a packet out...
            if msg.buffer_id == ofproto.OFP_NO_BUFFER:
                datapath.send_msg( parser.OFPPacketOut(datapath=datapath, buffer_id=ofproto.OFP_NO_BUFFER, in_port=ofproto.OFPP_ANY, actions=actions, data=msg.data))

            # otherwise add_flow will also send the saved message if we give it the buffer ID
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst)
            self.add_flow(datapath, 10, match, actions, msg.buffer_id)
