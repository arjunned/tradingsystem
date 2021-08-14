# -*- coding: utf-8 -*-
"""
Created on Fri Apr 23 11:38:36 2021

@author: Sandip.Khairnar
"""

import websocket
import six
import base64
import zlib
import datetime
import time
import json
import threading
import ssl

class SmartWebSocket(object):
    ROOT_URI='wss://wsfeeds.angelbroking.com/NestHtml5Mobile/socket/stream'
    HB_INTERVAL=30
    HB_THREAD_FLAG=False
    WS_RECONNECT_FLAG=False
    feed_token=None
    client_code=None
    ws=None
    task_dict = {}
    
    def __init__(self, FEED_TOKEN, CLIENT_CODE):
        self.root = self.ROOT_URI
        self.feed_token = FEED_TOKEN
        self.client_code = CLIENT_CODE
        if self.client_code == None or self.feed_token == None:
            return "client_code or feed_token or task is missing"

    def _subscribe_on_open(self):
        request = {"task": "cn", "channel": "NONLM", "token": self.feed_token, "user": self.client_code,
                   "acctid": self.client_code}
        print(request)
        self.ws.send(
            six.b(json.dumps(request))
        )
        
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            # More statements comes here
            if self.HB_THREAD_FLAG:
                break
            print(datetime.datetime.now().__str__() + ' : Start task in the background')
            
            self.heartBeat()

            time.sleep(self.HB_INTERVAL)
    
    def subscribe(self, task, token):
        # print(self.task_dict)
        self.task_dict.update([(task,token),])
        # print(self.task_dict)
        if task in ("mw", "sfi", "dp"):
            strwatchlistscrips = token  # dynamic call
        
            try:
                request = {"task": task, "channel": strwatchlistscrips, "token": self.feed_token,
                           "user": self.client_code, "acctid": self.client_code}
        
                self.ws.send(
                    six.b(json.dumps(request))
                )
                return True
            except Exception as e:
                self._close(reason="Error while request sending: {}".format(str(e)))
                raise
        else:
            print("The task entered is invalid, Please enter correct task(mw,sfi,dp) ")
    
    def resubscribe(self):
        for task, marketwatch in self.task_dict.items():
            print(task, '->', marketwatch)
            try:
                request = {"task": task, "channel": marketwatch, "token": self.feed_token,
                           "user": self.client_code, "acctid": self.client_code}
        
                self.ws.send(
                    six.b(json.dumps(request))
                )
                return True
            except Exception as e:
                self._close(reason="Error while request sending: {}".format(str(e)))
                raise
        
    def heartBeat(self):        
        try:
            request = {"task": "hb", "channel": "", "token": self.feed_token, "user": self.client_code,
                       "acctid": self.client_code}
            print(request)
            self.ws.send(
                six.b(json.dumps(request))
            )
    
        except:
            print("HeartBeat Sending Failed")
            # time.sleep(60)
           
    def _parse_text_message(self, message):
        """Parse text message."""
     
        data = base64.b64decode(message)
        
        try:
            data = bytes((zlib.decompress(data)).decode("utf-8"), 'utf-8')
            data = json.loads(data.decode('utf8').replace("'", '"'))
            data = json.loads(json.dumps(data, indent=4, sort_keys=True))
        except ValueError:
            return
        
        # return data
        if data:
            self._on_message(self.ws,data)
    
    def connect(self):
        # websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(self.ROOT_URI, 
                                     on_message=self.__on_message, 
                                     on_close=self.__on_close, 
                                     on_open=self.__on_open,
                                     on_error=self.__on_error)
        
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def __on_message(self, ws, message):
        self._parse_text_message(message)
        # print(msg)
            
    def __on_open(self, ws):
        print("__on_open################")
        self.HB_THREAD_FLAG = False
        self._subscribe_on_open()
        if self.WS_RECONNECT_FLAG:
            self.WS_RECONNECT_FLAG = False
            self.resubscribe()
        else:
            self._on_open(ws)
    
    def __on_close(self, ws):
        self.HB_THREAD_FLAG = True
        print("__on_close################")
        self._on_close(ws)
              
    def __on_error(self, ws, error):
                             
        if ( "timed" in str(error) ) or ( "Connection is already closed" in str(error) ) or ( "Connection to remote host was lost" in str(error) ):
            
            self.WS_RECONNECT_FLAG = True
            self.HB_THREAD_FLAG = True
           
            if (ws is not None):
                ws.close()
                ws.on_message = None
                ws.on_open = None
                ws.close = None    
                # print (' deleting ws')
                del ws
       
            self.connect()
        else:
            print ('Error info: %s' %(error))
            self._on_error(ws, error)

    def _on_message(self, ws, message):
        pass
            
    def _on_open(self, ws):
        pass
    
    def _on_close(self, ws):
        pass
              
    def _on_error(self, ws, error):
        pass