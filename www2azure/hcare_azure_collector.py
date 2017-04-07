import logging, cherrypy, os
import time
#import datetime
import json
import threading
import requests
from base64 import b64decode,b64encode
from hmac import HMAC
import hashlib
from hashlib import sha256
#from urllib import urlencode
#from urllib import quote_plus
from urllib import pathname2url
#import urllib.parse

# ----- Class that contains the generic methods for websites (user check, error messages)
class WebServerBase:
    @staticmethod
    def error_page(status, message, traceback, version):
        logging.warning("HTTP ERR = %s | %s | %s | %s" % (str(status), str(message), str(version), str(traceback)[0]))
        return "<html><body>Error %s</body></html>" % str(status)
    @staticmethod
    def handle_error():
        logging.warning("HTTP ERR 500")
        cherrypy.response.status = 500
        cherrypy.response.body = ["<html><body>Error 500</body></html>"]

    # --- CREDENTIALS MANAGEMENT
    theusers = { hashlib.sha256(str("username1").encode('utf-8')).hexdigest() : hashlib.sha256(str("username1_password").encode('utf-8')).hexdigest() }
    @staticmethod
    def validate_password(realm='localhost', username='', password=''):
        if 2 < len(username) < 20 and 4 < len(password) < 20:
            userh = hashlib.sha256(str(username).encode('utf-8')).hexdigest()
            passh = hashlib.sha256(str(password).encode('utf-8')).hexdigest()
            if userh in WebServerBase.theusers and WebServerBase.theusers[userh] == passh:
                cherrypy.session['usrun'] = username
                return True
        logging.warning("LOGIN ERROR WITH %s | %s | %s" % (str(realm), str(username), str(password)))
        return False

# ----- Class used to communicate with MS Azure IoT Hub
class D2CMsgSender:
    #API_VERSION = '2016-02-03'
    API_VERSION = '2016-11-14'
    TOKEN_VALID_SECS = 10
    #TOKEN_FORMAT = 'SharedAccessSignature sr=%s&sig=%s&se=%s'
    TOKEN_FORMAT = 'SharedAccessSignature sr=%s&sig=%s&se=%s&skn=%s'

    def __init__(self, connectString=None):
        if connectString is not None:
            iotHost, keyName, keyValue = [sub[sub.index('=') + 1:] for sub in connectString.split(";")]
            self.iotHost = iotHost
            self.keyName = keyName
            self.keyValue = keyValue

    def _buildExpiryOn(self):
        return '%d' % (time.time() + self.TOKEN_VALID_SECS)

    def _buildIoTHubSasToken(self, deviceId2):
        
        resourceUri = '%s/devices/%s' % (self.iotHost, deviceId2)
        targetUri = resourceUri.lower()
        expiryTime = self._buildExpiryOn()
        toSign = '%s\n%s' % (targetUri, expiryTime)
        key = b64decode(self.keyValue.encode('utf-8'))
        
        #python 3 code commented
        signature = pathname2url(b64encode(HMAC(key, toSign.encode('utf-8'), sha256).digest())).replace('/', '%2F')

        # signature = urllib.quote(
        #     base64.b64encode(
        #         hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
        #     )
        # ).replace('/', '%2F')
        return self.TOKEN_FORMAT % (targetUri, signature, expiryTime, self.keyName) 
        """
        sasToken = 'SharedAccessSignature sr=itr2-iothub.azure-devices.net%2Fdevices%2Fitr2-data&sig=nbfex4r7MwYD0th%2FgAdDr8ub5C8fAjRb0VdhuB6Hkog%3D&se=1488882250'
        print("%s" % sasToken)
        
        
        resourceUri = '%s/devices/%s' % (self.iotHost, deviceId2)
        print("resourceUri is {0}").format(resourceUri)
        
        #targetUri = resourceUri.lower()
        # Lower case URL-encoding of the lower case resource URI
        #targetUri = resourceUri.lower().replace('/', '%2f')
        #print("targetUri is {0}").format(targetUri)
        #targetUri = quote_plus(resourceUri)
        #print("targetUri is {0}").format(targetUri)
        
        #expiryTime = self._buildExpiryOn()
        ttl = time.time() + 3600
        print("{0}").format(ttl)
        ttl = 1488882250        
        print("{0}").format(ttl)
        key = self.keyValue
        sign_key = "%s\n%d" % ((quote_plus(resourceUri)), int(ttl).encode("utf-8"))
        print("{0}").format(sign_key)
        signature = b64encode(HMAC(b64decode(key), sign_key, sha256).digest())

        rawtoken = {
            'sr' :  resourceUri,
            'sig': signature,
            'se' : str(int(ttl))
            }

        return 'SharedAccessSignature ' + urlencode(rawtoken)
        #print("%s" % signature)
        # signature = urllib.quote(
        #     base64.b64encode(
        #         hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
        #     )
        # ).replace('/', '%2F')
        
        #return sasToken
        #return self.TOKEN_FORMAT % (targetUri, signature, expiryTime, self.keyName)
        """
    def sendD2CMsg(self, deviceId2, message2):
        sasToken = self._buildIoTHubSasToken(deviceId2)
        #print("%s" % sasToken)
        
        url = 'https://%s/devices/%s/messages/events?api-version=%s' % (self.iotHost, deviceId2, self.API_VERSION)
        #print("%s" % url)
        
        # sasToken for iothub
        #sasToken = 'SharedAccessSignature sr=itr2-iothub.azure-devices.net&sig=n5YYnBT3%2Fmti4MkzD%2FEaGwpyRVx4BChxOEzAAXg1hLE%3D&se=1520418134&skn=iothubowner'
        # sasToken for device
        #sasToken = 'SharedAccessSignature sr=itr2-iothub.azure-devices.net%2Fdevices%2Fitr2-data&sig=nbfex4r7MwYD0th%2FgAdDr8ub5C8fAjRb0VdhuB6Hkog%3D&se=1488882250'
        #print("%s" % sasToken)
        
        # need proxy to work
        #proxies = {'http': 'web-proxy.rose.hpecorp.net:8080', 'https': 'web-proxy.rose.hpecorp.net:8080'}
        #sasToken = 'SharedAccessSignature sr=itr2-iothub.azure-devices.net%2Fdevices%2Fitr2-data&sig=ZzC4yXX5fAHFEM5JNfPSkNJtkDQY1OnejnOToHmyihc%3D&se=1488885609'        
        proxies = {'http': 'web-proxy.cup.hp.com:8080', 'https': 'web-proxy.cup.hp.com:8080'}
        r = requests.post(url, headers={'Authorization': sasToken, 'Content-Type': 'application/json'}, proxies=proxies, data=message2, timeout=10)
        return r.text, r.status_code

# ----- Class containing the code in charge of processing the json data
class HCareAzureCollector(object):
    ourBufferLock        = threading.Lock()
    ourBufferTimeCounter = 0.0
    ourBufferDuration    = 10                 # NUMBER OF SECONDS TO BUFFERIZE BEFORE SENDING TO IOT HUB
    ourBufferData        = dict()               # Data bufferized : ourBufferData['key1=UserID']['key2=EventType'] = list of event_data

    def __init__(self):
        pass

    # -- This function will stack (accumulate) the data in a list part of a dict()
    @staticmethod
    #def add_event_to_buffer(deviceID="", eventName="", eventData=None, trainingMode="none"):
    def add_event_to_buffer(deviceID="", eventName="",eventData=None, trainingMode="none"):
        #if len(deviceID)>1 and len(eventName)>1 and len(eventData) > 0 :
        if (len(deviceID)>1 and len(eventName)>1 and len(eventData)>0):
            # -- Locking the variable for multithread access
            #python 3.0 feature keyword argument 
            #HCareAzureCollector.ourBufferLock.acquire(blocking=True, timeout=-1)
            #HCareAzureCollector.ourBufferLock.acquire(True, -1)
            HCareAzureCollector.ourBufferLock.acquire(-1)
            
            # Adding the event data in the list() of the event type for this deviceID
            buffer_deviceID  = HCareAzureCollector.ourBufferData.get(deviceID) or dict()
            
            
            buffer_eventName = buffer_deviceID.get(eventName) or list()
            
            
            # --- ADDING THE EVENT DATA TO THE BUFFER IN MEMORY
            # @JOJO : Here nothing to do if you want to add every event to the buffer
            # timestamp is in eventData
            buffer_eventName.append(dict(eventData))  # eventData is sent with a pointer, hence needed to duplicate the instance for buffer.
            # buffer_eventName = [dict(eventData)]  # TO KEEP ONLY THE LAST EVENT, NO BUFFERING
    
            buffer_deviceID[eventName] = buffer_eventName
            #print ("buffer_eventName %s in buffer" % buffer_eventName)
            
            HCareAzureCollector.ourBufferData[deviceID] = buffer_deviceID
            #print ("deviceID %s in buffer" % buffer_deviceID)
            # -- Unlocking the variable for threading
            HCareAzureCollector.ourBufferLock.release()

    # -- This function will check if it is time to flush data to the iot hub and do it if needed. return false = no flush, true = flush operated
    @staticmethod
    def flush_buffer_to_iot_hub_if_needed():
        retour = False
        # -- Locking the variable for multithread access
        #python 3.0 feature keyword argument 
        #HCareAzureCollector.ourBufferLock.acquire(blocking=True, timeout=-1)
        #HCareAzureCollector.ourBufferLock.acquire(True, -1)
        HCareAzureCollector.ourBufferLock.acquire(-1)
        
        if HCareAzureCollector.ourBufferTimeCounter == 0.0:
            # -- If this is the 1st time the function is called, initialization of duration counter
            #HCareAzureCollector.ourBufferTimeCounter = time.perf_counter()
            HCareAzureCollector.ourBufferTimeCounter = time.time()
            retour = False

        #elif (time.perf_counter() - HCareAzureCollector.ourBufferTimeCounter) > HCareAzureCollector.ourBufferDuration :
        elif (time.time() - HCareAzureCollector.ourBufferTimeCounter) > HCareAzureCollector.ourBufferDuration:
            # -- HERE IT IS TIME TO FLUSH THE DATA !!
            #print("====== FLUSHING BUFFER (sending to IOT HUB) ======")
            
            #===========================================================
            # Very Important Azure Settings
            #============================================================
            #AzureConnectionString = 'HostName=msiothub2016.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=GKXEAW1ksgIN4C1CASIVcDuHkI6V6KFD+0lxEU0gDZk='
            AzureConnectionString = 'HostName=itr2-iothub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=LVfyxq2bQ0ewlhtFW9bRyBUzHOv6RoI3db1VuvPkUUo='
            #AzuredeviceId = 'iot4hc_azure_collector'
            AzuredeviceId = 'itr2-data'
            
            for buf_device in HCareAzureCollector.ourBufferData.keys():
                events_sent = events_not_sent = ""
                events_to_send = ['tracking', 'heartrate', 'barometer', 'ambientlight', 'skintemperature', 'uv', 'rrinterval']
                # print('Buffer for DeviceID = %s' % buf_device)
                for buf_eventtype in HCareAzureCollector.ourBufferData.get(buf_device).keys():
                    if buf_eventtype in events_to_send:
                        
                        #print("processing %s record" % buf_eventtype)
                        
                        buf_records = HCareAzureCollector.ourBufferData.get(buf_device).get(buf_eventtype)
                        nb_records   = len(buf_records)
                        #last_record = buf_records.pop()
                        
                        # json is dict
                        #json_object  = dict(last_measure)
                        #[event_id],[device_type],[device_id],[event_type],[time_stamp],[user_id],[training_mode],[interval]

                        nb_measures = 0
                        for record in buf_records :
                            nb_measures += 1
                            json_object = dict(record)
                            json_object['event_id'] = 'itr2'
                        #   json_object['MeasureType']  = 'tracking'
                        #   print('- Sending to IOT HuB : %s' % json_object)                    

                            # print('- Sending to IOT HuB : %s' % json_object)
                            d2cMsgSender = D2CMsgSender(AzureConnectionString)
                            message = json.dumps(json_object)
                            logging.info('json message %s sent to iothub' % buf_eventtype)
                            #logging.info('%s' % message)
                            (RetStr, RetCode) = d2cMsgSender.sendD2CMsg(AzuredeviceId, message)
                            if RetCode != 204 :
                                logging.error('Problem sending to IOT HuB : %d | %s' % (RetCode, RetStr))
    
                            events_sent += "%s(%d on %d);" % (buf_eventtype, nb_measures, nb_records)
                        
                    else:
                        events_not_sent += "%s(%d);" % (buf_eventtype, len(HCareAzureCollector.ourBufferData.get(buf_device).get(buf_eventtype) or []))
                print("== FLUSHED TO IOT HUB | Device %s : sent = %s | not sent = %s" % (buf_device, events_sent, events_not_sent or "nil(0 on 0);"))

            # HERE WE ARE RESETING THE BUFFER
            HCareAzureCollector.ourBufferData = dict()

            # Updating the time counter as buffer is now empty
            #HCareAzureCollector.ourBufferTimeCounter = time.perf_counter()
            print("resetting timer for next flush")
            HCareAzureCollector.ourBufferTimeCounter = time.time()
            retour = True

        # -- Unlocking the variable for threading
        logging.info("buffer is not full waiting for new msgs")
        HCareAzureCollector.ourBufferLock.release()
        return retour


    # ----- This method will handle the http request : a json is awaited in POST parameters
    @cherrypy.expose()
    # json interpretation by cherrypy doesn't work with the mobile app
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    #def hpeiot4hc(self, **argv):
    def hpeiot4hc(self):
        retourObj = dict()
        #print argv
        #print type(self)
        json_collected = cherrypy.request.json
        
        # check buffer whenever a json msg is received
        # the function will acquire a lock
        HCareAzureCollector.flush_buffer_to_iot_hub_if_needed()
        
        # only after the buffer checking new json message will be processed
        if not json_collected:
            # -- Here a web request is handled but the POST parameters are empty
            logging.warning("Web request without data received.")
        elif type(json_collected) is not dict :
            # -- Here a web request is handled but the POST parameters are not a json  (dict() type in python)
            logging.warning("Web request with bad data type %s instead of dict" % str(type(json_collected)))
        else:
            # -- Here a web request is handled with a json object in parameters
            if len(json_collected) == 1:
                # -- Here a web request is handled with a json that is not structured like the one sent by the mobile app
                logging.warning("Web request with json not encapsulated")
            else:
                # -- Here extraction of the json sent by the mobile app : it is encapsulated in the key of the unique object in the dict received
                #json_collected_string = argv.popitem()[0]
                #print(" type of data is %s" % type(data))
                #json_collected = json.loads(json_collected_string)
                
                # data received is a dict
                #json_collected = data
                
                # for root_key in json_collected.keys() :
                #     print("K | V = %s | %s" % (str(root_key), str(json_collected.get(root_key) or "")))
                event_payload                 = dict()
                print("%s" % json_collected)
                # the msg received is utf-8
                deviceID    = json_collected.get('deviceid')    or ""
                #print("deviceID received is %s" % deviceID)
                event_payload['device_id']      = deviceID
                event_payload['device_type']    = 'itr2-band'

                userID      = json_collected.get('userid')      or ""
                #print("userID received is %s" % deviceID)
                event_payload['user_id']      = userID 
                #events      = json_collected.get('events')      or list()
                #macaddress  = (json_collected.get('macAddress') or "00:00:00:00:00:00").lower()
                #gpslocation = json_collected.get('locationOfHandheld') or {'longitude': '0.0', 'latitude': '0.0'}
                
                if not(len(deviceID) > 1 and len(userID) > 1) :
                    logging.warning("Web request with json without deviceID or userID or empty event list")
                else:
                    # --- Status data = keep alive data
                    
                    #================================================================
                    # tracking is not needed for now
                    #==============================================================
                    #ts_utc_epoch = int(time.time()) # epoch time UTC as a float
                    #ts_utc_iso   = datetime.datetime.utcfromtimestamp(ts_utc_epoch).isoformat()
                    #print("- %s Reception of %d events from %s | %s | %s | %s" % (ts_utc_iso, len(events), userID, deviceID, macaddress, str(gpslocation)))
                    #tracking_data                 = dict()
                    #tracking_data['userID']       = userID
                    #tracking_data['macaddr']      = macaddress
                    #tracking_data['longitude']    = float(gpslocation['longitude'])
                    #tracking_data['latitude']     = float(gpslocation['latitude'])
                    #tracking_data['ts_utc_epoch'] = ts_utc_epoch
                    #tracking_data['ts_utc_iso']   = ts_utc_iso
                    #HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName='tracking', eventData=tracking_data, trainingMode='none')

                    # --- Buffering events = health measures
                    #for evt in events :
                    # one json record received at a time
                    # json_collected is a dict()
                    # {"eventtype": "rrinterval", "interval": 0.8296, "userid": "vincent.planat@hpe.com", "deviceid": "b81e905908354542", \
                    # "timestamp": 1483509624653, "trainingMode": "SITTING"}
                    event_timestamp            = json_collected.get('timestamp') or int(0)
                    #print("timestamp received is %s" % event_timestamp)
                    event_payload['time_stamp']   = event_timestamp
                    
                    event_type                 = (json_collected.get('eventtype') or "").lower()
                    event_payload['event_type']   = event_type

                    event_trainingMode         = (json_collected.get('trainingMode') or "none").lower()
                    event_payload['training_mode']   = event_trainingMode
                    
                    #event_data		       = dict()
                    #event_data                 = json_collected.get('data') or dict()

                    #if not(len(event_type)>1 and event_timestamp>0 and len(event_data)>0) :
                    if not (len(event_type) > 1 and event_timestamp > 0):
                        logging.warning("Events from %s|%s cannot be parsed" % (userID,deviceID))
                    else:
                        # ------------ HERE WE ARE LOOPING ON THE EVENTS --------------
                        #ts_measure                    = int(event_timestamp/1000)
                        
                        #event_type = event_type.encode('utf-8')
                        
                        #event_payload['ts_utc_epoch'] = ts_measure
                        #event_payload['ts_utc_iso']   = datetime.datetime.utcfromtimestamp(ts_measure).isoformat()
                        # print("EVENT : %d | %s | %s | %s" % (event_timestamp, event_type, str(event_data), str(event_trainingMode)))
                        try:
                            # {"temperature": 31.959999, "eventtype": "skinTemperature", "userid": "vincent.planat@hpe.com", "deviceid": "b81e905908354542", \
                            # "timestamp": 1483509720783, "trainingMode": "WALKING"}
                            # {"eventtype": "heartRate", "userid": "vincent.planat@hpe.com", "rate": "69", "deviceid": "b81e905908354542", \
                            # "timestamp": 1483509453251, "trainingMode": "NONE", "quality": 1}
                            # {"eventtype": "rrinterval", "interval": 0.8296, "userid": "vincent.planat@hpe.com", "deviceid": "b81e905908354542", \
                            # "timestamp": 1483509624653, "trainingMode": "SITTING"}
                            if event_type == u'heartrate':  # example of event_data = {'rate': 73, 'quality': 1}
                                
                                logging.info("heartrate record -> buffer")
                                #event_payload['heartrate_value']   = int(json_collected['rate'])
                                #event_payload['heartrate_quality'] = int(json_collected['quality'])
                                event_payload['rate']   = int(json_collected['rate'])
                                event_payload['quality'] = int(json_collected['quality'])
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                
                            elif event_type == u'skintemperature': # example of event_data = {'temperature': 31.09}
                                
                                logging.info("skintemperature record -> buffer")
                                #event_payload['skintemperature'] = float(json_collected['temperature'])
                                event_payload['temperature'] = float(json_collected['temperature'])
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                
                            elif event_type == u'rrinterval':
                                # example of event_data = {'interval': 0.730048}
                                logging.info("rrinterval record  -> buffer")
                                event_payload['interval']   = float(json_collected['interval'])
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                
                            else:
                                logging.warning("Event type received but not interpreted : %s | %s" % (event_type, str(event_payload)))
                                
                        except Exception as e :
                            logging.warning('Exception parsing event %s : %s' % (event_type, str(e)))
                    # Checking if it is time to flush the buffer
                    #HCareAzureCollector.flush_buffer_to_iot_hub_if_needed()

            # contenu  = "Type   = %s\n" % type(argv)
            # contenu += "Length = %s\n\n" % len(argv)
            # contenu += str(argv)
            # # objraw = argv.get('obj')
            # # if objraw:
            # #     contenu += "\nReceived obj of type :\n%s" % str(type(objraw))
            # heur = datetime.datetime.isoformat(datetime.datetime.now())
            # heur = heur.replace(':','').replace('-','').replace('.','')
            # fichier_nom = "./weblog" + heur
            # fichier = Path(fichier_nom)
            # fich = fichier.open(mode='w', encoding='utf-8', errors='replace')
            # fich.write(contenu)
            # fich.close()

        return retourObj
	

if __name__ == '__main__':
    # --- Logs Definition  logging.Logger.manager.loggerDict.keys()
    Level_of_logs = level = logging.INFO
    logging.addLevelName(logging.DEBUG - 2, 'DEBUG_DETAILS')  # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=Level_of_logs, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(name)s|%(levelname)s|%(asctime)s | %(message)s")  # %(thread)d %(funcName)s L%(lineno)d
    logging.getLogger("requests").setLevel(logging.WARNING)  # On desactive les logs pour la librairie requests
    logging.info("Starting from %s" % str(os.getcwd()))


    # http://docs.cherrypy.org/en/latest/pkg/cherrypy.html?highlight=ssl#cherrypy._cpserver.Server
    server_config = {
        'server.socket_host'        : '16.250.1.189',
	#'server.socket_host'		: '0.0.0.0',
        'server.socket_port'        : 25934,
        'server.socket_queue_size'  : 5, # The "backlog" argument to socket.listen(); specifies the maximum number of queued connections (default 5).
        'server.socket_timeout'     : 10,  # The timeout in seconds for accepted connections (default 10).
        'server.accepted_queue_size': 20,    # The maximum number of requests which will be queued up before the server refuses to accept it (default -1, meaning no limit).
        'server.thread_pool'        : 5,  # The number of worker threads to start up in the pool.
        'server.thread_pool_max'    : 40,  # he maximum size of the worker-thread pool. Use -1 to indicate no limit.

        # 'server.ssl_module'             : 'builtin',    # ''pyopenssl' PAS COMPATIBLE PYHON 3 nov-2016
        # 'server.ssl_private_key'        : Path("." / Path('clepr.pem')).as_posix() ,   # The filename of the private key to use with SSL.
        # 'server.ssl_certificate'        : Path("." / Path('cle_cert.pem')).as_posix(), # The filename of the SSL certificate to use.
        # 'server.ssl_certificate_chain'  : None, # When using PyOpenSSL, the certificate chain to pass to Context.load_verify_locations.
        # 'server.ssl_context'            : None, # When using PyOpenSSL, an instance of SSL.Context.

        'error_page.401': WebServerBase.error_page,
        'error_page.402': WebServerBase.error_page,
        'error_page.403': WebServerBase.error_page,
        'error_page.404': WebServerBase.error_page,  # en cas de parametres incorrects sur une URL/fonction, on finit en 404
        'request.error_response': WebServerBase.handle_error,

        'log.screen': False, 'log.access_file': '', 'log.error_file': '',
        'engine.autoreload.on': False,  # Sinon le server se relance des qu'un fichier py est modifie...
    }
    cherrypy.config.update(server_config)

    app_conf = {'/': {'tools.staticdir.on'  : False,  # 'tools.staticdir.index'  : "index.html", 'tools.staticdir.dir'           : Path().cwd().joinpath("webpages").as_posix(),
                      'tools.auth_basic.on' : False, # 'tools.auth_basic.realm' : 'localhost', 'tools.auth_basic.checkpassword' : WebServerBase.validate_password,
                      'tools.sessions.on'   : False }
                # '/favicon.ico' : { 'tools.staticfile.on': True, 'tools.staticfile.filename': Path().cwd().joinpath("webpages").joinpath("images").joinpath("favicon.gif").as_posix() }
                }
    cherrypy.tree.mount(HCareAzureCollector(), "/", app_conf)

    # --- Loglevel pour CherryPy : a faire une fois les serveurs mounted et avant le start
    for log_mgt in logging.Logger.manager.loggerDict.keys():
        if "cherrypy.access" in log_mgt:
            logging.getLogger(log_mgt).setLevel(logging.WARNING)

    # -------- Lancement --------
    cherrypy.engine.start()
    cherrypy.engine.block()
