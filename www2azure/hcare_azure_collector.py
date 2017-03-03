import logging, cherrypy, os
import time, datetime
import json
import threading
import requests
import base64
import hmac
import hashlib
import urllib
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
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'

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
        key = base64.b64decode(self.keyValue.encode('utf-8'))
        #signature = urllib.parse.quote(
        signature = urllib.pathname2url(
            base64.b64encode(
                hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
            )
        ).replace('/', '%2F')
        # signature = urllib.quote(
        #     base64.b64encode(
        #         hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
        #     )
        # ).replace('/', '%2F')
        return self.TOKEN_FORMAT % (signature, expiryTime, self.keyName, targetUri)

    def sendD2CMsg(self, deviceId2, message2):
        sasToken = self._buildIoTHubSasToken(deviceId2)
        url = 'https://%s/devices/%s/messages/events?api-version=%s' % (self.iotHost, deviceId2, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message2, timeout=10)
        return r.text, r.status_code

# ----- Class containing the code in charge of processing the json data
class HCareAzureCollector(object):
    ourBufferLock        = threading.Lock()
    ourBufferTimeCounter = 0.0
    ourBufferDuration    = 45                 # NUMBER OF SECONDS TO BUFFERIZE BEFORE SENDING TO IOT HUB
    ourBufferData        = dict()               # Data bufferized : ourBufferData['key1=UserID']['key2=EventType'] = list of event_data

    def __init__(self):
        pass

    # -- This function will stack (accumulate) the data in a list part of a dict()
    @staticmethod
    #def add_event_to_buffer(deviceID="", eventName="", eventData=None, trainingMode="none"):
	def add_event_to_buffer(deviceID="", eventName="",trainingMode="none"):
        #if len(deviceID)>1 and len(eventName)>1 and len(eventData) > 0 :
		if len(deviceID)>1 and len(eventName)>1 and len(eventData) > 0 :
            # -- Locking the variable for multithread access
            HCareAzureCollector.ourBufferLock.acquire(blocking=True, timeout=-1)

            # Adding the event data in the list() of the event type for this deviceID
            buffer_deviceID  = HCareAzureCollector.ourBufferData.get(deviceID) or dict()
            buffer_eventName = buffer_deviceID.get(eventName) or list()

            # --- ADDING THE EVENT DATA TO THE BUFFER IN MEMORY
            # @JOJO : Here nothing to do if you want to add every event to the buffer
            buffer_eventName.append(dict(eventData))  # eventData is sent with a pointer, hence needed to duplicate the instance for buffer.
            # buffer_eventName = [dict(eventData)]  # TO KEEP ONLY THE LAST EVENT, NO BUFFERING

            buffer_deviceID[eventName] = buffer_eventName
            HCareAzureCollector.ourBufferData[deviceID] = buffer_deviceID

            # -- Unlocking the variable for threading
            HCareAzureCollector.ourBufferLock.release()

    # -- This function will check if it is time to flush data to the iot hub and do it if needed. return false = no flush, true = flush operated
    @staticmethod
    def flush_buffer_to_iot_hub_if_needed():
        retour = False
        # -- Locking the variable for multithread access
        HCareAzureCollector.ourBufferLock.acquire(blocking=True, timeout=-1)

        if HCareAzureCollector.ourBufferTimeCounter == 0.0 :
            # -- If this is the 1st time the function is called, initialization of duration counter
            HCareAzureCollector.ourBufferTimeCounter = time.perf_counter()
            retour = False

        elif (time.perf_counter() - HCareAzureCollector.ourBufferTimeCounter) > HCareAzureCollector.ourBufferDuration :
            # -- HERE IT IS TIME TO FLUSH THE DATA !!
            #print("====== FLUSHING BUFFER (sending to IOT HUB) ======")
            #AzureConnectionString = 'HostName=msiothub2016.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=GKXEAW1ksgIN4C1CASIVcDuHkI6V6KFD+0lxEU0gDZk='
            AzureConnectionString = 'HostName=itr2-iothub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=LVfyxq2bQ0ewlhtFW9bRyBUzHOv6RoI3db1VuvPkUUo='
            #AzuredeviceId = 'iot4hc_azure_collector'
            AzuredeviceId = 'itr2-data'
            for buf_device in HCareAzureCollector.ourBufferData.keys() :
                events_sent = events_not_sent = ""
                events_to_send = ['tracking', 'heartrate', 'barometer', 'ambientlight', 'skintemperature', 'uv']
                # print('Buffer for DeviceID = %s' % buf_device)
                for buf_eventtype in HCareAzureCollector.ourBufferData.get(buf_device).keys() :
                    if buf_eventtype in events_to_send :
                        buf_measures = HCareAzureCollector.ourBufferData.get(buf_device).get(buf_eventtype)
                        nb_mesures   = len(buf_measures)
                        last_measure = buf_measures.pop()
                        json_object  = dict(last_measure)
                        json_object['AzureDeviceId'] = 'iot4hc_azure_collector'
                        json_object['SensorID']      = buf_device
                        json_object['MeasureType']   = buf_eventtype

                        # print('- Sending to IOT HuB : %s' % json_object)
                        d2cMsgSender = D2CMsgSender(AzureConnectionString)
                        message = json.dumps(json_object)
                        (RetStr, RetCode) = d2cMsgSender.sendD2CMsg(AzuredeviceId, message)
                        if RetCode != 204 :
                            logging.error('Problem sending to IOT HuB : %d | %s' % (RetCode, RetStr))

                        # nb_mesures = 0
                        # for measure in buf_measures :
                        #     nb_mesures += 1
                        #     json_object = dict(measure)
                        #     json_object['SensorID']     = buf_device
                        #     json_object['MeasureType']  = 'tracking'
                        #     print('- Sending to IOT HuB : %s' % json_object)


                        events_sent += "%s(1 on %d);" % (buf_eventtype, nb_mesures)
                    else :
                        events_not_sent += "%s(%d);" % (buf_eventtype, len(HCareAzureCollector.ourBufferData.get(buf_device).get(buf_eventtype) or []))
                print("== FLUSHED TO IOT HUB | Device %s : sent = %s | not sent = %s" % (buf_device, events_sent, events_not_sent))

            # HERE WE ARE RESETING THE BUFFER
            HCareAzureCollector.ourBufferData = dict()

            # Updating the time counter as buffer is now empty
            HCareAzureCollector.ourBufferTimeCounter = time.perf_counter()
            retour = True

        # -- Unlocking the variable for threading
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
	data = cherrypy.request.json
        if not data:
            # -- Here a web request is handled but the POST parameters are empty
            logging.warning("Web request without data received.")
        elif type(data) is not dict :
            # -- Here a web request is handled but the POST parameters are not a json  (dict() type in python)
            logging.warning("Web request with bad data type %s instead of dict" % str(type(data)))
        else :
            # -- Here a web request is handled with a json object in parameters
            if len(data) == 1 :
                # -- Here a web request is handled with a json that is not structured like the one sent by the mobile app
                logging.warning("Web request with json not encapsulated")
            else :
                # -- Here extraction of the json sent by the mobile app : it is encapsulated in the key of the unique object in the dict received
                #json_collected_string = argv.popitem()[0]
		json_collected_string = data
                print json_collected_string
		json_collected = json.loads(json_collected_string)
		
                # for root_key in json_collected.keys() :
                #     print("K | V = %s | %s" % (str(root_key), str(json_collected.get(root_key) or "")))
				
                deviceID    = json_collected.get('deviceID')    or ""
                userID      = json_collected.get('userID')      or ""
                #events      = json_collected.get('events')      or list()
                macaddress  = (json_collected.get('macAddress') or "00:00:00:00:00:00").lower()
                gpslocation = json_collected.get('locationOfHandheld') or {'longitude': '0.0', 'latitude': '0.0'}
				
                if not(len(deviceID) > 1 and len(userID) > 1) :
                    logging.warning("Web request with json without deviceID or userID or empty event list")
                else :
                    # --- Status data = keep alive data
                    ts_utc_epoch = int(time.time()) # epoch time UTC as a float
                    ts_utc_iso   = datetime.datetime.utcfromtimestamp(ts_utc_epoch).isoformat()
                    #print("- %s Reception of %d events from %s | %s | %s | %s" % (ts_utc_iso, len(events), userID, deviceID, macaddress, str(gpslocation)))
                    tracking_data                 = dict()
                    tracking_data['userID']       = userID
                    tracking_data['macaddr']      = macaddress
                    tracking_data['longitude']    = float(gpslocation['longitude'])
                    tracking_data['latitude']     = float(gpslocation['latitude'])
                    tracking_data['ts_utc_epoch'] = ts_utc_epoch
                    tracking_data['ts_utc_iso']   = ts_utc_iso
                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName='tracking', eventData=tracking_data, trainingMode='none')

                    # --- Buffering events = health measures
                    #for evt in events :
                        event_timestamp            = json_collected.get('timeStamp') or int(0)
                        event_type                 = (json_collected.get('eventType') or "").lower()
                        event_trainingMode         = (json_collected.get('trainingMode') or "none").lower()
						event_data				   =dict()
                        #event_data                 = json_collected.get('data') or dict()

                        #if not(len(event_type)>1 and event_timestamp>0 and len(event_data)>0) :
						if not(len(event_type)>1 and event_timestamp>0) :
                            logging.warning("Events from %s|%s cannot be parsed" % (userID,deviceID))
                        else :
                            # ------------ HERE WE ARE LOOPING ON THE EVENTS --------------
                            ts_measure                    = int(event_timestamp/1000)
                            event_payload                 = dict()
                            event_payload['ts_utc_epoch'] = ts_measure
                            event_payload['ts_utc_iso']   = datetime.datetime.utcfromtimestamp(ts_measure).isoformat()
                            # print("EVENT : %d | %s | %s | %s" % (event_timestamp, event_type, str(event_data), str(event_trainingMode)))
                            try :
                                if event_type == "heartrate" :  # example of event_data = {'rate': 73, 'quality': 1}
                                    event_payload['heartrate_value']   = int(json_collected['rate'])
                                    event_payload['heartrate_quality'] = int(json_collected['quality'])
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                elif event_type == "barometer" : # example of event_data = {'temperature': 35.145833, 'airPressure': 994.240723}
                                    event_payload['baro_airtemperature'] = float(json_collected['temperature'])
                                    event_payload['baro_airpressure']    = float(json_collected['airPressure'])
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                elif event_type == "ambientlight" : # example of event_data = {'brightness': 45}
                                    event_payload['brightness'] = int(json_collected['brightness'])
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                elif event_type == "skintemperature" : # example of event_data = {'temperature': 31.09}
                                    event_payload['skintemperature'] = float(json_collected['temperature'])
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)
                                elif event_type == "uv" : # example of event_data = {'index': 0}
                                    event_payload['uv'] = int(json_collected['index'])
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_payload, trainingMode=event_trainingMode)


                                elif event_type == "gyroscope" :
                                    # example of event_data = {'x': -0.579268, 'z': -0.823171, 'y': -0.060976}
									event_data['x']   = json_collected['x']
									event_data['z']   = json_collected['z']
									event_data['y']   = json_collected['y']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "accelerometer" :
                                    # example of event_data = {'x': 0.513672, 'z': 0.842285, 'y': 0.063477}
									event_data['x']   = json_collected['x']
									event_data['z']   = json_collected['z']
									event_data['y']   = json_collected['y']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "rrinterval" :
                                    # example of event_data = {'interval': 0.730048}
									event_data['interval']   = json_collected['interval']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "altimeter" :
                                    # example of event_data = {'rate': -19.0, 'stepsDescended': 428, 'totalGain': 373506, 'steppingGain': 2175, 'stepsAscended': 512, 'flightsDescended': 4, 'totalLoss': 377450, 'flightsAscended': 5, 'steppingLoss': 1807}
                                    event_data['rate']   = json_collected['rate']
									event_data['stepsDescended']   = json_collected['stepsDescended']
									event_data['totalGain']   = json_collected['totalGain']
									event_data['steppingGain']   = json_collected['steppingGain']
									event_data['stepsAscended']   = json_collected['stepsAscended']
									event_data['flightsDescended']   = json_collected['flightsDescended']
									event_data['totalLoss']   = json_collected['totalLoss']
									event_data['flightsAscended']   = json_collected['flightsAscended']
									event_data['steppingLoss']   = json_collected['steppingLoss']
									# VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "distance" :
                                    # example of event_data = {'totalDistance': 153232, 'currentMotion': 1, 'speed': 0.0, 'pace': 0.0}
									event_data['totalDistance']   = json_collected['totalDistance']
									event_data['currentMotion']   = json_collected['currentMotion']
									event_data['speed']   = json_collected['speed']
									event_data['pace']   = json_collected['pace']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "pedometer" :
                                    # example of event_data = {'totalSteps': 2064}
									event_data['totalSteps']   = json_collected['totalSteps']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "calorie" :
                                    # example of event_data = {'calories': 27584}
									event_data['calories']   = json_collected['calories']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                elif event_type == "contact" :
                                    # example of event_data = {'state': 1}
									event_data['state']   = json_collected['state']
                                    # VALUE TO BE PARSED, VERIFIED, ETC...
                                    HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data, trainingMode=event_trainingMode)
                                else :
                                    logging.warning("Event type received and not interpreted : %s | %s" % (event_type, str(event_data)))
                            except Exception as e :
                                logging.warning('Exception parsing event %s : %s' % (event_type, str(e)))
                    # Checking if it is time to flush the buffer
                    HCareAzureCollector.flush_buffer_to_iot_hub_if_needed()

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
