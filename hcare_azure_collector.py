import logging, cherrypy, os
from pathlib import Path
import datetime
import time
import json
import hashlib
import threading

# ----- Class that contains the generic methods for websites
class ServBase:
    @staticmethod
    def error_page(status, message, traceback, version):
        logging.warning("HTTP ERR = %s | %s | %s" % (str(status), str(message), str(version)))
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
            if userh in ServBase.theusers and ServBase.theusers[userh] == passh:
                cherrypy.session['usrun'] = username
                return True
        logging.warning("LOGIN ERROR WITH %s | %s | %s" % (str(realm), str(username), str(password)))
        return False

# ----- Class containing the code in charge of processing the json data
class HCareAzureCollector(object):
    ourBufferLock        = threading.Lock()
    ourBufferTimeCounter = 0.0
    ourBufferDuration    = 60                 # NUMBER OF SECONDS TO BUFFERIZE BEFORE SENDING TO IOT HUB
    ourBufferData        = dict()               # Data bufferized : ourBufferData['key1=UserID']['key2=EventType'] = list of event_data

    def __init__(self):
        pass

    # -- This function will stack (accumulate) the data in a list part of a dict()
    @staticmethod
    def add_event_to_buffer(deviceID="", eventName="", eventData=None):
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

            print("====== FLUSHING BUFFER : TO BE DEVELOPPED (sending to IOT HUB) ======")

            # @JOJO : Here you have the buffer to be sent to IOT Hub, it contains list of events

            # @JOJO : the buffer is a list of event_data contained in a dict of dict : buffer[deviceID][eventID] = event_data (a dict) that have been pushed in the buffer

            for buf_device in HCareAzureCollector.ourBufferData.keys() :
                print('Buffer for DeviceID = %s' % buf_device)
                for buf_eventtype in HCareAzureCollector.ourBufferData.get(buf_device).keys() :
                    print(' - %s : %d event data in buffer' % (buf_eventtype, len(HCareAzureCollector.ourBufferData.get(buf_device).get(buf_eventtype) or [])))

            print("====== CONTENT OF THE BUFFER (WILL BE EMPTY) ======")
            #print(str(HCareAzureCollector.ourBufferData))

            # HERE WE ARE ONLY RESETING THE BUFFER
            HCareAzureCollector.ourBufferData = dict()

            # Updating the time counter as buffer is now empty
            HCareAzureCollector.ourBufferTimeCounter = time.perf_counter()
            retour = True

        # -- Unlocking the variable for threading
        HCareAzureCollector.ourBufferLock.release()
        return retour





    # ----- This method will handle the http request : a json is awaited in POST parameters
    @cherrypy.expose()
    # @cherrypy.tools.json_in()
    # @cherrypy.tools.json_out()
    # def get_init(self, _="" ):
    def hpeiot4hc(self, **argv):
        retourObj = dict()
        if not argv:
            # -- Here a web request is handled but the POST parameters are empty
            logging.warning("Web request without data received.")
        elif type(argv) is not dict :
            # -- Here a web request is handled but the POST parameters are not a json  (dict() type in python)
            logging.warning("Web request with bad data type %s instead of dict" % str(type(argv)))
        else :
            # -- Here a web request is handled with a json object in parameters
            if len(argv) != 1 :
                # -- Here a web request is handled with a json that is not structured like the one sent by the mobile app
                logging.warning("Web request with json not encapsulated")
            else :
                # -- Here extraction of the json sent by the mobile app : it is encapsulated in the key of the unique object in the dict received
                json_collected_string = argv.popitem()[0]
                json_collected = json.loads(json_collected_string)

                deviceID = json_collected.get('deviceID') or ""
                userID   = json_collected.get('userID')   or ""
                events   = json_collected.get('events')   or list()

                if not(len(deviceID) > 1 and len(userID) > 1 and len(events) > 0) :
                    logging.warning("Web request with json without deviceID or userID or empty event list")
                else :
                    print("------ DATA RECEIVED : %d events from %s | %s ------" % (len(events), userID, deviceID))
                    for evt in events :
                        event_timestamp     = evt.get('timeStamp') or int(0)
                        event_type          = (evt.get('eventType') or "").lower()
                        event_trainingMode  = evt.get('trainingMode') or False
                        event_data          = evt.get('data') or dict()

                        if not(len(event_type)>1 and event_timestamp>0 and len(event_data)) :
                            logging.warning("Event from %s|%s cannot be parsed" % (userID,deviceID))
                        else :
                            # ------------ HERE WE ARE LOOPING ON THE EVENTS --------------

                            # @JOJO : here you have the loop on each event sent on HTTP by a device and a user
                            # @JOJO : here the idea is to parse each event to be sure to have a correct value --> Not done for the moment
                            # @JOJO : if the event is OK, it can be added to the memory buffer --> done with HCareAzureCollector.add_event_to_buffer(...

                            # @JOJO : uncomment line below to log all event on the screen
                            print("EVENT : %d | %s | %s" % (event_timestamp, event_type, str(event_data)))

                            if   event_type == "gyroscope" :
                                # example of event_data = {'x': -0.579268, 'z': -0.823171, 'y': -0.060976}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "accelerometer" :
                                # example of event_data = {'x': 0.513672, 'z': 0.842285, 'y': 0.063477}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "rrinterval" :
                                # example of event_data = {'interval': 0.730048}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "barometer" :
                                # example of event_data = {'temperature': 35.145833, 'airPressure': 994.240723}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "ambientlight" :
                                # example of event_data = {'brightness': 45}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "heartrate" :
                                # example of event_data = {'rate': 73, 'quality': 1}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "altimeter" :
                                # example of event_data = {'rate': -19.0, 'stepsDescended': 428, 'totalGain': 373506, 'steppingGain': 2175, 'stepsAscended': 512, 'flightsDescended': 4, 'totalLoss': 377450, 'flightsAscended': 5, 'steppingLoss': 1807}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "distance" :
                                # example of event_data = {'totalDistance': 153232, 'currentMotion': 1, 'speed': 0.0, 'pace': 0.0}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "pedometer" :
                                # example of event_data = {'totalSteps': 2064}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "calorie" :
                                # example of event_data = {'calories': 27584}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "skintemperature" :
                                # example of event_data = {'temperature': 31.09}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            elif event_type == "uv" :
                                # example of event_data = {'index': 0}
                                # VALUE TO BE PARSED, VERIFIED, ETC...
                                HCareAzureCollector.add_event_to_buffer(deviceID=deviceID, eventName=event_type, eventData=event_data)
                            else :
                                logging.warning("Event type received and not interpreted : %s | %s" % (event_type, str(event_data)))

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
        'server.socket_host'        : '0.0.0.0',
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

        'error_page.401': ServBase.error_page,
        'error_page.402': ServBase.error_page,
        'error_page.403': ServBase.error_page,
        'error_page.404': ServBase.error_page,  # en cas de parametres incorrects sur une URL/fonction, on finit en 404
        'request.error_response': ServBase.handle_error,

        'log.screen': False, 'log.access_file': '', 'log.error_file': '',
        'engine.autoreload.on': False,  # Sinon le server se relance des qu'un fichier py est modifie...
    }
    cherrypy.config.update(server_config)

    app_conf = {'/': {'tools.staticdir.on'  : False,  # 'tools.staticdir.index'  : "index.html", 'tools.staticdir.dir'           : Path().cwd().joinpath("webpages").as_posix(),
                      'tools.auth_basic.on' : False, # 'tools.auth_basic.realm' : 'localhost', 'tools.auth_basic.checkpassword' : ServBase.validate_password,
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
