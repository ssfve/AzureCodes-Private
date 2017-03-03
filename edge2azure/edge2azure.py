import time
import os
import sys

import logging
import configparser
import requests
import json
import threading

from kafka import KafkaConsumer
#import urllib.parse
import base64
import hashlib
import urllib
import hmac

## module constants
APP_NAME = "edge2azure"
VERSION = "0.1"

# ==============================================================================
# Class
# ==============================================================================
class D2CMsgSender:
    API_VERSION = '2016-02-03'
    TOKEN_VALID_SECS = 10
    TOKEN_FORMAT = 'SharedAccessSignature sig=%s&se=%s&skn=%s&sr=%s'
    
    def __init__(self, connect_string=None):
        if connect_string is not None:
            iotHost, keyName, keyValue = [sub[sub.index('=') + 1:] for sub in connect_string.split(";")]
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
        
        #python 3 code commented
        signature = urllib.pathname2url(base64.b64encode(hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest())).replace('/', '%2F')

        # signature = urllib.quote(
        #     base64.b64encode(
        #         hmac.HMAC(key, toSign.encode('utf-8'), hashlib.sha256).digest()
        #     )
        # ).replace('/', '%2F')
        return self.TOKEN_FORMAT % (signature, expiryTime, self.keyName, targetUri)	

    def sendD2CMsg(self, deviceId2, message2):
        sasToken = self._buildIoTHubSasToken(deviceId2)
        logging.info("sasToken is %s" % sasToken)
        url = 'https://%s/devices/%s/messages/events?api-version=%s' % (self.iotHost, deviceId2, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message2, timeout=10)
        return r.text, r.status_code

class GenericSensor(object):
    """ Keep common attributes for snesor, and provides generic method for different sensor.

    Args:
        None

    Attributes:
        __event_id (str): uuid value in string for uniquely identify an event in sensor
        attr_2_column_mapping (dict): Stands for mapping between instance attribute and (column name in Cassandra
         table, data type)
        karrios_data_points_list (list): a list of data points (dict) in Karrios data point:
        for example: one data point is {'timestamp': 1456939614201L, 'name': 'raw_gy_steps_ascended', 'value': 4408,
        'tags': {'user_id': u'vincent.planat@hpe.com', 'event_type': u'altimeter',
        'event_id': u'664df9f0-f838-4059-8cf2-470586a3f2af', 'training_mode': u'SITTING', 'device_type': u'msband2',
        'device_id': u'b81e905908354542'}}
    """
    #json_object = dict()

    def __init__(self):
        self.__event_id = ""
        self.__device_type = ""
        self.__device_id = ""
        self.__event_type = ""
        self.__timestamp = ""
        self.__user_id = ""
        self.__training_mode = ""
        self.attr_2_column_mapping = dict()
        self.attr_2_column_mapping["__event_id"] = ("event_id", "uuid")
        self.attr_2_column_mapping["__device_type"] = ("device_type", "text")
        self.attr_2_column_mapping["__device_id"] = ("device_id", "text")
        self.attr_2_column_mapping["__event_type"] = ("event_type", "text")
        self.attr_2_column_mapping["__timestamp"] = ("time_stamp", "bigint")
        self.attr_2_column_mapping["__user_id"] = ("user_id", "text")
        self.attr_2_column_mapping["__training_mode"] = ("training_mode", "text")

    def set_sensor(self, **kwargs):
        """Set attributes of the instance
            Args:
                **kwargs(dict): a dictionary of argument key and value pairs
        Returns:
            None
        """
        self.__event_id = kwargs.get("eventId", "NA")
        self.__device_type = kwargs.get("deviceType", "NA")
        self.__device_id = kwargs.get("deviceid", "NA")
        self.__event_type = kwargs.get("eventtype", "NA")
        self.__timestamp = kwargs.get("timestamp", "NA")
        self.__user_id = kwargs.get("userid", "NA")
        self.__training_mode = kwargs.get("trainingMode", "NA")

    def issue_send_IoT_hub(self, json_dumps, sensor_type):
        """ Set attributes of the instance
            Args:
                json_dumps(library function): reference to json.dumps
        Returns:
            None
        """	
        json_object = dict()
        try:
            # Loop through attributes in the self object and generate INSERT query and issue the query
            for key in self.__dict__.keys():
                if key.startswith("_" ):
                    dict_key = key.replace("_" + self.__class__.__name__, "").replace("_GenericSensor", "")
                    json_key = self.attr_2_column_mapping[dict_key][0]
                    json_value = self.__dict__[key]
                    json_object[json_key] = json_value
            
            json_object['iothub_msg_type'] = "edge2azure"    
            json_object['topic'] = sensor_type
        except KeyboardInterrupt:
            sys.exit(2)	
        except Exception as e:
            #logging.critical("USER BREAK : Still %d objects to process on %d" % (nb_msg_in_queue,Edge2AzureWorker.ourQueue.qsize()))
            logging.critical("Killing threads...Error is %s" % e)
        # To Azure
        #d2cMsgSender = D2CMsgSender(connectionString)    
        message = json_dumps(json_object)
        if message:
            answer = d2cMsgSender.sendD2CMsg(deviceId, message)
            logging.info("Sending 1 message to Azure IoT Hub...")
            logging.info("message sent is")
            #print "JSONStr:",message
            logging.info("answer is ----------------------------")
            logging.info(answer)
            logging.info("----------------------------")

class HeartRate(GenericSensor):
    def __init__(self):
        super(HeartRate, self).__init__()
        self.__rate = None
        self.__quality = None
        self.attr_2_column_mapping["__rate"] = ("rate", "int")
        self.attr_2_column_mapping["__quality"] = ("quality", "int")

    def set_sensor(self, **kwargs):
        super(HeartRate, self).set_sensor(**kwargs)
        self.__rate = kwargs.get("rate", None)
        self.__quality = kwargs.get("quality", None)

class Edge2AzureWorker(threading.Thread):

    ourQueueIDminiNotPrio = 0
    ourThreads  = list()
    ourStats    = dict()
    ourBufferLock = threading.Lock()

    @staticmethod
    def initProcessQueue():
        #Edge2AzureWorker.ourThreads.clear()
        Edge2AzureWorker.ourThreads      = list()
        Edge2AzureWorker.ourStats        = dict({'nb.msg.sent':0,'starting.time':0.0,'duration.sec':0,'nb.msg.sent.per.sec':0.0})

    @staticmethod
    def closeProcessQueue():
        #Edge2AzureWorker.ourThreads.clear()
        Edge2AzureWorker.ourThreads = list()

    @staticmethod
    def createThread():
        new_th =  Edge2AzureWorker()
        new_th.setDaemon(True)
        new_th.start()
        if len(Edge2AzureWorker.ourThreads) == 0 :
            Edge2AzureWorker.ourStats['starting.time'] = time.time()
        Edge2AzureWorker.ourThreads.append(new_th)
        logging.info("Thread for --%s-- Created" % sensor_type_name)
    
    @staticmethod
    def waitFinished():
        logging.info("Start Listening...")
        #Edge2AzureWorker.ourQueue.join()         # Attente que la queue soit vide
        try:
            for t in Edge2AzureWorker.ourThreads:    # wait all threads to finish
                t.join()
        except KeyboardInterrupt:
            sys.exit(2)
        #logging.info("End Listening...")
        Edge2AzureWorker.ourStats['duration.sec'] = time.time()-Edge2AzureWorker.ourStats['starting.time']
        Edge2AzureWorker.ourStats['nb.msg.sent.per.sec'] = \
        Edge2AzureWorker.ourStats['nb.msg.sent'] / Edge2AzureWorker.ourStats['duration.sec']

    def __init__(self):
        super(Edge2AzureWorker, self).__init__(group=None, target=None) # daemon=False)  # name=None
        #self.myTLock            = threading.Lock()
        self.threadType         = sensor_type_name

    # On demarre avec start(), mais on implemente les actions dans run(). 
    # Start() de la superclass creera le thread et appelera run() dedans
    def run(self):
        #perf1 = time.time()- perf0
        #if Edge2AzureWorker.ourStats['nb.msg.to.send'] > 0:    
        # put in buffer if specific buffer is not full
        for message in kafka_consumer_list:
            try:
                logging.info(" Received message topic is %s" % message.topic)
                logging.info(" Received message partition is %d" % message.partition)
                logging.info(" Received message offset is %d" % message.offset)
                logging.info(" Received message key is %s" % message.key)
                logging.info(" Received message value is %s" % message.value)
                #msg_json_dict = json.loads(message.value)
                sensor_type = message.value['eventtype'].strip().lower()
                logging.info(" sensor_type is %s" % sensor_type)
                logging.info(" threadType is %s" % self.threadType)
                if self.threadType == sensor_type:
                    # depends on msg get exact bufferlist and exact bufferlimit
                    sensor_buffer_list = bufferfactory.get_sensor_bufferlist(sensor_type)
                    sensor_buffer_limit = bufferfactory.get_sensor_bufferlimit(sensor_type)
                    #logging.info(" putting...")
                    sensor_buffer_list.append(json.dumps(message.value))
                    logging.info(" Cached 1 message from sensor --%s-- to buffer" % sensor_type)
                    logging.info(" Having %s message(s) of %s buffer msg limit" % (len(sensor_buffer_list),sensor_buffer_limit))
                    
                    if int(len(sensor_buffer_list)) >= int(sensor_buffer_limit):
                        # send record in buffer one by one to IoT Hub
                        logging.info(" Buffer of type --%s-- is full, sending msgs..." % sensor_type_name)
                        for record in sensor_buffer_list:
                            if record:
                                json_str = json.loads(record)
                                sensor_type = json_str['eventtype'].strip().lower()
                                # Specify the correct table for insertion
                                #if normalized_sensor_type_name not in DISABLE_SENSOR_SET:	
                                try:
                                    sensor_object_dict[sensor_type].set_sensor(**json_str)
                                    # thread safety lock on when writing to buffer
                                    Edge2AzureWorker.ourBufferLock.acquire(blocking=True, timeout=-1)
                                    sensor_object_dict[sensor_type].issue_send_IoT_hub(json.dumps,sensor_type)
                                    # thread safety lock off
                                    Edge2AzureWorker.ourBufferLock.release()
                                    logging.info(" 1 msg sent to Azure Iot Hub Successfully for sensor --%s--" % sensor_type)
                                except KeyError:
                                    logging.info(" There is NO sensor object available for this sensor type %s" % sensor_type)    
                            
                        logging.info(" Resetting Buffer of type --%s--" % sensor_type)
                        # the buffer is sent so clear old stuff
                        bufferfactory.clear_sensor_bufferlist(sensor_type)
            except KeyboardInterrupt:
                sys.exit(2)	
            except Exception as e:
                #logging.critical("USER BREAK : Still %d objects to process on %d" % (nb_msg_in_queue,Edge2AzureWorker.ourQueue.qsize()))
                logging.critical("Killing threads...Error is %s" % e)
 
class SensorFactory(object):
    def produce_sensor_object(self, sensor_type_name):
        """Return sensor object based on sensor type
            Args:
                sensor_type_name (str): validate type name of sensors like hdhld, accelerometer
        Returns:
            sensor object
        """
        if sensor_type_name.strip().lower() == "heartrate":
            return HeartRate()
        

class BufferFactory(object):
    """ return the type of buffer for the message and implement buffering
    Args:
        sensor_type_name
        sensor_
    Returns:
        list
    """
    def __init__(self, sensor_limit=3):
        logging.info(" Initializing buffer for all sensors STARTED")
        self.accel_limit = config_parser.get(APP_NAME,"accelerometer.buffersize")
        logging.info("accel buffer limit is %s" % self.accel_limit)
        self.gyroscope_limit = config_parser.get(APP_NAME,"gyroscope.buffersize")
        logging.info("gyroscope buffer limit is %s" % self.gyroscope_limit)
        self.altimeter_limit = config_parser.get(APP_NAME,"altimeter.buffersize")
        logging.info("altimeter buffer limit is %s" % self.altimeter_limit)
        self.ambientlight_limit = config_parser.get(APP_NAME,"ambientLight.buffersize")
        logging.info("ambientLight buffer limit is %s" % self.ambientlight_limit)
        self.barometer_limit = config_parser.get(APP_NAME,"barometer.buffersize")
        logging.info("barometer buffer limit is %s" % self.barometer_limit)
        self.calorie_limit = config_parser.get(APP_NAME,"calorie.buffersize")
        logging.info("calorie buffer limit is %s" % self.calorie_limit)
        self.distance_limit = config_parser.get(APP_NAME,"distance.buffersize")
        logging.info("distance buffer limit is %s" % self.distance_limit)
        self.uv_limit = config_parser.get(APP_NAME,"uv.buffersize")
        logging.info("uv buffer limit is %s" % self.uv_limit)
        self.skintemperature_limit = config_parser.get(APP_NAME,"skinTemperature.buffersize")
        logging.info("skinTemperature buffer limit is %s" % self.skintemperature_limit)
        self.gsr_limit = config_parser.get(APP_NAME,"gsr.buffersize")
        logging.info("gsr buffer limit is %s" % self.gsr_limit)
        self.heartrate_limit = config_parser.get(APP_NAME,"heartRate.buffersize")
        logging.info("heartRate buffer limit is %s" % self.heartrate_limit)
        self.rrinterval_limit = config_parser.get(APP_NAME,"rrInterval.buffersize")
        logging.info("rrInterval buffer limit is %s" % self.rrinterval_limit)
        self.accel_store = list()
        self.gyroscope_store = list()
        self.altimeter_store = list()
        self.ambientlight_store = list()
        self.barometer_store = list()
        self.calorie_store = list()
        self.distance_store = list()
        self.uv_store = list()
        self.skintemperature_store = list()
        self.gsr_store = list()
        self.heartrate_store = list()
        self.rrinterval_store = list()
        logging.info(" Initializing buffer for all sensors ENDED")

    def get_sensor_bufferlimit(self, sensor_type_name=None):
        if sensor_type_name == "hdhld":
            return self.hdhld_limit
        elif sensor_type_name == "accelerometer" or sensor_type_name.strip().lower() == "accel":
            return self.accel_limit
        elif sensor_type_name == "gyroscope":
            return self.gyroscope_limit
        elif sensor_type_name == "altimeter":
            return self.altimeter_limit
        elif sensor_type_name == "ambientlight":
            return self.ambientlight_limit
        elif sensor_type_name == "barometer":
            return self.barometer_limit
        elif sensor_type_name == "calorie":
            return self.calorie_limit
        elif sensor_type_name == "distance":
            return self.distance_limit
        elif sensor_type_name == "uv":
            return self.uv_limit
        elif sensor_type_name == "skintemperature":
            return self.skintemperature_limit
        elif sensor_type_name == "gsr":
            return self.gsr_limit
        elif sensor_type_name == "heartrate":
            return self.heartrate_limit
        elif sensor_type_name == "rrinterval":
            return self.rrinterval_limit
        else:
            return None

    def get_sensor_bufferlist(self, sensor_type_name=None):
        if sensor_type_name == "hdhld":
            return self.hdhld_store
        elif sensor_type_name == "accelerometer" or sensor_type_name.strip().lower() == "accel":
            return self.accel_store
        elif sensor_type_name == "gyroscope":
            return self.gyroscope_store
        elif sensor_type_name == "altimeter":
            return self.altimeter_store
        elif sensor_type_name == "ambientlight":
            return self.ambientlight_store
        elif sensor_type_name == "barometer":
            return self.barometer_store
        elif sensor_type_name == "calorie":
            return self.calorie_store
        elif sensor_type_name == "distance":
            return self.distance_store
        elif sensor_type_name == "uv":
            return self.uv_store
        elif sensor_type_name == "skintemperature":
            return self.skintemperature_store
        elif sensor_type_name == "gsr":
            return self.gsr_store
        elif sensor_type_name == "heartrate":
            return self.heartrate_store
        elif sensor_type_name == "rrinterval":
            return self.rrinterval_store
        else:
            return None
            
    def clear_sensor_bufferlist(self, sensor_type_name=None):
        if sensor_type_name == "hdhld":
            self.hdhld_store = []
        elif sensor_type_name == "accelerometer" or sensor_type_name.strip().lower() == "accel":
            self.accel_store = []
        elif sensor_type_name == "gyroscope":
            self.gyroscope_store = []
        elif sensor_type_name == "altimeter":
            self.altimeter_store = []
        elif sensor_type_name == "ambientlight":
            self.ambientlight_store = []
        elif sensor_type_name == "barometer":
            self.barometer_store = []
        elif sensor_type_name == "calorie":
            self.calorie_store = []
        elif sensor_type_name == "distance":
            self.distance_store = []
        elif sensor_type_name == "uv":
            self.uv_store = []
        elif sensor_type_name == "skintemperature":
            self.skintemperature_store = []
        elif sensor_type_name == "gsr":
            self.gsr_store = []
        elif sensor_type_name == "heartrate":
            self.heartrate_store = []
        else sensor_type_name == "rrinterval":
            self.rrinterval_store = []
        

def get_elapsed_time(in_starttime_in_second, in_end_time_in_second, in_process_step_name):
    """Return elapsed time including hours, minutes and seconds.
    Args:
       in_starttime_in_second (int): elapsed time in seconds since UNIX epoch time
       in_end_time_in_second (int): elasped time in seconds since UNIX epoch time
       in_process_step_name (str): Nmae of process
    Return:
        rdd
    """
    duration_in_seconds = in_end_time_in_second - in_starttime_in_second
    out_second = int(duration_in_seconds) % 60
    minutes_amount = int(duration_in_seconds) // 60
    out_minute = minutes_amount % 60
    hours_amount = minutes_amount // 60
    return "    Total elapsed time of {} : {} Hours {} Minutes {} Seconds.\n".format(in_process_step_name, hours_amount
                                                                                     , out_minute,out_second)
                                                                                         
if __name__ == '__main__':

    global kafka_consumer_list,valid_sensor_type_list, valid_kafka_topic_list, DISABLE_SENSOR_SET
    #kafka_consumer_list = dict()
    
    logging.addLevelName(logging.DEBUG-2, 'DEBUG_DETAILS') 
    # Logging, arguments pour fichier : filename='example.log', filemode='w'
    logging.basicConfig(level=logging.INFO, datefmt="%m-%d %H:%M:%S", format="P%(process)d|T%(thread)d|%(levelname)s|%(asctime)s | %(message)s")  
    # %(thread)d %(funcName)s L%(lineno)d
    
    logging.getLogger("requests").setLevel(logging.WARNING) 
    logging.getLogger("schedule").setLevel(logging.WARNING) 

    # -- PATH
    sys.path.append('./')
    logging.info("Starting from %s" % str(os.getcwd()))

    # -- Lancement
    config_parser = configparser.ConfigParser()
    config_parser.read('./edge2azure.ini')

    # config IoT hub
    logging.info("------ SENDING A MESSAGE TO THE IOT BUS ------")
    connectionString = config_parser.get(APP_NAME, "azure.connection.string")
    logging.info("connectionString is %s" % connectionString)

    #connectionString = 'HostName=msiothub2016.azure-devices.net;SharedAccessKeyName=iothubowner;
    #SharedAccessKey=GKXEAW1ksgIN4C1CASIVcDuHkI6V6KFD+0lxEU0gDZk='
    #logger.info(connectionString)

    deviceId = config_parser.get(APP_NAME, "azure.device.id")
    logging.info("deviceId is %s" % deviceId)

    #deviceId = 'healthc_rwriter'
    #logger.info(deviceId)
    # Initialize edge2azure sender
    d2cMsgSender = D2CMsgSender(connectionString)  
    # -- Create the Queue & Fill with tweets from RDB
    #queue_of_tweets = Queue.PriorityQueue(maxsize=0)
        
    perf0 = time.time()
    
    # start kafka client
    kafka_addr = config_parser.get(APP_NAME,"kafka.address")
    logging.info("kafka_addr is %s" % kafka_addr)
    kafka_port = config_parser.get(APP_NAME,"kafka.port")
    logging.info("kafka_port is %s" % kafka_port)
    
    Edge2AzureWorker.initProcessQueue()
    
    valid_sensor_type_list = [x.strip().lower() for x in config_parser.get(APP_NAME,"sensor.list").split(';')]
    print ("valid sensor type list is %s" % valid_sensor_type_list)
    
    valid_kafka_topic_list = [x.strip() for x in config_parser.get(APP_NAME,"kafka.topic").split(';')]
    print ("valid kafka topic list is %s" % valid_kafka_topic_list)
    # Construct instances of all subclasses of GenericSensor by SensorFactory.
    print ("==> Construct instances of all subclasses of GenericSensor for CURRENT partition")
    sensor_object_dict = dict()
    # Initialize buffer for all sensors
    bufferfactory = BufferFactory()
    # Initialize kafka for all sensors
    #KafkaFactory().create_kafka_consumer()
    kafka_consumer_list = KafkaConsumer("iot_msband2_heartRate",bootstrap_servers=['10.3.35.91:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')))
    logging.info(valid_kafka_topic_list)
    kafka_consumer_list.subscribe(topics=valid_kafka_topic_list)

    # create thread for all sensors
    try:
        # Before work
        for sensor_type_name in valid_sensor_type_list:
            
                # SensorFactory will create the json type for each sensor
                sensor_object_dict[sensor_type_name] = \
                SensorFactory().produce_sensor_object(sensor_type_name)

                # createThread will create thread for each sensor and start ListenForMsg
                Edge2AzureWorker.createThread()
        
        # Working
        logging.info("All Threads Created Successfully.")
        Edge2AzureWorker.waitFinished()
        #bufferfactory.send_msg_with_buffer()
        # t.join()
        logging.debug("==> Wait for interrupting by typing control+c")

        # t = [th.join() for th in threads]
        while True:
            time.sleep(1)
            logging.debug('listening for msg...'),
    except KeyboardInterrupt:
        sys.exit(2)		                
    #Edge2AzureWorker.closeProcessQueue()
    #perf1 = time.time() - perf0
    #logging.info("Analyzed %d tweets in %0.1f sec | Global Rate = %0.1f" % (nb_msg_in_queue, perf1, nb_msg_in_queue/perf1))

