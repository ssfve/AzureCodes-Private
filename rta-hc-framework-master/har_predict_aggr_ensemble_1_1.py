#coding=utf-8
## Imports
import time
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.regression import LabeledPoint
from ConfigParser import SafeConfigParser
from  bisect import bisect_left
import requests
import re
import os
from kafka import KafkaProducer
import rta_constants
import logging
import json
import sys
import random
import kafka
from cassandra.cluster import Cluster
import subprocess
import uuid
import pandas
import numpy
import pickle
from sklearn.preprocessing import Imputer
from pyspark import SQLContext

APP_NAME = "har_predict_aggr"
VERSION_VAL = "0.6.0_07062016"
SENSOR_TYPE = APP_NAME.split("_")[2]
# Must be retrieved from the models name in /tmp
USER_LIST = ["vincent.planat@hpe.com", "user2@hpe.com"]
# It's used to parameter of this script to specify which algorithm is applied to predict/classify.
VALID_ALGORITHM_LIST = ["voting", "randomforest", "naivebayes"]

logging.config.fileConfig(rta_constants.PROPERTIES_LOG_FILE)
logger = logging.getLogger(APP_NAME)


# vpl 26/05
# apply weight to majority voting
WGT_ACTIVITY_SITTING = 3
WGT_ACTIVITY_WALKING = 3
WGT_ACTIVITY_WALKING_UP = 0
WGT_ACTIVITY_WALKING_DOWN = 0


class HarPredictAggr:
    """
    Opened
     - Fix process_data() which is actually only processing the first rdd (line ~199).
         Whe should have data_payload = val_rdd.collect() and then for value in data_payload (exp of kairosDb_writer.py)

    Skeleton code for a processor
            This code is launched by the rta_manager.py.
            At init phase it starts its own controller thread
            Then it listens to kafka topic(s) (spark streaming) and executes control command
    """

    # For handling spark streamcontext.
    g_ssc = None
    g_sc = None
    g_valid_user_broadcast_list = None

    # g_user_data_model_dict = None

    def __init__(self, algorithm_mode):
        # used to store the processor state
        self.current_state = None
        # used by the buffer algorithm to remember when the last proc was done
        # updated each time the buffer is processed and cleaned.
        self.schedule_time = 0
        self.buffers_map = dict()  # {"sensorName":sensor_buffer_dict}

        logger.debug("initializing Processor");
        # load properties from the rta.properties (use the APP_NAME as a section name)
        config_parser = SafeConfigParser()
        config_parser.read(rta_constants.PROPERTIES_FILE)

        # check the consistency of the APP_NAME section in rta.properties
        if (not config_parser.has_section(APP_NAME)):
            logger.error(
                "properties file %s mal-formated. Missing section %s. Exit" % (rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(APP_NAME, "kafka.data.in.topics")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: kafka.data.in.topics in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(APP_NAME, "kafka.data.out.topic")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: kafka.data.out.topic in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(APP_NAME, "sensor_ref_timestamp")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: sensor_ref_timestamp in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(APP_NAME, "buffer_time_window")):
            logger.error("properties file %s mal-formated. Missing attribut: buffer_time_window in section %s. Exit" % (
                rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)

        if (not config_parser.has_section(rta_constants.FRAMEWORK_NAME)):
            logger.error("properties file %s mal-formated. Missing section %s. Exit" % (
                rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "kafka.host_port")):
            logger.error("properties file %s mal-formated. Missing attribut: kafka.host_port in section %s. Exit" % (
                rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "zookeeper.host_port")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: zookeeper.host_port in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "kafka.control.topic")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: kafka.control.topic in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "spark.nb_threads")):
            logger.error("properties file %s mal-formated. Missing attribut: spark.nb_threads in section %s. Exit" % (
                rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if (not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "spark.batch_duration")):
            logger.error(
                "properties file %s mal-formated. Missing attribut: spark.batch_duration in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "cassandra.cluster"):
            logger.error(
                "properties file %s mal-formated. Missing attribut: cassandra.cluster in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if not config_parser.has_option(rta_constants.FRAMEWORK_NAME, "cassandra.keyspace"):
            logger.error(
                "properties file %s mal-formated. Missing attribut: cassandra.keyspace in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, rta_constants.FRAMEWORK_NAME))
            sys.exit(-1)
        if not config_parser.has_option(APP_NAME, "cassandra.data.target.table"):
            logger.error(
                "properties file %s mal-formated. Missing attribut: cassandra.data.target.table in section %s. Exit" % (
                    rta_constants.PROPERTIES_FILE, APP_NAME))
            sys.exit(-1)
        logger.debug("config Processor OK");
 			
        # Get which algorithm is applied by argument: valid name list VALID_ALGORITHM_LIST: voting and randomforest
        self.algorithm_name = algorithm_mode

        # retrieve config parameters
        # vpl. With DirectStream we need to use the 9092 port (or 6667 on DEV)
        self.kafka_host_consumer = config_parser.get(rta_constants.FRAMEWORK_NAME, "zookeeper.host_port")
        self.kafka_host_producer = config_parser.get(rta_constants.FRAMEWORK_NAME, "kafka.host_port")
        self.kafka_group = APP_NAME
        self.kafka_ctrl_topic = config_parser.get(rta_constants.FRAMEWORK_NAME, "kafka.control.topic")
        self.kafka_data_in_topics = config_parser.get(APP_NAME, "kafka.data.in.topics").replace(" ", "").strip(';')
        self.kafka_data_out_topic = config_parser.get(APP_NAME, "kafka.data.out.topic")
        self.sensor_ref_timestamp = config_parser.get(APP_NAME, "sensor_ref_timestamp")
        self.buffer_time_window = int(config_parser.get(APP_NAME, "buffer_time_window"))
        self.spark_nb_threads = config_parser.get(rta_constants.FRAMEWORK_NAME, "spark.nb_threads")
        self.spark_batch_duration = config_parser.get(rta_constants.FRAMEWORK_NAME, "spark.batch_duration")
        self.cluster = config_parser.get(rta_constants.FRAMEWORK_NAME, "cassandra.cluster")
        self.keyspace = config_parser.get(rta_constants.FRAMEWORK_NAME, "cassandra.keyspace")
        self.cassandra_target_table = config_parser.get(APP_NAME, "cassandra.data.target.table")
        self.karios_db_rest_url = config_parser.get(rta_constants.FRAMEWORK_NAME, "kairosdb.url")
        self.karios_db_post_size = config_parser.get(rta_constants.FRAMEWORK_NAME, "kairosdb.post.size")
        # add project folder name
        self.project_folder = config_parser.get(rta_constants.SKLEARN,'project_models_folder')

        logger.debug("Initialize Kafka Producer:%s" %self.kafka_host_producer);
        # kafka client producer (for sending back data result - not control). Used in process_data()
        self.data_producer = KafkaProducer(bootstrap_servers=self.kafka_host_producer)
        logger.debug("Kafka Producer OK");

        # It contains a list of data points to be sent to Karios database. Each data points must have below JSON format
        # {"name": "har_prediction","timestamp": 1461747206024,"value": 0,"tags": {"deviceid": "b81e905908354542",
        #   "userid": vincent.planat@hpe.com, "device_type": "msband2"}
        self.karrios_data_points_list = []

        # Check data models in HDFS
        logger.info("==> Check if data model folders exist in HDFS and get data model paths and valid user list")
        global process_start_time,os
        try:
		#12/27/2016 commit by JOJO
	    PROJECT_FOLDER = config_parser.get(rta_constants.SKLEARN,'project_models_folder')
            # PROJECT_FOLDER = '/opt/mount1/rta-hc/integration/rta-hc-framework-master/data-models'
            if not(os.path.exists(PROJECT_FOLDER)):
                logger.error("    Data model home path(%s) DOES NOT exist.", PROJECT_FOLDER)
                sys.exit(-1)
            # cmd_1 = subprocess.Popen(["cd",PROJECT_FOLDER],shell=True,stdout=subprocess.PIPE)
            os.chdir(PROJECT_FOLDER)
            ls_data_model = subprocess.Popen(["ls"],shell=True,stdout=subprocess.PIPE)
            (ls_data_model_result,err) = ls_data_model.communicate()
            logger.debug('model path list is here**************')
            logger.debug(ls_data_model_result)
            self.user_id_set, user_sensor_type_2_data_model_path = HarPredictAggr.parse_user_data_model_for_sk(PROJECT_FOLDER,ls_data_model_result)
            logger.info("    Data models in file system(%s) are available, and model name and its user ID are "
                                "parsed SUCCESSFULLY!", PROJECT_FOLDER)
            logger.debug('self.user_id_set is here############')
            logger.debug(self.user_id_set)
            logger.debug('sensor is here############')
            logger.debug(user_sensor_type_2_data_model_path)
            user_data_model_dict = dict()
		# ELVIN COMMENT 12/23
            #model_extension_pattern = ur'''(\.mdl)|(\.MDL)|(\.Mdl)'''
            #self.model_extension_pattern_obj = re.compile(model_extension_pattern)
            '''
			ls_data_model = subprocess.Popen(["hadoop", "fs", "-ls", rta_constants.DATA_MODEL_HOME_PATH],
                                             stdout=subprocess.PIPE)
            (ls_data_model_result, err) = ls_data_model.communicate()
            if ls_data_model_result == "":
                logger.error("    Data model home path(%s) DOES NOT exist.", rta_constants.DATA_MODEL_HOME_PATH)
                sys.exit(1)
            else:
                self.user_id_set, self.user_sensor_type_2_data_model_path = HarPredictAggr.parse_user_data_model(
                    ls_data_model_result, self.model_extension_pattern_obj)
                logger.info("    Data models in HDFS(%s) are available, and model name and its user ID are "
                            "parsed SUCCESSFULLY!", rta_constants.DATA_MODEL_HOME_PATH)
			'''
 

            # initialize the spark Context
            logger.debug("initializing Processor spark context")
            conf = SparkConf().setAppName(APP_NAME)
            # On yar cluster the deploy mode is directly given by the spark-submit command.
            #    spark-submit --master yarn --deploy-mode cluster
            # conf = conf.setMaster("local[*]")
            self.sc = SparkContext(conf=conf)

            logger.debug("initializing kafka stream context for sensors data");
            HarPredictAggr.g_ssc = StreamingContext(self.sc, int(self.spark_batch_duration))

            # Get valid user list and broadcast across Spark worker
            logger.info("==> Get valid user list and broadcast acroos Spark workers ...")
            self.user_list = self.sc.broadcast(list(self.user_id_set))
            global valid_user_broadcast_list
            valid_user_broadcast_list = self.user_list
            logger.debug("    valid user list: %s", self.user_list.value)

 		
            # Load data models built offline in HDFS, and Build a dictionary which contains User id and its data model
            #logger.info("==> Load data models built offline in HDFS "
                        #"and build dictionary with user ID and its data model ...")
            #self.user_data_model_dict = dict()
		

	    # VPL UPDATE 09/11
	    '''
            for u in self.user_id_set:
                try:
                    dm = self.user_sensor_type_2_data_model_path[(u, "aggregation")]
                except KeyError:
                    logger.warn("    There is NO data model in % for user ID: %s and sensor type: %s !!",
                                rta_constants.DATA_MODEL_HOME_PATH, u, "aggregation")
                else:
                    self.data_model = RandomForestModel.load(self.sc, dm)
		    print ('VPL--- load RF:%s' % dm)
                    self.user_data_model_dict[u] = self.data_model
            # HarPredictAggr.g_user_data_model_dict = self.user_data_model_dict
            if len(self.user_data_model_dict) == 0:
                logger.error("    There is NO data model in % for user ID list: %s and sensor type: %s !! EXIT",
                             rta_constants.DATA_MODEL_HOME_PATH, u, "aggregation")
                sys.exit(-1)
            else:
                logger.debug("    There is(are) total %d data model(s).", len(self.user_data_model_dict))
	    '''
	    # VPL UPDATE

            '''
            kafka Stream configuration for data input
                connect to the kafka topics (ctrl and data)
                Split topics into a dict e.g. {'topic1': 1, 'topic2': 1}
                add the control topic
            '''
            kafka_in_topics = dict(
                zip(self.kafka_data_in_topics.split(";"), [1] * len(self.kafka_data_in_topics.split(";"))))
            kafka_in_topics[self.kafka_ctrl_topic] = 1
            # kafka_stream = KafkaUtils.createStream(HarPredictAggr.g_ssc, self.kafka_host_consumer, self.kafka_group,
            #                                        kafka_in_topics).map(lambda x: x[1])
            kafka_stream = KafkaUtils.createDirectStream(HarPredictAggr.g_ssc, kafka_in_topics.keys(),
                                                         {"metadata.broker.list": self.kafka_host_producer})
            lines = kafka_stream.map(lambda x: x[1])
            lines.foreachRDD(lambda rdd: self.process_data(rdd))
            logger.debug("buffer_time_window:%d kafka consuler host:%s  group:%s topics:%s" % (
                self.buffer_time_window, self.kafka_host_consumer, self.kafka_group, kafka_in_topics));

            # self.current_state = rta_constants.RTA_PROCESSOR_STATE_INITIALIZED
            logger.debug("skip the rta_manager and set the PROCESSOR_STATE to STARTED")
            self.current_state = rta_constants.RTA_PROCESSOR_STATE_STARTED

            # start the kafka stream. Both ctrl and data are sequenced at the same rythm
            HarPredictAggr.g_ssc.start()
            HarPredictAggr.g_ssc.awaitTermination()
        except KeyboardInterrupt:
            logger.debug("    %s\n    %s" % ("Skip", get_elapsed_time(process_start_time, time.time(), APP_NAME)))
        except Exception as e:
            process_end_time = time.time()
            logger.error("    %s\n    %s" %(e.message, get_elapsed_time(process_start_time, process_end_time, APP_NAME)))
    def on_get_status(self, query_id):
        """
            send back to the control_msg topic the current status of this processor
        """
        logger.debug("get status query: %s" % self.current_state)
        return_val = "{\"control_msg\":\"get_processor_status\",\"processor_name\" : \"" + APP_NAME + "\",\"query_id\" : \"" + str(
            query_id) + "\", \"answer\" : \"" + self.current_state + "\"}"

        self.data_producer.send(self.kafka_ctrl_topic, return_val)
        return

    def on_start(self, query_id):
        """
            Method invoked by the ProcessorControler on reception of a start order on the control kafka topic
        """
        if not self.check_state_change(rta_constants.RTA_PROCESSOR_ORDER_START):
            logger.error("State transition forbidden. From %s to START" % self.current_state)
            return

        logger.debug("processor start")
        self.current_state = rta_constants.RTA_PROCESSOR_STATE_STARTED
        return

    def on_pause(self, query_id):
        """
            Method invoked by the ProcessorControler on reception of a pause order on the control kafka topic
            We cannot stop the streaming as it is also receiving control_msg
            As a consequence the process data will check the status and if on Paused then do not process the data
        """
        if not self.check_state_change(rta_constants.RTA_PROCESSOR_ORDER_PAUSE):
            logger.error("State transition forbidden. From %s to PAUSE" % self.current_state)
            return

        logger.debug("processor pause")
        self.current_state = rta_constants.RTA_PROCESSOR_STATE_PAUSED
        return

    def on_stop(self, query_id):
        """
            Method invoked by the ProcessorControler on reception of a stop order on the control kafka topic
        """
        if not self.check_state_change(rta_constants.RTA_PROCESSOR_ORDER_STOP):
            logger.error("State transition forbidden. From %s to STOP" % self.current_state)
            return

        logger.debug("processor stop")
        self.current_state = rta_constants.RTA_PROCESSOR_STATE_STOPPED
        return

    def on_destroy(self, query_id):
        """
            Method invoked by the ProcessorControler on reception of a destroy order on the control kafka topic
        """
        if not self.check_state_change(rta_constants.RTA_PROCESSOR_ORDER_DESTROY):
            logger.error("State transition forbidden. From %s to DESTROY" % self.current_state)
            return

        logger.debug("processor destroy")
        self.current_state = rta_constants.RTA_PROCESSOR_STATE_DESTROYED

        # Ending context.
        # HarAlgoSensor.g_ssc.stop(True, True)
        HarPredictAggr.g_ssc.stop(True, False)
        return

    def get_spark_session_instance(self,spark_context):
        if ('sqlContextSingletonInstance' not in globals()):
            globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
        return globals()['sqlContextSingletonInstance']

    def predict_naivebayes(self, rdd, userid,specific_user_data):
        sqlContext = self.get_spark_session_instance(rdd.context)
        logger.debug("Starting to naivebayes prediction....")
        rdd_data = rdd.context.parallelize(specific_user_data)
        rdd_acce = rdd_data.filter(lambda x:x['processor_name']=='har_randomforest_accelerometer')
        rdd_gyro = rdd_data.filter(lambda x:x['processor_name']=='har_randomforest_gyroscope')
        rdd_baro = rdd_data.filter(lambda x:x['processor_name']=='har_randomforest_barometer')
        if not rdd_acce.isEmpty():
            df_acce  = sqlContext.createDataFrame(rdd_acce)
            accpred = df_acce.toPandas()
            accpred.columns = ['eventid','processor_name','Accpred','window','userid']
            accpred = pandas.DataFrame(accpred,columns=['Accpred','window','processor_name'])
        else:
            accpred = pandas.DataFrame({"eventid":[],"processor_name":[],"userid":[],"Accpred":[],"window":[]})
            accpred = pandas.DataFrame(accpred,columns=['Accpred','window','processor_name'])

        if not rdd_gyro.isEmpty():
            df_gyro  = sqlContext.createDataFrame(rdd_gyro)
            gyrpred = df_gyro.toPandas()
            gyrpred.columns = ['eventid','processor_name','Gyrpred','window','userid']
            gyrpred = pandas.DataFrame(gyrpred,columns=['Gyrpred','window','processor_name'])
        else:
            gyrpred = pandas.DataFrame({"eventid":[],"processor_name":[],"userid":[],"Gyrpred":[],"window":[]})
            gyrpred = pandas.DataFrame(gyrpred,columns=['Gyrpred','window','processor_name'])

        if not rdd_baro.isEmpty():
            df_baro  = sqlContext.createDataFrame(rdd_baro)
            barpred = df_baro.toPandas()
            barpred.columns = ['eventid','processor_name','Barpred','window','userid']
            barpred = pandas.DataFrame(barpred,columns=['Barpred','window','processor_name'])
        else:
            barpred = pandas.DataFrame({"eventid":[],"processor_name":[],"userid":[],"Barpred":[],"window":[]})
            barpred = pandas.DataFrame(barpred,columns=['Barpred','window','processor_name'])

        # print barpred
        # print gyrpred
        # print accpred
        # df_acce  = sqlContext.createDataFrame(rdd_acce)
        # df_gyro  = sqlContext.createDataFrame(rdd_gyro)
        # df_baro  = sqlContext.createDataFrame(rdd_baro)
        
        testData = pandas.concat([df.set_index(['window']) for df in [accpred, gyrpred, barpred]],axis=1).reset_index()
        # testData.dropna(subset = ['window'])
        # data = [accpred,gyrpred,barpred]
        # testData = pandas.concat(data)
        # testData = pandas.concat(data, keys=['window'])
        logger.debug("Trying to convert the feature data as dataframe.....")
        # print testData

        dataFeat = testData[['Accpred', 'Gyrpred', 'Barpred']]
        dataWindow = testData['window']
        dataFeat.loc[dataFeat.isnull().all(axis=1), :] = dataFeat.ffill()
        #IF THERE ARE ANY EMPTY CELLS (NaN) IN ROW, REPLACE WITH 'Z'.
        dataFeat.fillna('Z', inplace=True)
        # print("dataFeat is here####")
        # print(dataFeat)
        PROJECT_FOLDER = self.project_folder
        # print (PROJECT_FOLDER)
        #LOAD  LabelEncoder
        LeFilename = 'sk_aggr_'+str(userid)+'_labelEncoder'
        Lmodel = pickle.load(open(PROJECT_FOLDER+'/'+LeFilename, 'rb'))

        dataFeatIndex = dataFeat.apply(Lmodel.transform)
        #MISSING CELLS, NOW LABELLED AS '4' NEED TO BE FILLED USING ANOTHER VALUE
        #REPLACE EMPTY CELL WITH MOST FREQUENT VALUE
        imp = Imputer(missing_values=4, strategy= 'most_frequent', axis=1)
        featDataFilled = imp.fit_transform(dataFeatIndex)
        #LOAD OneHotEncoder
        oheFilename = 'sk_aggr_'+str(userid)+'_ohEncoder'
        oheModel = pickle.load(open(PROJECT_FOLDER+'/'+oheFilename, 'rb'))
        dataFeatOhe = oheModel.transform(featDataFilled)
        mnbFilename = 'sk_aggr_'+str(userid)+'_mnbModel'
        logger.debug('Loading the aggeration model file from %s ',PROJECT_FOLDER+'/'+mnbFilename)
        mnbModel = pickle.load(open(PROJECT_FOLDER+'/'+mnbFilename, 'rb'))
        prediction = pandas.DataFrame(mnbModel.predict(dataFeatOhe))
        prediction.columns = ['prediction']
        prediction_result = pandas.concat([prediction, dataWindow], axis=1)
        prediction_result['userid'] = userid
        prediction_result['eventId'] = [str(uuid.uuid4()) for i in range(len(prediction_result.index))]
        logger.debug('Gathering the prediction result....')
        # print prediction_result
        return prediction_result

    # process_rt_data_naivebayes
    def process_rt_data_4_naivebayes(self, val_rdd):
        # print 'var_rdd is here##########'
        # print (val_rdd.collect())
        # rdd = val_rdd.map(lambda x: json.loads(x))
        rdd = val_rdd.map(lambda x:(x['userid'],[x]))
        # print (rdd.collect())
        def filter_items(x ,y):
            return x+y
        rdd_by_user_id = rdd.reduceByKey(filter_items)
        # print (rdd_by_user_id.collect())
        # print 'after rdd to dict#####'
        # print (rdd_by_user_id.map(lambda x:{x[0]:x[1]}).collect())
        ls = rdd_by_user_id.map(lambda x:{x[0]:x[1]}).collect()
        # print ls
        return ls

    def process_data(self, val_rdd):
        """
            this method is in charge of processing any rdd. We have 2 types of rdd
                - control rdd (on the control_msg topic)
                - data rdd
            1) check if rdd is empty
            2) check if rdd for control or not
        """
        global valid_user_broadcast_list, VALID_ALGORITHM_LIST, APP_NAME, process_start_time
        logger.debug("    %s" %(get_elapsed_time(process_start_time, time.time(), "predict aggregation sensor "
                                                                                  "activities")))
        if val_rdd.isEmpty():
            logger.debug("empty val rdd")
            return

        # check if the rdd is for the controler or not.
        try:
            # We are using now CreateDirectStream which does not return the same format of rdd
            # val_msg_json = json.loads(val_rdd.take(1)[0][1])
            logger.debug("First record in RDD: %s", val_rdd.take(1))
            val_msg_json = json.loads(val_rdd.take(1)[0])
        except ValueError:
            logger.debug("val rdd is not valid JSON. No processing to do")
            return

        # Get all control messages in CURRENT RDD in case there are control messages and sensor data messages in the RDD
        ctrl_rdd = val_rdd.filter(lambda x: "control_msg" in json.loads(x))
        # Get all sensor data messages whose userid is valid user id in the list
        # print ("user_lis is here#####")
        # print (valid_user_broadcast_list.value)
        user_data_rdd = val_rdd.filter(lambda x: x != "").map(lambda x:json.loads(x))
        # .filter(lambda x: x["userid"] in valid_user_broadcast_list.value)
        # logger.debug("There are %d records will be processing! ",user_data_rdd.count())
        # print (user_data_rdd.collect())

        if not ctrl_rdd.isEmpty():
            self.process_ctrl_cmd(val_rdd)
        else:
            logger.debug("There is no control message in this RDD. ")

        if not user_data_rdd.isEmpty():
            user_data_counts = user_data_rdd.count()
            if not self.current_state == rta_constants.RTA_PROCESSOR_STATE_STARTED:
                logger.debug("Not in STARTED STATE. Cannot proces data")
                return
            logger.debug("There is(are) %d records with valid user list %s in current RDD.",
                         user_data_counts, self.user_id_set)
            # format data for naivebayes prediction
            if self.algorithm_name == VALID_ALGORITHM_LIST[2]:
                """ Sample of selected_data
                selected_data_set:[{u'vincent.planat@hpe.com': 
                [{u'eventId': u'c748d758-c31b-49ae-a36b-267e3d990801', u'processor_name': u'har_randomforest_accelerometer', u'userid': u'vincent.planat@hpe.com', u'result': u'SETTING', u'time_stamp': 1472554191789}, 
                {u'eventId': u'c748d758-c31b-49ae-a36b-267e3d990801', u'processor_name': u'har_randomforest_accelerometer', u'userid': u'vincent.planat@hpe.com', u'result': u'SETTING', u'time_stamp': 1472554191790}, 
                {u'eventId': u'c748d758-c31b-49ae-a36b-267e3d990801', u'processor_name': u'har_randomforest_accelerometer', u'userid': u'vincent.planat@hpe.com', u'result': u'SETTING', u'time_stamp': 1472554191793}]}]
                """
                selected_data = self.process_rt_data_4_naivebayes(user_data_rdd)
                # print "selected data is here#####"
                # print (selected_data)
            else:
                selected_data = self.process_rt_data(user_data_rdd)

            # vpl 19/05/2016
            if (selected_data == -1):
                logger.debug("Nothing to process in process_data. Return")
                return

            if self.algorithm_name == VALID_ALGORITHM_LIST[2]:
                for item in selected_data:
                    for k,v in item.items():        
                        predict_result = self.predict_naivebayes(val_rdd,k,v)
                        # Create connection object for writing data to Kafka queue
                        producer = kafka.KafkaProducer(bootstrap_servers=[self.kafka_host_producer])
                        # Create connection object for writing data to Cassandra database
                        # vpl 31/05/2016
                        try:
                            cluster_object = Cluster([self.cluster])
                            session = cluster_object.connect(self.keyspace)
                        except Exception, e:
                                logger.error("Encounter error in retrieving Cassandra cluster reference %s" % str(e))     
                        logger.debug("There are %d records ready to store in database...",len(predict_result.index))
                        for index, row in predict_result.iterrows():
                            predicted_result_record_4_kafka = {"eventId": row["eventId"],
                                                               "processor_name": APP_NAME, "userid": row['userid'],
                                                               "time_stamp": row["window"],
                                                               "activity": row['prediction']}
                            predicted_result_record_4_cassandra_insert_query = "INSERT INTO " + self.cassandra_target_table + \
                                                                               "(event_id, user_id, time_stamp, activity) " \
                                                                               "VALUES (?, ?, ?, ?)"
                            predicted_result_record_4_cassandra_value_list = (uuid.UUID(row["eventId"]), row['userid'],
                                                                              row["window"], row['prediction'])
                            final_predict_result_number = HarPredictAggr.map_predict_result_string_2_number(row['prediction'])                                               
                            self.append_karios_data_points_sklearn(self.cassandra_target_table, row, final_predict_result_number,row['userid'])
                            logger.debug("Inserting prediction record into karrios....")
                            self.issue_insert_query_karrios(requests, json.dumps, self.karios_db_rest_url,
                                                            self.karios_db_post_size)
                            # logger.debug("INSERT QUERY for Cassandra: %s", predicted_result_record_4_cassandra_insert_query)
                            # print ('output topic is###')
                            # print (self.kafka_data_out_topic)
                            try:
                                future = producer.send(self.kafka_data_out_topic, json.dumps(predicted_result_record_4_kafka))
                            except Exception, e:
                                logger.error("Encounter error in sending result to kafka queue %s" % str(e))
                            try:
                                logger.debug('inserting data into cassandra table #'+self.cassandra_target_table)
                                logger.debug('inserted data primary key is '+row["eventId"])
                                prepared = session.prepare(predicted_result_record_4_cassandra_insert_query)
                                session.execute(prepared,predicted_result_record_4_cassandra_value_list)
                                # session.execute_async(predicted_result_record_4_cassandra_insert_query,
                                #                       predicted_result_record_4_cassandra_value_list)
                            except Exception, e:
                                logger.error("Encounter error in sending result to cassandra DB %s" % str(e))
                        # Force script to send rest of data points in self.karrios_data_points_list.
                    self.issue_insert_query_karrios(requests, json.dumps, self.karios_db_rest_url, self.karios_db_post_size,
                                                    True)
                    try:
                        producer.close()
                        session.shutdown()
                    except Exception, e:
                        logger.error("Encounter error in closing Cassandra cluster reference %s" % str(e))

            else:
                """ Sample of selected_data
                selected_data_set:{'user2@hpe.com':
                [[{'eventId': u'8f2d48c2-72ac-4686-93f7-a1fefc927598', 'processor_name': u'har_randomforest_gyroscope', 'userid': u'user2@hpe.com', 'result': u'SITTING', 'time_stamp': 1461630615817}],
                [{'eventId': u'd1d2034c-e9c3-4f69-96c6-d85a24d5c65a', 'processor_name': u'har_randomforest_accelerometer', 'userid': u'user2@hpe.com', 'result': u'SITTING', 'time_stamp': 1461630617850}]]
                }
                """
                for u in selected_data.keys():
                    logger.debug("Current user: %s in this RDD", u)
                    logger.debug("Current events: %s", selected_data[u])
                    if self.algorithm_name == VALID_ALGORITHM_LIST[0]:
                        # Voting algorithm
                        final_predict_result = HarPredictAggr.simulated_prediction(selected_data[u])
                        final_predict_result_number = HarPredictAggr.map_predict_result_string_2_number(
                            final_predict_result)
                        logger.debug("predicted_result with voting algorithm : %s", final_predict_result)
                    elif self.algorithm_name == VALID_ALGORITHM_LIST[1]:
                        # Means random forest algorithm should be applied
                        ensemble_input_list = HarPredictAggr.get_vector_4_ensemble(selected_data[u])
                        logger.debug("ensemble input list: %s", ensemble_input_list)
                        ensemble_input_rdd = self.sc.parallelize([ensemble_input_list])
                        ensemble_input_features = ensemble_input_rdd.map(lambda x:
                                                                         LabeledPoint(0, x)).map(lambda x: x.features)
                        final_predict_result_number = self.user_data_model_dict[u].predict(
                            ensemble_input_features).collect()[0]
                        final_predict_result = HarPredictAggr.map_predict_result_number_2_string(
                            final_predict_result_number)
                        logger.debug("predicted result with random forest algorithm: %s", final_predict_result)
                    else:
                        return

                # Create connection object for writing data to Kafka queue
                producer = kafka.KafkaProducer(bootstrap_servers=[self.kafka_host_producer])
                # Create connection object for writing data to Cassandra database
                # vpl 31/05/2016
                try:
                    cluster_object = Cluster([self.cluster])
                    session = cluster_object.connect(self.keyspace)
                except Exception, e:
                        logger.error("Encounter error in retrieving Cassandra cluster reference %s" % str(e))     
                        
                # loop each event in selected_data
                for event in selected_data[u]:
                    predicted_result_record_4_kafka = {"eventId": event[0]["eventId"],
                                                       "processor_name": APP_NAME, "userid": u,
                                                       "time_stamp": event[0]["time_stamp"],
                                                       "activity": final_predict_result}
                    predicted_result_record_4_cassandra_insert_query = "INSERT INTO " + self.cassandra_target_table + \
                                                                       "(event_id, user_id, time_stamp, activity) " \
                                                                       "VALUES (%s, %s, %s, %s)"
                    predicted_result_record_4_cassandra_value_list = [uuid.UUID(event[0]["eventId"]), u,
                                                                      event[0]["time_stamp"], final_predict_result]
                    self.append_karios_data_points(self.cassandra_target_table, event, final_predict_result_number, u)
                    self.issue_insert_query_karrios(requests, json.dumps, self.karios_db_rest_url,
                                                    self.karios_db_post_size)
                    # logger.debug("INSERT QUERY for Cassandra: %s", predicted_result_record_4_cassandra_insert_query)

                    try:
                        future = producer.send(self.kafka_data_out_topic, json.dumps(predicted_result_record_4_kafka))
                    except Exception, e:
                        logger.error("Encounter error in sending result to kafka queue %s" % str(e))
                    try:
                        session.execute_async(predicted_result_record_4_cassandra_insert_query,
                                              predicted_result_record_4_cassandra_value_list)
                    except Exception, e:
                        logger.error("Encounter error in sending result to cassandra DB %s" % str(e))
                # Force script to send rest of data points in self.karrios_data_points_list.
                self.issue_insert_query_karrios(requests, json.dumps, self.karios_db_rest_url, self.karios_db_post_size,
                                                True)
                try:
                    producer.close()
                    session.shutdown()
                except Exception, e:
                    logger.error("Encounter error in closing Cassandra cluster reference %s" % str(e))     

        else:
            logger.debug("There is NO sensor record with valid user list %s in current RDD.",
                         valid_user_broadcast_list.value)
        return

    def process_rt_data(self, val_rdd):
        """
            update the buffers with rdd content
            1) check if schedule_time == 0 meaning that this is the first time we process anything
                if Yes then set schedule_time to the oldest event timestamp of buffer identified by  sensor_ref_timestamp (rta.properties)

            2) Select the buffer with the smallest nb of event (lowest frequency) ==> lowest_freq_buffer

            3) (While ...) process the buffer as long as we find something to do
                In lowest_freq_buffer do we have events betweens scheduletime & scheduletime + self.buffer_time_window ?
                (No) exit the While
                (Yes)
                    Scan all the buffer and update the selected_data_set with the value of each buffer
                    This include buffer cleanup with data older than newest_ts_lowfreq_buffer (same time ref for all buffers)
                Move the schedule_time ahead: buffer_time_window
        """
        # result_data_set will contain the set of data, in each buffer, whose timestamp is the closest to schedule_time
        # {"userId":[event,event], "userId":[event,event]}
        #   event is: {[eventid,deviceType,deviceId ]
        result_data_set = dict()

        # vpl 05/05/2016
        last_buffers_ts = dict()
        current_buffers_ts = dict()

        # update the buffers_map with the rdd content (several json sensor msgs)
        self.update_buffers(val_rdd)

        # vpl 05/05/2016
        # We need to align the schedule_time to
        # the the oldest event timestamp of buffer identified by  sensor_ref_timestamp (rta.properties)
        # if (self.schedule_time == 0):

        # vpl 19/05/2016        
        # If update buffer did not buffer anything for this sensor then return
        # 02/06/2016
        '''
        if (not self.buffers_map.has_key(self.sensor_ref_timestamp)):
            logger.debug("update_buffer did not buffer anything for sensor ref ts:%s. Return" % self.sensor_ref_timestamp)
            return -1

        sensor_sorted_ts = sorted(self.buffers_map[self.sensor_ref_timestamp].keys())
        self.schedule_time = sensor_sorted_ts[0]
        logger.debug("initialize schedule_time:%d . Reference sensor is:%s" % (
            self.schedule_time, self.sensor_ref_timestamp))

        # what is the buffer with the smallest nb of event (lowest frequency) ==> lowest_freq_buffer
        # I will us it as a time reference for schedule_time with buffer_time_window increment
        # buffers_size is a dictionary {size-of-bufferName:bufferName}
        buffers_size = {len(self.buffers_map[buffer_name]): buffer_name for buffer_name in self.buffers_map}
        buffers_size_sorted = sorted(buffers_size.keys())
        lowest_freq_buffer_name = buffers_size[buffers_size_sorted[0]]
        lowest_freq_buffer = self.buffers_map[lowest_freq_buffer_name]
        logger.debug("The lowest freq buffer is:%s   with size:%d" % (lowest_freq_buffer_name, len(lowest_freq_buffer)))
        '''

        # for userId in USER_LIST:
        for userId in self.user_id_set:
            # 02/06/2016
            # start update MChg
            logger.debug("sensor ref timestamp is %s ",self.sensor_ref_timestamp)
            logger.debug("self buffers_map is %s",self.buffers_map)
            if (not self.sensor_ref_timestamp in self.buffers_map):
                logger.info("sensor:%s is not present on traffic plan. Exit the processing loop" % self.sensor_ref_timestamp)
                return -1
        
        
            tmp_buffer_val = dict((k,v) for k,v in self.buffers_map[self.sensor_ref_timestamp].items() if v[2] == userId)            
            sensor_sorted_ts = sorted(tmp_buffer_val.keys())
            if (len(sensor_sorted_ts) == 0):         
                logger.debug("update_buffer did not buffer anything for sensor ref ts:%s for user:%s. Return" % 
                    (self.sensor_ref_timestamp,userId))
                continue
            
            self.schedule_time = sensor_sorted_ts[0]
            logger.debug("initialize schedule_time:%d . Reference sensor is:%s for user:%s" % (
                self.schedule_time, self.sensor_ref_timestamp,userId))
    
            buffers_size = {}
            for buffer_name in self.buffers_map:
                tmp_buffer_val = dict((k,v) for k,v in self.buffers_map[buffer_name].items() if v[2] == userId)
                buffers_size[len(tmp_buffer_val)] = buffer_name
            buffers_size_sorted = sorted(buffers_size.keys())
            lowest_freq_buffer_name = buffers_size[buffers_size_sorted[0]]
            lowest_freq_buffer = dict((k,v) for k,v in self.buffers_map[lowest_freq_buffer_name].items() if v[2] == userId)
            logger.debug("The lowest freq buffer is:%s   with size:%d" % (lowest_freq_buffer_name, len(lowest_freq_buffer)))
            # end update MChg
            
            # process the buffer as long as we find something to do
            # we have to loop through the different users

            # filter by userId
            logger.debug("processing user:%s" % userId)
            lowest_freq_userid_buffer = {i: lowest_freq_buffer[i] for i in lowest_freq_buffer if
                                         lowest_freq_buffer[i][2] == userId}
            if (len(lowest_freq_userid_buffer) == 0):
                logger.debug(
                    "the buffer filtering based userId:%s resulted into a empty buffer. Skip and go to next userId" % userId)
                continue
            buffer_processing = True
            while (buffer_processing == True):
                # In lowest_freq_userid_buffer do we have events betweens scheduletime & self.buffer_time_window ?
                # filter by userId
                sorted_ts = sorted(lowest_freq_userid_buffer.keys())
                scan_ts = long(self.schedule_time + self.buffer_time_window)
                newest_ts_lowfreq_buffer = self.find_closest_ts(sorted_ts, scan_ts, rta_constants.CLOSEST_CHOICE_BACKW)
                logger.debug("find the closest timestamp of:%d in lowest_freq_buffer_name:%s.  Result:%d" % (
                    scan_ts, lowest_freq_buffer_name, newest_ts_lowfreq_buffer))
                if (newest_ts_lowfreq_buffer == -1):
                    # This is not the time to process anything..
                    # so we return and wait the next buffer update
                    buffer_processing = False
                    logger.debug(
                        "The timestamp:%d used to search for event in buffer is above the lowest_freq_buffer_name:%s. So nothing to process" % (
                            scan_ts, lowest_freq_buffer_name))

                else:
                    # Scan all the buffer and update the result_data_set with the value of each buffer
                    for buffer_name in self.buffers_map:
                        buffer_val = self.buffers_map[buffer_name]
                        # filter by user_id
                        buffer_val = {i: buffer_val[i] for i in buffer_val if buffer_val[i][2] == userId}
                        if len(buffer_val) != 0:
                            sorted_ts_buffer = sorted(buffer_val.keys())
                            # http://integers.python.codesolution.site/57044-5673-from-list-integers-get-number-closest-given-value.html)
                            closest_ts = self.find_closest_ts(sorted_ts_buffer, newest_ts_lowfreq_buffer,
                                                              rta_constants.CLOSEST_CHOICE_BACKW_FORW)
                            if (closest_ts != -1):
                                # vpl 01/06/2016
                                #if (len(result_data_set) > 0):
                                if (userId in result_data_set):
                                    if (len(result_data_set[userId]) > 0):
                                        list_selected_event = result_data_set[userId]
                                else:
                                    list_selected_event = []

                                list_selected_event.append(buffer_val[closest_ts])
                                result_data_set[userId] = list_selected_event

                                # vpl 05/05/2016
                                # buffer_processing = False

                                # cleanup  buffer. We remove the data that are older than newest_ts_lowfreq_buffer
                                # create a new dictionary with cleaned data
                                closest_ts = self.find_closest_ts(sorted_ts, self.schedule_time,
                                                                  rta_constants.CLOSEST_CHOICE_BACKW)
                                new_buffer_val = {i: buffer_val[i] for i in buffer_val if
                                                  (i > newest_ts_lowfreq_buffer and buffer_val[i][2] == userId)}

                                # vpl 05/05/2016
                                current_buffers_ts[buffer_name] = newest_ts_lowfreq_buffer
                                logger.debug(
                                    "cleanup done: buffer:%20s previous-size:%d     current-size:%d for time_stamp:%d" % (
                                        buffer_name, len(buffer_val), len(new_buffer_val), newest_ts_lowfreq_buffer))
                                        
                                # vpl 31/05/2016
                                buffer_val_cleanup = self.buffers_map[buffer_name]
                                logger.debug(
                                    "cleanup reference buffers_map. For sensor:%s Original size:%d" % (buffer_name,len(buffer_val_cleanup)))
                                new_buffer_val_cleanup = {i: buffer_val_cleanup[i] for i in buffer_val_cleanup if
                                        ((buffer_val_cleanup[i][2] != userId) or
                                        (i > newest_ts_lowfreq_buffer and buffer_val_cleanup[i][2] == userId))
                                }
                                logger.debug(
                                    "cleanup reference buffers_map. For sensor:%s Updated size:%d" % (buffer_name,len(new_buffer_val_cleanup)))
                                self.buffers_map[buffer_name] = new_buffer_val_cleanup
                                
                            else:
                                logger.debug("the timestamp:%d is above the buffer:%s. Nothing to retrieve. Skip" % (
                                    newest_ts_lowfreq_buffer, buffer_name))
                        else:
                            logger.debug("This buffer %s is empty. Skip it" % buffer_name)

                    # vpl 05/05/2016
                    # check if the current_buffers_ts = last_buffers_ts
                    logger.debug("current_buffers_ts:%s" % current_buffers_ts)
                    logger.debug("last_buffers_ts:%s" % last_buffers_ts)
                    matching = True
                    # if this is the first loop.last_buffers_ts is empty. decision is immediate
                    if not last_buffers_ts:
                        matching = False
                    else:
                        for buffer_scanner in current_buffers_ts:
                            # vpl 01/06/2016
                            if (not buffer_scanner in last_buffers_ts):
                                logger.debug("%s is not present in last_buffers_ts. matching=False" % buffer_scanner)
                                matching = False
                            else:
                                if current_buffers_ts[buffer_scanner] != last_buffers_ts[buffer_scanner]:
                                    matching = False
                            '''
                            if current_buffers_ts[buffer_scanner] != last_buffers_ts[buffer_scanner]:
                                matching = False
                            '''
                    # replace last by current
                    last_buffers_ts = {i: current_buffers_ts[i] for i in current_buffers_ts}
                    # matching = True  means that current_buffers_ts = last_buffers_ts. We need to exit the main loop
                    # matching = False means that current_buffers_ts != last_buffers_ts. We need to iterate again
                    logger.debug("compared the current and last buffers_ts:%s" % matching)
                    if (matching):
                        buffer_processing = False

                    # move the schedule_time ahead and reset the selected_data_set dictionary
                    self.schedule_time = self.schedule_time + self.buffer_time_window

        result_data_set = self.prepare_result_json(result_data_set)
        return result_data_set

    def process_ctrl_cmd(self, ctrl_rdd):
        """
            Invoked on reception of message on the ctrl command kafka topic
            0) check if rdd is Empty. Check format of query cmd (JSON)
            1) check if the processor name matches with config
            2) check if its a response to get_process_status
            3) extract content of the rdd received as a json
                {"control_msg": "get_processor_status", "processor_name": "har_randomForest_accelerometer", "query_id" : "qwewqr-fsdfsd-1234"}
            4) invoke the self.caller.on_xxx() method based on the ctrl_msg
        """
        if (ctrl_rdd.isEmpty()):
            logger.debug("empty Ctrl command")
            return

        # We are using now CreateDirectStream which does not return the same format of rdd
        ctrl_msg_json = json.loads(ctrl_rdd.take(1)[0])
        # ctrl_msg_json = json.loads(ctrl_rdd.take(1)[0][1])

        if ((not 'control_msg' in ctrl_msg_json) |
                (not 'processor_name' in ctrl_msg_json) |
                (not 'query_id' in ctrl_msg_json)):
            logger.error(" one(s) of control_msg key(s) mal-formated %s:" % ctrl_msg_json)
            return

        if ((ctrl_msg_json['control_msg'] == "get_processor_status") &
                ('answer' in ctrl_msg_json)):
            logger.debug("This is an answer to get_process_status query. Skip")
            return

        if (not ctrl_msg_json['processor_name'] == APP_NAME):
            logger.debug("processor_name: %s not for processor: %s" % (ctrl_msg_json['processor_name'], APP_NAME))
            return

        # invoke the on_xxx() methods
        if ctrl_msg_json['control_msg'] == "get_processor_status":
            self.on_get_status(ctrl_msg_json['query_id'])
        elif ctrl_msg_json['control_msg'] == "start_processor":
            self.on_start(ctrl_msg_json['query_id'])
        elif ctrl_msg_json['control_msg'] == "stop_processor":
            self.on_stop(ctrl_msg_json['query_id'])
        elif ctrl_msg_json['control_msg'] == "pause_processor":
            self.on_pause(ctrl_msg_json['query_id'])
        elif ctrl_msg_json['control_msg'] == "destroy_processor":
            self.on_destroy(ctrl_msg_json['query_id'])
        else:
            logger.error("could not recognize ctrl_msg: %s" % ctrl_msg_json['control_msg'])

    def check_state_change(self, change_order):
        """
        Check the authorized state order based on current state.
        Authorized list is
            INITIALIZE from STOPPED
            START from INITIALIZED
            PAUSE from STARTED
            START from PAUSED
            STOP from PAUSED
            DESTROY from INITIALIZED
        """
        if (
                                    ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_STOPPED) &
                                         (change_order == rta_constants.RTA_PROCESSOR_ORDER_INITIALIZE)) |
                                    ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_INITIALIZED) &
                                         (change_order == rta_constants.RTA_PROCESSOR_ORDER_START)) |
                                ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_STARTED) &
                                     (change_order == rta_constants.RTA_PROCESSOR_ORDER_PAUSE)) |
                            ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_PAUSED) &
                                 (change_order == rta_constants.RTA_PROCESSOR_ORDER_START)) |
                        ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_PAUSED) &
                             (change_order == rta_constants.RTA_PROCESSOR_ORDER_STOP)) |
                    ((self.current_state == rta_constants.RTA_PROCESSOR_STATE_INITIALIZED) &
                         (change_order == rta_constants.RTA_PROCESSOR_ORDER_DESTROY))
        ):
            return True
        else:
            return False

    def find_closest_ts(self, list_ts, ts, choice):
        '''
        return the closest value of the list.
        3 choice for processing the search, Backward & forward, backward, forward
        Example
            sorted_ts = [1200, 1300, 1500, 1700]
            find_closest_ts(1260,sorted_ts,rta_constants.CLOSEST_CHOICE_BACKW_FORW)       ==> 1300
            find_closest_ts(1260,sorted_ts,rta_constants.CLOSEST_CHOICE_BACKW)            ==> 1200
            find_closest_ts(1260,sorted_ts,rta_constants.CLOSEST_CHOICE_FORW)             ==> 1300
            '''
        if (ts > sorted(list_ts)[-1]):
            return -1
        pos = bisect_left(list_ts, ts)
        if pos == 0:
            return list_ts[0]
        if pos == len(list_ts):
            return list_ts[-1]
        before = list_ts[pos - 1]
        after = list_ts[pos]
        if (choice == rta_constants.CLOSEST_CHOICE_BACKW_FORW):
            if after - ts < ts - before:
                return after
            else:
                return before
        elif (choice == rta_constants.CLOSEST_CHOICE_BACKW):
            return before
        elif (choice == rta_constants.CLOSEST_CHOICE_FORW):
            return after
        else:
            logger.debug("find_closest_ts unknow choice:", choice)
            return None

    def update_buffers(self, val_rdd):
        '''
        This method update the buffers_map with the values received from the rdd_val
        '''
        data_payload = val_rdd.collect()

        for value in data_payload:
            # check if the rdd is for the controler or not.
            try:
		print ("VPL 0912 - %s" % type(value))
                jsonObj = json.loads(value)
            except ValueError:
                logger.debug("val rdd is not valid JSON. No processing to do")
                return
            # jsonObj is a list. Need to retrieve the fisrt and unique value [{.....}]
            #logger.debug("jsonobj %s" % jsonObj)
            if type(jsonObj) == list:
                jsonObj = jsonObj[0]
            sensorType = jsonObj['processor_name'].split('_')[-1]
            # check if a buffer for this sensor has already been created
            if not sensorType in self.buffers_map:
                self.buffers_map[sensorType] = {}

            # update the buffer
            # These events are produced by the different har_processor_xx processors
            # {"eventId": "eadb9bb8-b9fd-4bfe-850d-bce3ceaf7189", "processor_name": "har_randomforest_accelerometer", "userid": "vincent.planat@hpe.com", "result": "WALKING_UP", "time_stamp": 1460995726187}
            buffer_val = self.buffers_map[sensorType]
            # print "processing data for %s:"% sensorType
            sensorVals = [jsonObj["eventId"], jsonObj["processor_name"], jsonObj["userid"], jsonObj["time_stamp"],
                          jsonObj["result"]]
            buffer_val[jsonObj["time_stamp"]] = sensorVals

    def prepare_result_json(self, processing_data_set):
        '''
            This method is used to prepare the json result return by the data buffer
        '''
        for user_val in processing_data_set:
            list_events = processing_data_set[user_val]
            logger.debug("prepare result json for :%s" % list_events)
            list_of_json = []
            for event in list_events:
                data_json = [{"eventId": event[0], "processor_name": event[1], "result": event[4], "userid": event[2],
                              "time_stamp": event[3]}]
                list_of_json.append(data_json)
            processing_data_set[user_val] = list_of_json

        logger.debug("prepare result json: %s", processing_data_set)

        return processing_data_set

    @staticmethod
    def simulated_prediction(list_events):
        """
            Simulation of majority voting algorithm, and return one activity
            with most frequency based on inputting events.
            if there is more than one activities with most frequency , and we pick up one of them randomly
        """

        counters = [0] * 4
        return_vals = [rta_constants.RTA_ACTIVITY_SITTING, rta_constants.RTA_ACTIVITY_WALKING,
                       rta_constants.RTA_ACTIVITY_WALKING_UP, rta_constants.RTA_ACTIVITY_WALKING_DOWN]
        # calculate frequency of activities.
        event_val_to_check = ""
        for e in list_events:
            if type(e) == list:
                event = e[0]
            else:
                event = e
                
            # vpl 26/05
            # event is a float (0.0, 1.0, 2.0 etc ..). Need to map to a string
            event_val_to_check = return_vals[int(event['result'])]
            
            #if event['result'] == rta_constants.RTA_ACTIVITY_SITTING:
            if event_val_to_check == rta_constants.RTA_ACTIVITY_SITTING:
                #counters[0] += 1
                counters[0] += WGT_ACTIVITY_SITTING
            elif event_val_to_check == rta_constants.RTA_ACTIVITY_WALKING:
                #counters[1] += 1
                counters[1] += WGT_ACTIVITY_WALKING
            elif event_val_to_check == rta_constants.RTA_ACTIVITY_WALKING_UP:
                #counters[2] += 1
                counters[2] += WGT_ACTIVITY_WALKING_UP
            elif event_val_to_check == rta_constants.RTA_ACTIVITY_WALKING_DOWN:
                #counters[3] += 1 
                counters[3] += WGT_ACTIVITY_WALKING_DOWN
        
        # vpl 26/05
        tmp_counters = ""
        for i in range(0,len(counters)):
            tmp_counters = ("counters[%d]:%d   " % (i,counters[i])) + tmp_counters
        logger.debug("   voting entry:%s" % tmp_counters)
        
        
        # Computer frequency of frequency of activities.
        counters_frequency = {}
        for i in counters:
            if i not in counters_frequency.keys():
                counters_frequency[i] = 1
            else:
                counters_frequency[i] += 1
        # Get indices of counters in reverse order.
        sorted_counter_index = sorted(range(len(counters)), key=lambda k: counters[k], reverse=True)
        # Check if more than one most frequency activities.
        max_value = counters[sorted_counter_index[0]]
        max_value_frequency = counters_frequency[max_value]
        if max_value_frequency > 1:
            # if there is more than one activities with most frequency , and we pick up one of them randomly
            random_max_index = random.choice(sorted_counter_index[:max_value_frequency])
            logger.debug("index of max is:%d" % random_max_index)
            return return_vals[random_max_index]
            # print "random_max_index: %d" % random_max_index
        else:
            # print sorted_counter_index[0]
            logger.debug("index of max is:%d" % sorted_counter_index[0])
            return return_vals[sorted_counter_index[0]]

    @staticmethod
    def map_predict_result_string_2_number(predict_result_str):
        string_list = [rta_constants.RTA_ACTIVITY_SITTING, rta_constants.RTA_ACTIVITY_WALKING,
                       rta_constants.RTA_ACTIVITY_WALKING_UP, rta_constants.RTA_ACTIVITY_WALKING_DOWN]
        number_list = range(1, len(string_list) + 1)
        map_string_2_number = dict(zip(string_list, number_list))
        return map_string_2_number[predict_result_str]
    #12/27/2016 commit by JOJO
    @staticmethod
    def parse_user_data_model_for_sk(model_path,ls_data_model_str):
         """This is specific method for sklearn      Parse result of file system of model files, and return a tuple (set, dictionary)     dictionary with (user_          id, sensor_type): data_model_path
        a set of users
         Args:
           model_path (str): storing model file's path
           ls_data_model_str (str): result string from hadoop ls command     Returns:
           tuple     """
         in_user_sensor_type_2_data_model_path = {}
         in_user_id_set = set()
         for line in ls_data_model_str.split("\n"):
              data_model_path = model_path+'/'+line
              fields = line.split('_')
              if len(fields)==5:
                  user_id = fields[2]
                  in_user_id_set.add(user_id)
                  sensor_type = fields[1]
                  in_user_sensor_type_2_data_model_path[(user_id,sensor_type)] = data_model_path
         return (in_user_id_set, in_user_sensor_type_2_data_model_path)
    @staticmethod
    def map_predict_result_number_2_string(predict_result_number):
        string_list = [rta_constants.RTA_ACTIVITY_SITTING, rta_constants.RTA_ACTIVITY_WALKING,
                       rta_constants.RTA_ACTIVITY_WALKING_UP, rta_constants.RTA_ACTIVITY_WALKING_DOWN]
        # number_list = range(1, len(string_list) + 1)
        # map_string_2_number = dict(zip(string_list, number_list))
        return string_list[int(predict_result_number)]

    def issue_insert_query_karrios(self, rest_post_obj, json_dumps, karrios_rest_url, max_number_of_datapoints,
                                   force_send_flag=False):
        """Insert max number of Karrios data points to KarriosDB if force_send_flag is set to false
            Args:
                rest_post_obj(requests): reference to requests package object
                json_dumps(reference to json.dumps): reference to method "dumps" in json package
                karrios_rest_url(str): restful port of KarriosDB
                max_number_of_datapoints(int): max number of data points to be inserted to KarriosDB
                force_send_flag(Optional: boolean): indicates if all data points should be dumped regardless of
                max_number_of_datapoints
        Returns:
            None
        """
        headers = {'content-type': 'application/json'}
        number_of_data_points = len(self.karrios_data_points_list)
        remain_data_point_index = 0
        if force_send_flag:
            if number_of_data_points > 0:
                number_of_data_points_2_send = len(self.karrios_data_points_list)
                json_data = json_dumps(self.karrios_data_points_list)
                remain_data_point_index = number_of_data_points
            else:
                number_of_data_points_2_send = 0
                json_data = ""
                remain_data_point_index = 0
        else:
            if number_of_data_points >= max_number_of_datapoints:
                number_of_data_points_2_send = len(self.karrios_data_points_list[:max_number_of_datapoints])
                json_data = json_dumps(self.karrios_data_points_list[:max_number_of_datapoints])
                remain_data_point_index = max_number_of_datapoints
            else:
                json_data = ""
                number_of_data_points_2_send = 0
                remain_data_point_index = 0
        if json_data != "":
            resp = rest_post_obj.post(karrios_rest_url, data=json_data, headers=headers)
            if resp.status_code != 204:
                # Print should be replaced with logger in future
                # print "Failed to datapoints posted to kairosDb %s" % (resp.status_code)
                logger.error("Failed to datapoints posted to kairosDb %s", resp.status_code)
            else:
                # Print should be replaced with logger in future
                # print "successful"
                logger.debug("Succeeded in posting %d data points to karios database", number_of_data_points_2_send)
        # Reset self.karrios_data_points_list
        self.karrios_data_points_list = self.karrios_data_points_list[remain_data_point_index:]

    def append_karios_data_points_sklearn(self, metric_name, event, predicted_result_number, user_name):
        data_point_dict = {}
        tags_dict = {}
        data_point_dict["name"] = metric_name
        data_point_dict["timestamp"] = event["window"]
        data_point_dict["value"] = predicted_result_number
        # predicted_result_record_2_karios_data_point_tag_dict["deviceid"] = event[0][""]
        tags_dict["userid"] = user_name
        data_point_dict["tags"] = tags_dict
        logger.debug("data point: %s", data_point_dict)
        self.karrios_data_points_list.append(data_point_dict)

    def append_karios_data_points(self, metric_name, event, predicted_result_number, user_name):
        """Append Karrios data points to self.karrios_data_points_list based on incoming input
            Args:
                table_name(str): name of table in Cassandra, which is used for metric name in Karrios.
                for example, Karrios metric name is "raw_accelerometer_x" if table_name is
                "accelerometer" and column name is "x"
                re_compile(reference to re.compile): reference to method of re.compile
        Returns:
            None
        """
        data_point_dict = {}
        tags_dict = {}
        data_point_dict["name"] = metric_name
        data_point_dict["timestamp"] = event[0]["time_stamp"]
        data_point_dict["value"] = predicted_result_number
        # predicted_result_record_2_karios_data_point_tag_dict["deviceid"] = event[0][""]
        tags_dict["userid"] = user_name
        data_point_dict["tags"] = tags_dict
        logger.debug("data point: %s", data_point_dict)
        self.karrios_data_points_list.append(data_point_dict)

    @staticmethod
    def get_vector_4_ensemble(events):
        """ Retrieve a list of events seen as below, and return a list (vector) of result numbers from processor stage
        to be passed to ensemble data model. Vector must require below format. Note None will be set if missing value
        for a sensor type. None is valid for ensemble data model.
        [[{'eventId': u'8f2d48c2-72ac-4686-93f7-a1fefc927598', 'processor_name': u'har_randomforest_gyroscope', 'userid': u'user2@hpe.com', 'result': 1, 'time_stamp': 1461630615817}],
        [{'eventId': u'd1d2034c-e9c3-4f69-96c6-d85a24d5c65a', 'processor_name': u'har_randomforest_accelerometer', 'userid': u'user2@hpe.com', 'result': 2, 'time_stamp': 1461630617850}]]
        format of vector to be as a input to ensemble data model:
        accelerometer 	gyroscope	altimeter	ambientLight	barometer	calorie	Distance
        """
        # Initialize vector with None
        input_vector = [None] * 7
        for e in events:
            if type(e) == list:
                event = e[0]
            else:
                event = e
            # get sensor type
            sensor_type = event["processor_name"].split("_")[-1]
            # logger.debug("sensor_type: %s", sensor_type)
            result_number = float(event["result"])
            # logger.debug("result: %s", result_number)
            if sensor_type == "accelerometer":
                # Note each element in vector must start with zeor, so we must subtract map function by one
                input_vector[0] = result_number
            elif sensor_type == "gyroscope":
                input_vector[1] = result_number
            elif sensor_type == "altimeter":
                input_vector[2] = result_number
            elif sensor_type == "ambientLight":
                input_vector[3] = result_number
            elif sensor_type == "barometer":
                input_vector[4] = result_number
            elif sensor_type == "calorie":
                input_vector[5] = result_number
            elif sensor_type == "distance":
                input_vector[6] = result_number
            else:
                pass
                # logger.debug("input_vector: %s", input_vector)
        # logger.debug("input_vector: %s", input_vector)
        return input_vector

    @staticmethod
    def parse_user_data_model(ls_data_model_str, model_extension_pattern_obj):
        """Parse result of hadoop filesystem command, and return a tuple (set, dictionary)
        dictionary with (user_id, sensor_type): data_model_path
        a set of users
        Args:
            ls_data_model_str (str): result string from hadoop ls command
        Returns:
            tuple
        """
        in_user_sensor_type_2_data_model_path = {}
        in_user_id_set = set()
        for line in ls_data_model_str.split("\n"):
            fields = [field.strip() for field in line.split(" ") if field.strip() != ""]
            if len(fields) == 8:
                data_model_path = fields[-1]
                data_model_name = data_model_path.split("/")[-1]
                # print(data_model_name)
                user_id = model_extension_pattern_obj.sub("", data_model_name.split("_")[1])
                in_user_id_set.add(user_id)
                sensor_type = data_model_name.split("_")[0]
                # print(user_id, sensor_type)
                in_user_sensor_type_2_data_model_path[(user_id, sensor_type)] = data_model_path
                # print(in_user_sensor_type_2_data_model_path)
        return (in_user_id_set, in_user_sensor_type_2_data_model_path)


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
                                                                                     , out_minute, out_second)

if __name__ == "__main__":
    # Check if arguments are valid
    if len(sys.argv) != 2:
        print "Usage: har_predict_aggr_ensemble.py <prediction algorithm> \nValid prediction algorithm: " + \
              " | ".join(VALID_ALGORITHM_LIST)
        sys.exit(-1)
    elif len(sys.argv) == 2 and sys.argv[1] not in ["voting", "randomforest", "naivebayes"]:
        print "Usage: har_predict_aggr_ensemble.py <prediction algorithm> \nValid prediction algorithm: " + \
              " | ".join(VALID_ALGORITHM_LIST)
        sys.exit(-1)
    else:
        algorithm_name = sys.argv[1]

    valid_user_broadcast_list = None

    process_start_time = time.time()

    myProcessor = HarPredictAggr(algorithm_name)
