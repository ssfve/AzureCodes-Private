# -*- coding: utf-8 -*-
"""
Author: Wu Jun-Yi(junyi.wu@hpe.com)
Creation Date: 2016-09-22
Purpose: To manage all of the processors start and stop from cmd line

update
 - vpl 2311 Fixed the disconnection problem (when term session is disconnect) for tomcat
 - Larry 11/29 Fixed the processors can be running in background when ssh has been disconnected
 - JOJO 1/5/2017 Update the platform_mngt_cli.py to remove the HDFS line 
"""
import os
import commands
from cmd import Cmd
import subprocess
import rta_constants
import logging
import logging.config
import sys
import re
import ConfigParser
from platform_mngt_lib import ProcessInfo

class ProcessCMD(Cmd):
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(rta_constants.PROPERTIES_FILE)
    spark_master_url = config_parser.get(rta_constants.FRAMEWORK_NAME, "spark.master_url")
    general_path_project = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.general.project")
    accl_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.accl.log")
    baro_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.baro.log")
    gyro_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.gyro.log")
    aggr_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.aggr.log")
    raw_writer_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.raw.writer.log")
    novelty_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.novelty.log")
    default_log = config_parser.get(rta_constants.FRAMEWORK_NAME, "name.default.log")

    # processor install path setting
    general_path_hadoop = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.hadoop")
    general_path_spark = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.spark")
    general_path_kafka = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.kafka")
    general_path_kafka_properties = 'config/server.properties'
    general_path_zookeeper = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.zookeeper")
    general_path_zookeeper_properties = 'config/zookeeper.properties'
    general_path_rta_pid = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.rta.pid")
    general_path_kairosDB = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.kairosDB")
    general_path_tomcat = config_parser.get(rta_constants.FRAMEWORK_NAME, "path.tomcat")

    #1/5/2017 Commit by JOJO
    '''
    cmd_start_hadoop_hdfs = general_path_hadoop+'/sbin/start-dfs.sh'
    cmd_stop_hadoop_hdfs = general_path_hadoop+'/sbin/stop-dfs.sh'
    '''

    # Vpl 2311 update to fix problem of zookeeper shutdown when term exit (10.x timeout)
    #cmd_start_kafka = general_path_kafka+'/bin/kafka-server-start.sh'+' '+general_path_kafka+'/'+general_path_kafka_properties+' &'
    cmd_start_kafka = ['nohup',general_path_kafka+'/bin/kafka-server-start.sh',general_path_kafka+'/'+general_path_kafka_properties]
    cmd_stop_kafka = general_path_kafka+'/bin/kafka-server-stop.sh'+' '+general_path_kafka+'/'+general_path_kafka_properties+' &'
    #cmd_start_zookeeper = general_path_zookeeper+'/bin/zookeeper-server-start.sh'+' '+general_path_zookeeper+'/'+general_path_zookeeper_properties+' &'
    cmd_start_zookeeper = ['nohup',general_path_zookeeper+'/bin/zookeeper-server-start.sh',general_path_zookeeper+'/'+general_path_zookeeper_properties]
    cmd_stop_zookeeper = general_path_zookeeper+'/bin/zookeeper-server-stop.sh'

    cmd_start_spark = general_path_spark+'/sbin/start-all.sh'
    cmd_stop_spark = general_path_spark+'/sbin/stop-all.sh'


    # Vpl 2311 update to fix problem of cassandra shutdown when term exit (10.x timeout)
    # cmd_start_cassandra = 'cassandra &'
    cmd_start_cassandra = '/usr/sbin/cassandra'
    cmd_stop_cassandra = 'ps -ef | grep "cassandra.service.CassandraDaemon" | grep -v grep | awk "{print $1}"'

	# Vpl 2311 update to fix problem of kairosDb shutdown when term exit (10.x timeout)
    #cmd_start_kairosDB = general_path_kairosDB+'/bin/kairosdb.sh start'
    cmd_start_kairosDB = general_path_kairosDB+'/bin/kairosdb.sh'
    cmd_stop_kairosDB = general_path_kairosDB+'/bin/kairosdb.sh stop'

    cmd_start_grafana = 'sudo service grafana-server start'
    cmd_stop_grafana = 'sudo service grafana-server stop'

	# Vpl 2311 update to fix problem of catalina shutdown when term exit (10.x timeout)
    # cmd_start_tomcat = general_path_tomcat + '/bin/catalina.sh start'
    cmd_start_tomcat = general_path_tomcat + '/bin/catalina.sh'
    cmd_stop_tomcat = general_path_tomcat + '/bin/catalina.sh stop'
    # ps ax | grep -i 'catalina' | grep -v grep | awk '{print $1}'

    cmd_start_web_management =  general_path_project+'/src/'+'platform_mngt_web.py'

    cmd_stop_spark_master = general_path_spark+'/sbin/stop-master.sh'
    cmd_start_spark_master = general_path_spark+'/sbin/start-master.sh'
    cmd_stop_spark_worker = general_path_spark+'/sbin/stop-slaves.sh'
    cmd_start_spark_worker = general_path_spark+'/sbin/start-slaves.sh'
    cmd_start_accl_processor = 'nohup python '+general_path_project+'/src/rta_wrapper.py -sh "'+general_path_project+'" -sn "rta_processor_simple_1_1.py accelerometer sklearn"'+' 1>../log/'+accl_log+' 2>&1 &'
    #cmd_start_accl_processor = ['python',general_path_project+'/src/rta_wrapper.py','-sh',general_path_project,'-sn',"rta_processor_simple_1_1.py accelerometer sklearn","1>../log/"+accl_log+" 2>&1"]
    cmd_stop_accl_processor = "/bin/bash -c 'kill -s SIGTERM `cat "+general_path_rta_pid+"/rta_processor_simple_1_1_accelerometer_sklearn.pid"+"`'"
    cmd_start_gyro_processor = 'nohup python '+general_path_project+'/src/rta_wrapper.py -sh "'+general_path_project+'" -sn "rta_processor_simple_1_1.py gyroscope sklearn"'+' 1>../log/'+gyro_log+' 2>&1 &'
    cmd_stop_gyro_processor = "/bin/bash -c 'kill -s SIGTERM `cat "+general_path_rta_pid+"/rta_processor_simple_1_1_gyroscope_sklearn.pid"+"`'"
    cmd_start_baro_processor = 'nohup python '+general_path_project+'/src/rta_wrapper.py -sh "'+general_path_project+'" -sn "rta_processor_simple_1_1.py barometer sklearn"'+' 1>../log/'+baro_log+' 2>&1 &'
    cmd_stop_baro_processor = "/bin/bash -c 'kill -s SIGTERM `cat "+general_path_rta_pid+"/rta_processor_simple_1_1_barometer_sklearn.pid"+"`'"
    cmd_start_aggr_naiv = 'nohup python '+general_path_project+'/src/rta_wrapper.py -sh "'+general_path_project+'" -sn "har_predict_aggr_ensemble_1_1.py naivebayes"'+' 1>../log/'+aggr_log+' 2>&1 &'
    cmd_stop_aggr_naiv = "/bin/bash -c 'kill -s SIGTERM `cat "+general_path_rta_pid+"/har_predict_aggr_ensemble_1_1_naivebayes.pid"+"`'"

    cmd_start_raw_writer = 'nohup '+general_path_spark+"/bin/spark-submit --master "+spark_master_url+" --executor-memory 2G --driver-memory 512M --total-executor-cores 1 --jars "+general_path_project+"/libs/spark-streaming-kafka-assembly_2.10-1.6.0.jar "+general_path_project+"/src/raw_writer.py 1>../log/"+raw_writer_log+" 2>&1 &"
    cmd_stop_raw_writer = "ps ax |grep -i 'java' |grep -i 'raw_writer.py' | grep -v grep | awk '{print $1}'"
    cmd_start_novelty_detector = 'nohup '+general_path_spark+'/bin/spark-submit --master '+spark_master_url+' --executor-memory 2G  --driver-memory 512M --total-executor-cores 1 --jars "../libs/spark-streaming-kafka-assembly_2.10-1.6.0.jar" --py-files "rta_constants.py,rta_datasets.py" '+general_path_project+'/src/novelty_detect.py 1>../log/'+novelty_log+' 2>&1 &'
    cmd_stop_novelty_detector ="ps ax |grep -i 'java' |grep -i 'novelty_detect.py' | grep -v grep | awk '{print $1}'"


    """Simple command processor example."""

    prompt = 'rta>> '
    intro = ("Welcome to RTA cmd line world. \n:"
        "you may input 'help [command]' keyword to tracking all the cmds help\n"
        "As following cmds are available to execute\n"
        "start | stop | list | exit")

    doc_header = 'useful guide'
    misc_header = 'misc_header'
    undoc_header = 'No useful guide'

    def construct_logger(self,in_logger_file_path):
        """Instantiate and construct logger object based on log properties file by default. Otherwise logger object will be
        constructed with default properties.
        Args:
            in_logger_dir_path (str): path followed by file name where logger properties/configuration file resides
        Returns:
            logger object
        """
        if os.path.exists(in_logger_file_path):
            logging.config.fileConfig(in_logger_file_path)
            logger = logging.getLogger(os.path.basename(__file__))
        else:
            # If logger property/configuration file doesn't exist,
            # and logger object will be constructed with default properties.
            logger = logging.getLogger(os.path.basename(__file__))
            logger.setLevel(logging.DEBUG)
            # Create a new logger file
            logger_file_path_object = open(in_logger_file_path, 'a+')
            logger_file_path_object.close()
            # create a file handler
            handler = logging.FileHandler(in_logger_file_path)
            handler.setLevel(logging.INFO)
            # create a logging format
            formatter = logging.Formatter('[%(asctime)s - %(name)s - %(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            # add the handlers to the logger
            logger.addHandler(handler)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            logger.warning("The logger configuration file %s doesn't exist, "
                            "so logger object will be constructed with default properties.", in_logger_file_path)
        return logger

    def get_running_status(self):
        """get currenly running processor list
        Returns:
            False - this list is null and not
            list = list of processors
        """
        obj = ProcessInfo('jobs')
        process_list = obj.handle_parameter()
        if process_list:
            # get the hostname
            hostname = process_list[0]
            del process_list[0]
            process_list = obj.extract_process(process_list)
            # print 'dict is here$$$$$'
            dict_processor = []
            for proc_val in process_list:
                if proc_val.search_result ==0:
                    dict_processor.append({'processor':proc_val.name,'status':'Stopped','PID':str(proc_val.pid)})
                elif proc_val.search_result >=1:
                    dict_processor.append({'processor':proc_val.name,'status':'Running','PID':str(proc_val.pid)})
                    # dict_processor[proc_val.name] = 'Running'
                # print ("|%-20s|%-5s|"%(proc_val.name,proc_val.search_result))
            # print dict_processor
            return dict_processor
        else:
            return False


    def emptyline(self):
        pass

    def do_shell(self, line):
        "Run a shell command"
        print "running shell command:", line
        output = os.popen(line).read()
        print output
        self.last_output = output

    def do_list(self,line):
        """list
        list all the configured processor status
        """
        # app_logger = self.construct_logger(rta_constants.PROPERTIES_LOG_FILE)
        obj = ProcessInfo('jobs')
        process_list = obj.handle_parameter()

        if process_list:
            # get the hostname
            hostname = process_list[0]
            del process_list[0]
            process_list = obj.extract_process(process_list)
            # print 'dict is here$$$$$'
            # sys.exit(1)
            dict_processor = []
            for proc_val in process_list:
                if proc_val.search_result ==0:
                    dict_processor.append({'processor':proc_val.name,'status':'Stopped','PID':str(proc_val.pid)})

                elif proc_val.search_result >=1:
                    dict_processor.append({'processor':proc_val.name,'status':'Running','PID':str(proc_val.pid)})
                    # dict_processor[proc_val.name] = 'Running'
                # print ("|%-20s|%-5s|"%(proc_val.name,proc_val.search_result))
            # print dict_processor
            print('##############################################')
            print('PID  #'+' Processor                    #'+' Status')
            print('##############################################')
            spark_ls = []
            for processor in dict_processor:
                if processor.get('processor') == 'spark<spark_worker>' or processor.get('processor') == 'spark<spark_master>':
                    spark_ls.append(processor)
                    del dict_processor[dict_processor.index(processor)]
            # print dict_processor
            for processor in dict_processor:
                space_pid = 7 - len(processor.get('PID'))
                space_name = 30 - len(processor.get('processor'))
                if processor.get('status') == 'Running':
                    print str(processor.get('PID'))+space_pid*' '+processor.get('processor') + space_name*' '+ '\33[32m' +processor.get('status')+ '\33[0m'
                else:
                    print str(processor.get('PID'))+space_pid*' '+processor.get('processor') + space_name*' '+ '\33[33m' +processor.get('status')+ '\33[0m'
                    # space_num = 30 - len(k)
                    # print k + space_num*' '+v
            print 7*' '+'spark'
            for item in spark_ls:
                space_pid = 8 - len(item.get('PID'))
                space_name = 29 - len(item.get('processor').split('<')[1].split('>')[0])
                if item.get('status')=='Running':
                    print str(item.get('PID'))+space_pid*' '+item.get('processor').split('<')[1].split('>')[0] + space_name*' '+ '\33[32m'+item.get('status')+'\33[0m'
                else:
                    print str(item.get('PID'))+space_pid*' '+item.get('processor').split('<')[1].split('>')[0] + space_name*' '+ '\33[33m'+item.get('status')+'\33[0m'
            print('##############################################')
        else:
            print("cmd is not support from this host")

    def do_start(self,processor):
        """start [processor]
        start the name of processor
        available processor list
        [spark]          -->  Engine data processing
        [kafka]          -->  Data pipelines and streaming
        [web_management]          -->  Integration webserver
        [zookeeper]      -->  Zookeeper central service
        [cassandra]      -->  Cassandra real-time database
        [kairosDb]       -->  Time Series Database on Cassandra
        [grafana]        -->  Grafana realtime series dashboard
        [tomcat]         -->  Java servlet server
        [raw_writer]     -->  Raw data writer on cassandra
        [novelty]        -->  Novelty detection for Healthcare
        [accl_processor] -->  Accelerometer HAR classifier
        [baro_processor] -->  Barometer HAR classifier
        [gyro_processor] -->  Gyroscope HAR classifier
        [aggr_processor] -->  Aggregation HAR classifier
        """
        # app_logger = self.construct_logger(rta_constants.PROPERTIES_LOG_FILE)
        running_dict = {}
        for item in self.get_running_status():
            running_dict[item.get('processor')]=item.get('status')

        if processor == 'spark':
            if running_dict:
                if running_dict['spark<spark_worker>'] != 'Running' and running_dict['spark<spark_master>'] != 'Running':
                    try:
                        cmd_line = self.cmd_start_spark
                        cmd = subprocess.Popen([cmd_line],shell=True,stdout=subprocess.PIPE)
                        (output,err) = cmd.communicate()
                        # app_logger.info('*********output logging **************')
                        print(output)
                        return
                    except Exception as ex:
                        print("    Failed to run processor with ERROR(%s).", str(ex))
                        sys.exit(1)
                else:
                    if running_dict['spark<spark_worker>'] == 'Running' or running_dict['spark<spark_master>'] == 'Running':
                        print('Spark Server is running!! please trying to stop it before it starts.')
                        return
            else:
                print('Please type correct command! You may use "help start" see more help')
                return

        elif processor == 'tomcat':
            if running_dict.has_key('tomcat') and running_dict['tomcat'] != 'Running':
                try:
                    cmd_line = self.cmd_start_tomcat
                    # print('staring tomcat server------->')
                    print cmd_line

                    # 2311 Vpl update to fix problem of catalina shutdown when term exit (10.x timeout)
                    cmd = subprocess.call(['nohup',cmd_line,'start'])
                    #cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    #(output,err) = cmd.communicate()

                    # app_logger.info('*********output logging **************')
                    #print(output)
                    return
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('tomcat'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                else:
                    print('Tomcat Server is running!! please trying to stop it before it start.')
                    return

        elif processor == 'HDFS':
            #1/5/2017 Commit by JOJO
            '''
            if running_dict.has_key('HDFS') and running_dict['HDFS'] != 'Running':
                try:
                    cmd_line = self.cmd_start_hadoop_hdfs
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # (output,err) = cmd.communicate()
                    # print(output)
                    # app_logger.info('*********output logging **************')
                    # print(output)
                    print('HDFS has been started!')
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('HDFS'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                else:
                    print('HDFS server is running!! please trying to stop it before it start.')
                    return
            '''
            print('Please type correct command! You may use "help start" see more help')
            return
        elif processor == 'web_management':
            if running_dict.has_key('web_management') and running_dict['web_management'] != 'Running':
                try:
                    cmd_line = 'python '+self.cmd_start_web_management
                    print('starting web_management webserver------->')
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    (output,err) = cmd.communicate()
                    print(output)
                    # app_logger.info('*********output logging **************')
                    # print(output)
                    print('web_management webserver has been started!')
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('web_management'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                else:
                    print('Flask webserver is running!! please trying to stop it before it start.')
                    return

        elif processor == 'novelty':
            if running_dict.has_key('novelty') and running_dict['novelty'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running':
                try:
                    cmd_line = self.cmd_start_novelty_detector
                    # print('staring novelty------->')
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # (output,err) = cmd.communicate()

                    # app_logger.info('*********output logging **************')
                    print('novelty has been started!')
                    return
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('novelty'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                elif running_dict['novelty'] == 'Running':
                    print('novelty processor is running!! please trying to stop it before it start.')
                    return
                elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                    print('Please start spark first!! trying to use command "start spark"')
                    return

        elif processor == 'raw_writer':
            if running_dict.has_key('raw_writer') and running_dict['raw_writer'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running':
                try:
                    cmd_line = self.cmd_start_raw_writer
                    # print('staring raw_writer------->')
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # (output,err) = cmd.communicate()
                    print('raw_writer has been started!')
                    return

                    # app_logger.info('*********output logging **************')
                    # print(output)
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('raw_writer'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                elif running_dict['raw_writer'] == 'Running':
                    print('raw_writer processor is running!! please trying to stop it before it start.')
                    return
                elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                    print('Please start spark first!! trying to use command "start spark"')
                    return

        elif processor == 'cassandra':
            if running_dict.has_key('cassandra') and running_dict['cassandra'] != 'Running':
                try:
                    cmd_line = self.cmd_start_cassandra
                    # print('starting cassandra------->')
                    # print cmd_line

                    #2311 Vpl update to fix problem of cassandra shutdown when term exit (10.x timeout)
                    #cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    cmd = subprocess.call(['nohup',cmd_line])
                    #(output,err) = cmd.communicate()

                    # app_logger.info('*********output logging **************')
                    # print(output)
                    print ('cassandra has been started!')
                    return
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('cassandra'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                else:
                    print('cassandra Server is running!! please trying to stop it before it start.')
                    return

        elif processor == 'kairosDb':
            if running_dict.has_key('kairosDb') and running_dict['kairosDb'] != 'Running' and running_dict['cassandra']=='Running':
                try:
                    cmd_line = self.cmd_start_kairosDB
                    # print('staring kairosDB------->')

                    # print cmd_line
					#2311 Vpl update to fix problem of kairosDb shutdown when term exit (10.x timeout)
					#cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    cmd = subprocess.call(['nohup',cmd_line,'start'])
                    #(output,err) = cmd.communicate()

                    # app_logger.info('*********output logging **************')
                    print('kairosDb has been started!')
                    return
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('kairosDb'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                elif running_dict['cassandra']=='Stopped':
                    print('cassandra required starting before kairosDb is running!! please trying to "start cassandra" first')
                    return
                elif running_dict['kairosDB'] == 'Running':
                    print('kairosDB Server is running!! please trying to stop it before it starts.')
                    return

        elif processor == 'grafana':
            if running_dict.has_key('grafana') and running_dict['grafana'] != 'Running' and running_dict['kairosDb']=='Running':
                try:
                    cmd_line = self.cmd_start_grafana
                    # print('staring grafana------->')
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # (output,err) = cmd.communicate()
                    # app_logger.info('*********output logging **************')
                    # print(output)
                    print ('grafana has been started!')
                    return
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('grafana'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                elif running_dict['kairosDb']=='Stopped':
                     print('kairosDb required starting before grafana is running!! please trying to "start kairoseDb" first')
                     return
                elif running_dict['grafana'] == 'Running':
                    print('grafana Server is running!! please trying to stop it before it starts.')
                    return

        elif processor == 'kafka':
            if running_dict.has_key('kafka') and running_dict['kafka'] != 'Running' and running_dict['zookeeper']=='Running':
                try:
                    cmd_line = self.cmd_start_kafka
                    print('starting kafka------->')
                    # print cmd_line

                    #2311 Vpl update to fix problem of zookeeper shutdown when term exit (10.x timeout)
                    #cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    cmd = subprocess.Popen(cmd_line)
                    # (output,err) = cmd.communicate()
                    # print (output)
                    print ('kafka has been started!')
                    return
                    # app_logger.info('*********output logging **************')
                    # print(output)
                except Exception as ex:
                    print("    Failed to run processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('kafka'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                elif running_dict['zookeeper']=='Stopped':
                    print('zookeeper required starting before kafka is running!! please trying to "start zookeeper" first')
                    return
                elif running_dict['kafka'] == 'Running':
                    print('Kafka Server is running!! please trying to stop it before it starts.')
                    return

        elif processor == 'zookeeper':
            if running_dict.has_key('zookeeper') and running_dict['zookeeper'] != 'Running':
                try:
                    cmd_line = self.cmd_start_zookeeper
                    # print('staring zookeeper------->')
                    # print (cmd_line)

                    #2311 Vpl update to fix problem of zookeeper shutdown when term exit (10.x timeout)
                    #cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    cmd = subprocess.Popen(cmd_line)
                    # (output,err) = cmd.communicate()
                    # print (output)

                    print('zookeeper has been started!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('zookeeper'):
                    print('Please type correct command! You may use "help start" see more help')
                    return
                else:
                    print('Zookeeper Server is running!! please trying to stop it before it starts.')
                    return

        elif processor == 'accl_processor':
            if running_dict:
                if running_dict['accl_processor'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running' and running_dict['zookeeper'] == 'Running' and running_dict['kafka'] == 'Running':
                    try:
                        cmd_line = self.cmd_start_accl_processor
                        print cmd_line
                        cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                        #cmd = subprocess.Popen(['nohup',cmd_line])
                        # cmd = subprocess.Popen(cmd_line)

                        print ('Accelerometer processor has been started')
                        return
                    except Exception as ex:
                        print("    Failed to run processor with ERROR(%s).", str(ex))
                        sys.exit(1)
                else:
                    if running_dict['accl_processor'] == 'Running':
                        print('Accelerometer processor is running!! please trying to stop it before it starts.')
                        return
                    elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                        print('Please start spark first!! trying to use command "start spark"')
                        return
                    elif running_dict['zookeeper'] == 'Stopped':
                        print('Please start zookeeper server first!! trying to use command "start zookeeper"')
                        return
                    elif running_dict['kafka'] == 'Stopped':
                        print('Please start kafka server first!! trying to use command "start kafka"')
                        return
            else:
                print('Please type correct command! You may use "help start" see more help')
                sys.exit(1)

        elif processor == 'baro_processor':
            if running_dict:
                if running_dict['baro_processor'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running' and running_dict['zookeeper'] == 'Running' and running_dict['kafka'] == 'Running':
                    try:
                        cmd_line = self.cmd_start_baro_processor
                        cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                        print ('Barometer processor has been started')
			print (cmd_line)
                        return
                    except Exception as ex:
                        print("    Failed to run processor with ERROR(%s).", str(ex))
                        sys.exit(1)
                else:
                    if running_dict['baro_processor'] == 'Running':
                        print('Barometer processor is running!! please trying to stop it before it starts.')
                        return
                    elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                        print('Please start spark first!! trying to use command "start spark"')
                        return
                    elif running_dict['zookeeper'] == 'Stopped':
                        print('Please start zookeeper server first!! trying to use command "start zookeeper"')
                        return
                    elif running_dict['kafka'] == 'Stopped':
                        print('Please start kafka server first!! trying to use command "start kafka"')
                        return
            else:
                print('Please type correct command! You may use "help start" see more help')
                sys.exit(1)

        elif processor == 'gyro_processor':
            if running_dict:
                if running_dict['gyro_processor'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running' and running_dict['zookeeper'] == 'Running' and running_dict['kafka'] == 'Running':
                    try:
                        cmd_line = self.cmd_start_gyro_processor
                        cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                        print ('Gyroscope processor has been started')
                        return
                    except Exception as ex:
                        print("    Failed to run processor with ERROR(%s).", str(ex))
                        sys.exit(1)
                else:
                    if running_dict['gyro_processor'] == 'Running':
                        print('Gyroscope processor is running!! please trying to stop it before it starts.')
                        return
                    elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                        print('Please start spark first!! trying to use command "start spark"')
                        return
                    elif running_dict['zookeeper'] == 'Stopped':
                        print('Please start zookeeper server first!! trying to use command "start zookeeper"')
                        return
                    elif running_dict['kafka'] == 'Stopped':
                        print('Please start kafka server first!! trying to use command "start kafka"')
                        return
            else:
                print('Please type correct command! You may use "help start" see more help')
                sys.exit(1)

        elif processor == 'aggr_processor':
            if running_dict:
                if running_dict['aggr_processor'] != 'Running' and running_dict['spark<spark_worker>'] == 'Running' and running_dict['spark<spark_master>'] == 'Running' and running_dict['zookeeper'] == 'Running' and running_dict['kafka'] == 'Running':
                    try:
                        cmd_line = self.cmd_start_aggr_naiv
                        cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                        print ('Aggregator processor has been started')
                        return
                    except Exception as ex:
                        print("    Failed to run processor with ERROR(%s).", str(ex))
                        sys.exit(1)
                else:
                    if running_dict['aggr_processor'] == 'Running':
                        print('Aggregator processor is running!! please trying to stop it before it starts.')
                        return
                    elif running_dict['spark<spark_worker>'] == 'Stopped' or running_dict['spark<spark_master>'] == 'Stopped':
                        print('Please start spark first!! trying to use command "start spark"')
                        return
                    elif running_dict['zookeeper'] == 'Stopped':
                        print('Please start zookeeper server first!! trying to use command "start zookeeper"')
                        return
                    elif running_dict['kafka'] == 'Stopped':
                        print('Please start kafka server first!! trying to use command "start kafka"')
                        return
            else:
                print ('Please type correct command! You may use "help start" see more help')
                sys.exit(1)

        else:
            print ('Please type correct command! You may use "help start" see more help')

    def do_stop(self,processor):
        """stop [processor]
        Stop the name of processor
        available processor list
        [spark]          -->  Engine data processing
        [kafka]          -->  Data pipelines and streaming
        [web_management]          -->  Integration webserver
        [zookeeper]      -->  Zookeeper central service
        [cassandra]      -->  Cassandra real-time database
        [kairosDb]       -->  Time Series Database on Cassandra
        [grafana]        -->  Grafana realtime series dashboard
        [tomcat]         -->  Java servlet server
        [raw_writter]    -->  Raw data writer on cassandra
        [novelty]        -->  Novelty detection for Healthcare
        [accl_processor] -->  Accelerometer HAR classifier
        [baro_processor] -->  Barometer HAR classifier
        [gyro_processor] -->  Gyroscope HAR classifier
        [aggr_processor] -->  Aggregation HAR classifier
        """
        # app_logger = self.construct_logger(rta_constants.PROPERTIES_LOG_FILE)
        running_dict = {}
        runnind_pids = {}
        for item in self.get_running_status():
            running_dict[item.get('processor')]=item.get('status')
            runnind_pids[item.get('processor')]=item.get('PID')
        if processor == 'spark':
            if running_dict.has_key('spark<spark_master>') and running_dict.has_key('spark<spark_master>') and (running_dict['spark<spark_master>']=='Running' or running_dict['spark<spark_worker>']=='Running'):
                try:
                    cmd_line = self.cmd_stop_spark
                    cmd = subprocess.Popen([cmd_line],shell=True,stdout=subprocess.PIPE)
                    (output,err) = cmd.communicate()
                    # print('*********output logging **************')
                    print(output)
                    print ('Spark has been stopped!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not (running_dict.has_key('spark<spark_master>') or running_dict.has_key('spark<spark_master>')):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                elif running_dict['spark<spark_master>']!='Running' or running_dict['spark<spark_worker>']!='Running':
                    print('Spark server is already stopped! No action need to do!')
                    return

        elif processor == 'HDFS':
            #1/5/2017 Commit by JOJO
            '''
            if running_dict.has_key('HDFS') and running_dict['HDFS'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_hadoop_hdfs
                    # print('staring novelty------->')
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # (output,err) = cmd.communicate()

                    # app_logger.info('*********output logging **************')
                    print('HDFS server has been started!')
                    return
                    # pid = runnind_pids.get('tomcat')
                    # if not pid:
                    #     print "No Tomcat server to stop"
                    #     # sys.exit(1)
                    #     return
                    # else:
                    #     kill_cmd = "kill -s TERM "+pid
                    #     os.popen(kill_cmd)
                    #     print "Executed:"+kill_cmd
                    #     print ('Tomcat server has been stopped!')
                    #     return

                except Exception as ex:
                    print("    Failed to stop HDFS server with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('HDFS'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('HDFS server is already stopped! No action need to do!')
                    return
            '''
            print('Please type correct command! You may use "help stop" see more help')
            return
        elif processor == 'tomcat':
            if running_dict.has_key('tomcat') and running_dict['tomcat'] == 'Running':
                try:
                    pid = runnind_pids.get('tomcat')
                    if not pid:
                        print "No Tomcat server to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print ('Tomcat server has been stopped!')
                        return

                except Exception as ex:
                    print("    Failed to stop Tomcat server with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('tomcat'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Tomcat server is already stopped! No action need to do!')
                    return

        elif processor == 'kairosDb':
            if running_dict.has_key('kairosDb') and running_dict['kairosDb'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_kairosDB
                    # print 'stopping kairosDB------>'
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    (output,err) = cmd.communicate()
                    # print (cmd.pid)
                    # print('*********output logging **************')
                    print (output)
                    print('Stopped kairosDB server')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('kairosDb'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('KairosDB Server is already stopped! No action need to do!')
                    return

        elif processor == 'cassandra':
            if running_dict.has_key('cassandra') and running_dict['cassandra'] == 'Running':
                try:
                    pid = runnind_pids.get('cassandra')
                    if not pid:
                        print "No cassandra server need to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print('Stopped cassandra server')
                        return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('cassandra'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Cassandra is already stopped! No action need to do!')
                    return

        elif processor == 'grafana':
            if running_dict.has_key('grafana') and running_dict['grafana'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_grafana
                    # print 'stopping grafana------>'
                    # print cmd_line
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    (output,err) = cmd.communicate()
                    # print (cmd.pid)
                    # print('*********output logging **************')
                    print('Stopped grafana server')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('grafana'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Grafana server is already stopped! No action need to do!')
                    return

        elif processor == 'kafka':
            if running_dict.has_key('kafka') and running_dict['kafka'] == 'Running':
                try:
                    # cmd_line = self.cmd_stop_kafka
                    # print cmd_line
                    # cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    # print ('kafka has been stopped!')
                    # return

                    pid = runnind_pids.get('kafka')
                    if not pid:
                        print "No kafka server to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -9 "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print('Kafka has been stopped')
                        return

                    # print('*********output logging **************')
                    # cmd_line = self.cmd_stop_kafka
                    # print cmd_line
                    # cmd_line = "ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}'"
                    # cmd_line ="ps ax > larry.txt"
                    # runjob.run_processor(cmd_line)
                    # # cmd_line = "ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}'"
                    # # print cmd_line
                    # # os.popen(cmd_line)
                    # sys.exit(1)
                    # print process_ls

                    # (status, output) = commands.getstatusoutput(cmd_line)
                    # print status, output
                    # sys.exit(1)

                    # f = open('larry.txt','r')
                    # f.close()
                    # result = list()
                    # for line in f.readlines():
                    #     print line
                    #     result.append(line)
                    # print result
                    # sys.exit(1)
                    # ps_cmd = "ps ax"
                    # regex_search = re.compile(".*kafka\.Kafka config\/server\.properties")

                    # # Retrieve the result of ps ax into a list
                    # procc_list=os.popen(ps_cmd).readlines()
                    # print procc_list
                    # # filter each element of the list and keep only the one matching the regex
                    # procc_line = filter(regex_search.match,procc_list)
                    # print procc_line
                    # the resulting procc_line is a list of 1 element.
                    # We scan this elem (for elem in procc_line) and split its content using a space seperator
                    # pid = [elem.split(' ')[0] for elem in procc_line]
                    # The first ([0]) one is the pid we are looking for
                    # print ("result:%s" % pid[0])
                    # ps_list = []
                    # for item in process_ls:
                    #     if item.find('org.apache.zookeeper.server.quorum.QuorumPeerMain'):
                    #         ps_list.append(item)
                    # print ps_list
                    # print pid_str.split(' ')[0]
                    # sys.exit(1)
                    # Check if the PIDS variable is empty

                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('kafka'):
                    print('Please type correct command! please check with your administrator')
                    return
                else:
                    print('Kafka Server is already stopped! No action need to do!')
                    return

        elif processor == 'zookeeper':
            if running_dict.has_key('zookeeper') and running_dict['zookeeper'] == 'Running':
                try:
                    pid = runnind_pids.get('zookeeper')
                    if not pid:
                        print "No zookeeper server to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print('Zookeeper has been stopped')
                        return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('zookeeper'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Zookeeper Server is already stopped! No action need to do!')
                    return

        elif processor == 'accl_processor':
            if running_dict.has_key('accl_processor') and running_dict['accl_processor'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_accl_processor
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    print ('accl_processor has been stopped!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('accl_processor'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Accelerometer processor is already stopped! No action need to do!')
                    return

        elif processor == 'gyro_processor':
            if running_dict.has_key('gyro_processor') and running_dict['gyro_processor'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_gyro_processor
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    print ('gyro_processor has been stopped!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('gyro_processor'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Gyroscope processor is already stopped! No action need to do!')
                    return

        elif processor == 'baro_processor':
            if running_dict.has_key('baro_processor') and running_dict['baro_processor'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_baro_processor
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    print ('baro_processor has been stopped!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('baro_processor'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Barometer processor is already stopped! No action need to do!')
                    return

        elif processor == 'aggr_processor':
            if running_dict.has_key('aggr_processor') and running_dict['aggr_processor'] == 'Running':
                try:
                    cmd_line = self.cmd_stop_aggr_naiv
                    cmd = subprocess.Popen([cmd_line],shell=True,stderr=subprocess.PIPE)
                    print ('aggr_processor has been stopped!')
                    return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('aggr_processor'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Aggregator processor is already stopped! No action need to do!')
                    return

        elif processor == 'raw_writer':
            if running_dict.has_key('raw_writer') and running_dict['raw_writer'] == 'Running':
                try:
                    pid = runnind_pids.get('raw_writer')
                    if not pid:
                        print "No raw_writer processor need to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print('raw_writer processor has been stopped')
                        return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('raw_writer'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Raw_writer processor is already stopped! No action need to do!')
                    return

        elif processor == 'novelty':
            if running_dict.has_key('novelty') and running_dict['novelty'] == 'Running':
                try:
                    pid = runnind_pids.get('novelty')
                    if not pid:
                        print "No novelty processor need to stop"
                        # sys.exit(1)
                        return
                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print ('novelty has been stopped!')
                        return
                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('novelty'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return
                else:
                    print('Novelty_detector processor is already stopped! No action need to do!')
                    return

        elif processor == 'web_management':
            if running_dict.has_key('web_management') and running_dict['web_management'] == 'Running':
                try:
                    pid = runnind_pids.get('web_management')
                    if not pid:
                        print ("No web_management processor need to stop")
                        # sys.exit(1)

                    else:
                        kill_cmd = "kill -s TERM "+pid
                        os.popen(kill_cmd)
                        print "Executed:"+kill_cmd
                        print ('Flask has been stopped!')

                except Exception as ex:
                    print("    Failed to stop processor with ERROR(%s).", str(ex))
                    sys.exit(1)
            else:
                if not running_dict.has_key('web_management'):
                    print('Please type correct command! You may use "help stop" see more help')
                    return

                else:
                    print('web_management is already stopped! No action need to do!')
                    return

        else:
            print ('Please type correct command! You may use "help stop" see more help')

    def do_exit(self, line):
        """
        Exit the RTA command line
        """
        return True

if __name__ == "__main__":
    ProcessCMD().cmdloop()
