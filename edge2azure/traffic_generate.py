# !/usr/bin/python
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 03 17:01:56 2016

@author: planat

@author: Chen-Gang He
        2016-06-11
        1> refactor initial snippets
        2016-06-23
        1> Removed unnecessary comments and
	ELvin Lin
	1) added kafka functionality
"""
import glob
import json
import os
import sys
import time
import random
import argparse
import logging
import threading
from kafka import KafkaClient
from kafka import KafkaProducer

# LAMBD_INV = 3.
# NB_USERS = 3
# DURATION_TEST = 10000
import requests

TIME_2_WAIT_AFTER_POST_IN_MILLISECOND = 3000

# ==============================================================================
# Function
# ==============================================================================
def user_thread(thread_duration_in_millisecond, user, json_file_list, logger):
    """Thread main part to do POST operation. lifetime of thread is set to thread_duration
        Args:
            thread_duration_in_millisecond (int): millisecond number since UNIX epoch time
            user (str): user name
            json_file_dict_list (list): a list of dictionaries based on each JSON files
            servlet_url (str): servlet URL where processed data should be post
            logger (logging object): logging object
    Returns:
        None
    """
    
    thread_start_time = int(round(time.time() * 1000))
    # time for thread to be dead
    thread_end_time = thread_start_time + thread_duration_in_millisecond
    
    logger.debug("The thread is started from %s, and will be ended by %s"
                 , time.ctime(thread_start_time / 1000), time.ctime(thread_end_time / 1000))
    # Initial flag to determine if thread should be dead
    end_thread = False
    # Start posting
    post_count = 0
    
    try:
        while not end_thread:
            # Process each list data in json_file_list
            for line in json_file_list:
                # Make post_count start with 1 rather than 0
                post_count += 1
                # print("        [thread_%d|user:%s] Post JSON data at %d round(s)" % (thread_id, user, post_count))
                logger.debug("     Attempt to send JSON data at %d round(s)" % post_count)
                
                # Get kafka_topic from message body
                #kafka_topic = file_dict["eventtype"]
                kafka_topic = "iot_msband2_heartRate"
                kafka_message = line
                logger.debug("current Kafka_topic is %s" % kafka_topic)
                logger.debug("Kafka_message sent is %s" % kafka_message)

                #kafka produces a message and send it to a kafka broker    
                producer.send_messages(kafka_topic,kafka_message)
                logger.debug("Message sent successfully")
                #if r.status_code == requests.codes.ok:
                 #   logger.debug("       Succeeded in posting data at %d round(s)", post_count)
                #else:
                 #   logger.error("       Failed to post data at %d round(s) due to %s" % (post_count, r.text))
                
                time_to_sleep = int(TIME_2_WAIT_AFTER_POST_IN_MILLISECOND / 1000. + random.randint(-3, 3))
                logger.debug("     Sleep for %d seconds after sending message to kafka" % (time_to_sleep))
                time.sleep(time_to_sleep)
                current_time = int(round(time.time() * 1000))
                # check if it's time to stop processing
                if (current_time >= thread_end_time):
                    logger.info("==> Stop the thread_for user:%s which sent %d message(s) !" % (user, post_count))
                    # end_thread = True
                    return None
                    
    except KeyboardInterrupt:
        logger.info("==> Stop the thread_for user:%s which sent %d message(s) !" % (user, post_count))
        sys.exit(2)

def handle_commandline():
    """Do with script's arguments
        Args:
            None
    Returns:
        dict(list of users, traffic_ref_path, test_duration, lambd_inv, servlet_post_url)
    """
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-u", "--nb_users", type=str, help="Users to be simulated to send traffic JSON data, and it"
                                                           "should be enclosed by double quote and delimitered by "
                                                           "pipeline sign like "
                                                           "\"chen-gangh@hpe.com|he_chengang@hotmail.com",
                        required=True)
    parser.add_argument("-p", "--traffic_ref_path", type=str,
                        help="the file system path where the traffic reference files are stored", default="./",
                        required=True)
    
    parser.add_argument("-d", "--test_duration", type=int, help="(ms) Life_time of each individual user thread"
                                                                " (the same for all of them)", default=10000)
    
    parser.add_argument("-l", "--lambd_inv", type=int, help="The average duration (in seconds) between each user thread"
                                                            " startup (poisson distribution)", default=3)
    
    #parser.add_argument("-s", "--servlet_post_url", type=str, help="acquisition Servlet URL",
    #                    default="http://c2t08977.itcs.hpecorp.net:9797/simpleiothttp2kafka/raw_msband_v2")
    
    args = parser.parse_args()
    argsdict = vars(args)
    return argsdict


def parse_json_file(file_name_path):
    """Parse file_name_path in JSON and replace "userID" with user
    Args:
        file_name_path (str): absolute path and file name
    Returns:
        list
    """
    
    f_in = open(file_name_path)
    data_lines = f_in.readlines()
    return data_lines


def get_files(in_path_file, logger):
    """Get files to be processed for arguments and return a list of files to be processed
    Args:
        in_path_file (str): file directory or files with wild character like "?" "*" or a single file which is to
        be processed.
        logger (logger object): logger object
    Returns:
        list of full path of each data file
    """

    # make sure there is no "\" or "/" appended to directory
    path_file = in_path_file.rstrip("\\").rstrip('/')
    # Check if DATA_PATH exists or not. If not, exit with error singal
    if not os.path.exists(path_file):
        if len(glob.glob(path_file)) == 0:
            # print("The DATA directory or file %s DOES NOT exist and exit abnormally!" % in_path_file)
            logger.error("The DATA directory or file %s DOES NOT exist and exit abnormally!" % in_path_file)
            sys.exit(1)
        else:
            # Means there are files match in_file_path if wild charaters are used. C;\\*.sql
            return glob.glob(path_file)
    elif len(glob.glob(path_file)) == 1 and not os.path.isdir(path_file):
        # Means path_file is an existing file
        return glob.glob(path_file)
    elif len(os.listdir(path_file)) == 0:
        # Means in_path_file is an empty directory.
        logger.debug("The directory %s is an empty directory, and EXIST!" % in_path_file)
        sys.exit(0)
    else:
        # path_file is an existing directory and consists of several files
        file_list = os.listdir(path_file)
        # print file_list
        # Sort files in timestamp in file_name like 20160223171929_heartRate.json
        # to be processed.
        file_list.sort(key=lambda x: x.split('_')[0])
        # print file_list
        return [os.path.join(path_file, ff) for ff in file_list]

# Main part
# Create NB_USERS threads one y one in sequence with an random (poisson) sleep time
def main():
    
    global producer
    os.environ['NO_PROXY'] = 'stackoverflow.com'
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)-5s] (%(threadName)-28s) %(message)-20s', )

    # Handle arguments
    args_dict = handle_commandline()
    logging.debug("    %s", args_dict)

    # Get files to be processed
    logging.info("==> Start traffic generation to servlet server ...")
    logging.info("==> Get files to be processed ...")
    
    files_2_processed = get_files(args_dict["traffic_ref_path"], logging)
    
    logging.debug("    %s", files_2_processed)

    # Parse files to be processed
    logging.info("==> Parse files to be processed ...")
   
    # create kafka client
    #kafka = KafkaClient("localhost:9092")
    logging.info("==> kafkaClient connection is successful ...")
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	  
    id = 0 
    for file in files_2_processed:
        parsed_files_list = parse_json_file(file)
        id += 1
        # Spawn thread for each user
        threads = list()
        try:
            for user in args_dict["nb_users"].split('|'):
            
                normalized_user = user.strip()
                # print ("==> Start thread_%d for user %s" % (id, normalized_user))
                # thread.start_new_thread(name= "thread_" + id, user_thread, (id, args_dict[2], normalized_user,
                # parsed_files_list, args_dict[4]))
                print ("==> sending datafile %d%d%d%d%d%d%d%d%d%d%d%d content" % (id,id,id,id,id,id,id,id,id,id,id,id)) 
	       
                #print parsed_files_list
                t = threading.Thread(name="thread_" + str(id) + "|" + user, target=user_thread,
                                 args=(args_dict["test_duration"], normalized_user, parsed_files_list, logging))
                t.setDaemon(True)
                threads.append(t)
                t.start()
            
                sleep_time = int(random.expovariate(1.0 / float(args_dict["lambd_inv"])))
                # sleep_time = 2
                time.sleep(sleep_time)

                # Wait until last thread is done
                for th in threads:
                    th.join(30000)
                
                # t.join()
                logging.debug("==> Wait for interrupting by typing control+c")
    		
        except KeyboardInterrupt:
            sys.exit(2)

# ==============================================================================
# Main
# ==============================================================================
if __name__ == "__main__":
    main()
