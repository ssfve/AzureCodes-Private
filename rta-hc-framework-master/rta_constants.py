'''
Created on Mar 11, 2016

@author: planat
'''
YARN_EXECUTION_MODE = "YARN"
LOCAL_EXECUTION_MODE = "LOCAL"

FRAMEWORK_NAME = "framework"               # used in rta.properties file
PROPERTIES_FILE_PATH = "/opt/rta_hc/rta-hc-framework-master/conf/"
PROPERTIES_FILE = "/opt/rta_hc/rta-hc-framework-master/conf/rta.properties"
PROPERTIES_LOG_FILE = "/opt/rta_hc/rta-hc-framework-master/conf/log.properties"

RTA_ACTIVITY_WALKING = "WALKING"
RTA_ACTIVITY_SITTING = "SITTING"
RTA_ACTIVITY_WALKING_UP = "WALKING_UP"
RTA_ACTIVITY_WALKING_DOWN = "WALKING_DOWN"

RTA_PROCESSOR_STATE_INITIALIZED = "INITIALIZED"
RTA_PROCESSOR_STATE_STARTED = "STARTED"
RTA_PROCESSOR_STATE_PAUSED = "PAUSED"
RTA_PROCESSOR_STATE_STOPPED = "STOPPED"
RTA_PROCESSOR_STATE_DESTROYED = "DESTROYED"

RTA_PROCESSOR_ORDER_INITIALIZE = "INITIALIZE"
RTA_PROCESSOR_ORDER_START = "START"
RTA_PROCESSOR_ORDER_PAUSE = "PAUSE"
RTA_PROCESSOR_ORDER_STOP = "STOP"
RTA_PROCESSOR_ORDER_DESTROY = "DESTROY"

SENSORS_ACTIVITY_TYPE   = ["accelerometer","gyroscope","altimeter","ambientLight","barometer","calorie","distance","uv","contact","pedometer"]
SENSORS_PHYSIO_TYPE     = ["skinTemperature","heartRate","gsr","rrinterval"]
PREDICT_TYPE            = ['spark','sklearn']

# used by the find_closest_ts routine (databuffer algorithm)
CLOSEST_CHOICE_BACKW_FORW = "BACKW_FWD"
CLOSEST_CHOICE_BACKW = "BACKW"
CLOSEST_CHOICE_FORW = "FWD"

# Data model path
#DATA_MODEL_HOME_PATH = "/tmp/models"

#sklearn properties setting
SKLEARN = 'sklearn'

#plat_form_mon properties setting
PLATFORM_MON = 'platform_mon'
