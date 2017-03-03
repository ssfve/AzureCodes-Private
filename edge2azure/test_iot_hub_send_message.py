import requests
from urlparse import urlparse
#import urllib.parse
import datetime
import json
import base64
import hashlib
import time 
import urllib
import hmac


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
        print (sasToken)
        url = 'https://%s/devices/%s/messages/events?api-version=%s' % (self.iotHost, deviceId2, self.API_VERSION)
        r = requests.post(url, headers={'Authorization': sasToken}, data=message2, timeout=10)
        return r.text, r.status_code

if __name__ == '__main__':
    print('------ SENDING A MESSAGE ON THE IOT BUS ------')
    connectionString = 'HostName=msiothub2016.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=GKXEAW1ksgIN4C1CASIVcDuHkI6V6KFD+0lxEU0gDZk='
    print (connectionString)
    deviceId = 'healthc_rwriter'

    json_object = dict()
    json_object['DeviceID']     = deviceId
    json_object['TS_creation']  = datetime.datetime.isoformat(datetime.datetime.now())
    json_object['FieldText']    = "Some text value for a field"
    json_object['FieldInteger'] = 1264
    json_object['FieldFloat']   = 67.749
    json_object['FieldList']    = ['a','b',13,946]
    json_object['FieldDict']    = {'myfield':'myvalue', 'anotherfield':267}

    d2cMsgSender = D2CMsgSender(connectionString)
    message = json.dumps(json_object)
    print (message)
    answer = d2cMsgSender.sendD2CMsg(deviceId, message)
    print (answer)


