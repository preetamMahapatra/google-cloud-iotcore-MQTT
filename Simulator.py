import argparse
import datetime
import os
import time
import socket
import random
from random import randint
import json
import jwt
import paho.mqtt.client as mqtt

def get_ip_address():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

# [START iot_mqtt_jwt]
def create_jwt(project_id, private_key_file, algorithm):
    
    token = {
            # The time that the token was issued at
            'iat': datetime.datetime.utcnow(),
            # The time the token expires.
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            # The audience field should always be set to the GCP project id.
            'aud': project_id
    }

    # Read the private key file.
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file))

    return jwt.encode(token, private_key, algorithm=algorithm)
# [END iot_mqtt_jwt]

# [START iot_mqtt_config]
def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return '{}: {}'.format(rc, mqtt.error_string(rc))

def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print('on_connect', mqtt.connack_string(rc))

def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print('on_disconnect', error_str(rc))

def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    #print('on_publish')

def get_client(
        project_id, cloud_region, registry_id, device_id, private_key_file,
        algorithm, ca_certs, mqtt_bridge_hostname, mqtt_bridge_port):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client = mqtt.Client(
            client_id=('projects/{}/locations/{}/registries/{}/devices/{}'
                       .format(
                               project_id,
                               cloud_region,
                               registry_id,
                               device_id)))

    # With Google Cloud IoT Core, the username field is ignored, and the
    # password field is used to transmit a JWT to authorize the device.
    client.username_pw_set(
            username='unused',
            password=create_jwt(
                    project_id, private_key_file, algorithm))

    # Enable SSL/TLS support.
    client.tls_set(ca_certs=ca_certs)

    # Register message callbacks. https://eclipse.org/paho/clients/python/docs/
    # describes additional callbacks that Paho supports. In this example, the
    # callbacks just print to standard out.
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    # Connect to the Google MQTT bridge.
    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    # Start the network loop.
    client.loop_start()

    return client
# [END iot_mqtt_config]

# [START iot_mqtt_run]
def main():
    ip = get_ip_address()
    hostname = socket.gethostname()
    # Need to change this config start
    device_id = ''
    project_id = ''
    cloud_region = 'us-central1'
    registry_id = ''
    private_key_file = '/home/xx/.ssh/ec_private.pem'
    ca_certs = '/home/xx/.ssh/roots.pem'
    # change end 
    algorithm = 'ES256'
    jwt_expires_minutes = 60
    mqtt_bridge_hostname = 'mqtt.googleapis.com'
    mqtt_bridge_port = 8883

    # Publish to the events or state topic based on the flag.
    sub_topic = 'events'

    mqtt_topic = '/devices/{}/{}'.format(device_id, sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = jwt_expires_minutes

    client = get_client(project_id, cloud_region, registry_id, 
        device_id,private_key_file, algorithm, ca_certs,
        mqtt_bridge_hostname, mqtt_bridge_port)

    data_file = 'data/SampleData.json'
    fr = open(data_file, 'r')
    i = 1 
    for line in fr:
        data = json.loads(line)
        payload = json.dumps(data)    # JSON object to string conversion
        print('Publishing message #{}: \'{}\''.format(i, payload))
        client.publish(mqtt_topic, payload, qos=1)
        i += 1
        time.sleep(0.1)

    # End the network loop and finish.
    client.loop_stop()
    print('Finished.')
# [END iot_mqtt_run]

if __name__ == '__main__':
    main()
