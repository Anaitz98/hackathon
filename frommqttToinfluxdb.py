from builtins import print

from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
import time
import json

db_host = 'localhost'
db_port = 8086
db_user = 'root'
db_password = 'root'
db_name = 'example'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True  # set flag
        print("connected OK Returned code =", rc)
        # client.subscribe(topic)
    else:
        print("Bad connection Returned code =", rc)


def on_message(client, userdata, msg):
    print(str(msg.payload))
    print(type(msg.payload))
    point_list = []
    # parse from bytes to string
    try:
        json_str = json.loads(msg.payload.decode("utf-8"))
        # we are only saving pressure into the database
        # customize your dictionary as required

        print( json_str['type'])
        if 'pressure' == json_str['type']:
            print('sartu da')
            point = {}
            point['measurement'] = 'preassure'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] = json_str['timestamp']
            point['fields'] = {'value': json_str['value']}
            point_list.append(point)
        elif 'temp' == json_str['type']:
            point = {}
            point['measurement'] = 'temp'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] = json_str['timestamp']
            point['fields'] = {'value': json_str['value']}
            point_list.append(point)
        elif 'speed_blade'== json_str['type']:
            point = {}
            point['measurement'] = 'speed_blade'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] = json_str['timestamp']
            point['fields'] = {'value': json_str['value']}
            point_list.append(point)
        elif 'speed_wind' == json_str['type']:
            point = {}
            point['measurement'] = 'speed_wind'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] = {'timestamp': json_str['timestamp']}
            point['fields'] = json_str['timestamp']
            point_list.append(point)
        elif 'energy' == json_str['type']:
            point = {}
            point['measurement'] = 'energy'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] =  json_str['timestamp']
            point['fields'] = {'value': json_str['value']}
            point_list.append(point)
        elif 'alarm' == json_str['type']:
            point = {}
            point['measurement'] = 'alarm'
            tags = {"host": "server01", "region": "us-west"}
            point['tags'] = tags
            point['time'] = json_str['timestamp']
            point['fields'] = {'value': json_str['value']}
            point_list.append(point)

        print(str(point_list) + 'asfad')

        save_data_into_influx(point_list)
    except Exception as e:
        print(e)

def save_data_into_influx(point_list: list):
    influx_client = InfluxDBClient(db_host, db_port, db_user, db_password, db_name)

    print('saving data')
    try:
        influx_client.write_points(point_list)
    except Exception as e:
        print(e)

    influx_client.close()


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print(string)


mqttc = mqtt.Client()

if __name__ == "__main__":
    mqttc.on_message = on_message
    mqttc.on_connect = on_connect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    # Uncomment to enable debug messages
    mqttc.on_log = on_log
    mqttc.username_pw_set(username="mqtt-user", password="ikerlankonnect")
    mqttc.connect("ec2-63-35-224-92.eu-west-1.compute.amazonaws.com", 1883, 60)
    mqttc.subscribe("UINT256", 0)
    mqttc.loop_start()


    while True:

        time.sleep(5)