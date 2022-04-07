from confluent_kafka import Producer
import socket
import json


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def store_to_kafka(topic, data):
    value = json.dumps(data)
    producer = Producer({
        'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()
    })
    producer.produce(topic, value=value, callback=acked)
    producer.poll(1)


def process_multiple_rows(topic, data, field):
    data_orig = data.copy()
    field_list = field.split(".")
    for key in field_list:
        try:
            try:
                data = json.loads(data[key])
            except:
                data = data[key]
        except Exception as e:
            return "error: {}".format(e)

    for d in data:
        index = 0
        for key in field_list:
            if len(field_list) == index + 1:
                try:
                    data_orig[field_list[index - 1]][key] = json.dumps(d)
                except:
                    data_orig[field_list[index - 1]][key] = d
            else:
                data_orig[key] = {}
            index += 1

        store_to_kafka(topic, data_orig)

    return "ok"
