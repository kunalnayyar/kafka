import os
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from typing import List
load_dotenv()

API_KEY = os.getenv('API_KEY')
ENDPOINT_SCHEMA_URL =os.getenv('ENDPOINT_SCHEMA_URL')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
BOOTSTRAP_SERVER =os.getenv('BOOTSTRAP_SERVER')
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = os.getenv('SCHEMA_REGISTRY_API_KEY')
SCHEMA_REGISTRY_API_SECRET = os.getenv('SCHEMA_REGISTRY_API_SECRET') 


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }

class Restaurant:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        self.record = record
    @staticmethod
    def dicttoresto(resto:dict,ctx):
        return Restaurant(record = resto)

    def __str__(self):
        return f"{self.record}"

def main(topic):
    schema_registry_client = SchemaRegistryClient(schema_config())
    restaurants:List[Restaurant]=[]
    # getting latest schema using id
    latest_schema_value = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str

    json_deserializer = JSONDeserializer(latest_schema_value,from_dict = Restaurant.dicttoresto)
    consumer_conf = sasl_conf()
    consumer_conf.update({'group.id' : 'group1', 'auto.offset.reset':'earliest' })
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            resto = json_deserializer(msg.value(),SerializationContext(msg.topic,MessageField.VALUE))

            if resto is not None:
                print("User record {}: Restaurant: {}\n".format(msg.key(),resto))
                restaurants.append(resto)

        except KeyboardInterrupt:
            break
    consumer.close()

    print("Number of records consumed by consumer 2 : {}".format(len(restaurants)))
    #Number of records consumed by consumer 2 : 74818 (with different group id) 
    #Number of records consumed by consumer 2 : 49966 (with same group id)
    


main("restaurent-take-away-data")