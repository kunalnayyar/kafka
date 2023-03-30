import os
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import pandas as pd
from dotenv import load_dotenv


load_dotenv()
file_path = "restaurant_orders.csv"
API_KEY = os.getenv('API_KEY')
ENDPOINT_SCHEMA_URL =os.getenv('ENDPOINT_SCHEMA_URL')
API_SECRET_KEY = os.getenv('API_SECRET_KEY')
BOOTSTRAP_SERVER =os.getenv('BOOTSTRAP_SERVER')
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = os.getenv('SCHEMA_REGISTRY_API_KEY')
SCHEMA_REGISTRY_API_SECRET = os.getenv('SCHEMA_REGISTRY_API_SECRET') 

"""
keeping below schema version as v1 in confluent-kafka
"""
schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "restaurant take away value schema",
  "properties": {
    "Order Number": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Order Date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Item Name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Product Price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Total products": {
      "description": "The type(v) type is used.",
      "type": "number"
    }
  },
  "title": "Restaurant-take-away-Record",
  "type": "object"
}
    """

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

	def __str__(self):
		return f"{self.record}"

def getRestaurantInstance(file_path):
	df = pd.read_csv(file_path)
	columns = list(df.columns)
	for orders in df.values:
		order = Restaurant(dict(zip(columns,orders)))
		yield order

def resttodict(resto:Restaurant,ctx):

	return resto.record

def delivery_report(err,msg):

	if err is not None:
		print("Delivery failed for User record {}: {}".format(msg.key(), err))
		return
	print('User record {} successfully produced to {} [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main(topic):

	schema_registry_client = SchemaRegistryClient(schema_config())
	subjects = schema_registry_client.get_subjects()
	#['restaurent-take-away-data-key', 'demo_topic-value', 'demo_topic-key', 'restaurent-take-away-data-value']
	latest_schema_value = schema_registry_client.get_latest_version('restaurent-take-away-data-value').schema.schema_str
	#print(latest_schema)
	latest_schema_key = schema_registry_client.get_latest_version('restaurent-take-away-data-key').schema.schema_str
	#print(latest_schema_key) # "utf-8" in confluent-kafka schema
	string_serializer = StringSerializer(latest_schema_key)
	json_serializer = JSONSerializer(latest_schema_value,schema_registry_client,resttodict)
	
	producer = Producer(sasl_conf())
	producer.poll(0.0)
	try:
		for resto in getRestaurantInstance(file_path=file_path):
			print(resto)
			producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()),resttodict),
                            value=json_serializer(resto, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
			#break
	except KeyboardInterrupt:
		pass
	except ValueError:
		print("Invalid input, discarding record...")
		pass
	print("\nFlushing records...")
	producer.flush()

main("restaurent-take-away-data")
