# schema-validation-example
 
Just checking if confluent enterprise perform the actual validation of the Avro payload. The scenario is the following:

- send the correct Avro record. It will register the schema with id 1
  ```
   Sending normal record and registering schema with id 1 ...
   key=12345, value={"tradeNumber": 12345, "registeredName": "MyCompany"} => partition=0, offset=0
  ```
- send the random number of bytes with the non-existing schema Id - throws the exception
  ```
   Sending wrong record with non-existing id ...
   Exception org.apache.kafka.common.InvalidRecordException: 
   One or more records have been rejected due to 1 record errors in total, and only showing the first three errors at most: 
   [RecordError(batchIndex=0, message='Log record DefaultRecord(offset=0, timestamp=1666711490360, key=5 bytes, value=9 bytes) 
   is  rejected by the record interceptor io.confluent.kafka.schemaregistry.validator.RecordSchemaValidator')]
  ```
- send the random number of bytes with the correct schema Id - the message is sent to Kafka and successfully stored
  ```
   Sending wrong record with existing id ...
   key=22222, value=[B@662f5666 => partition=0, offset=1
  ```
How to reproduce:

1. Run docker compose 

`docker-compose up -d`

2. Create a topic with a validation enabled

`docker exec -ti  broker /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic company --config confluent.value.schema.validation=true`

3. Run AvroProducer class
