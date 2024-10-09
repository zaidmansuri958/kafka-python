from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AppConfigs:
    applicationID = "ProducerApp"
    bootstrapServers = "localhost:9092"
    topicName = "test-topic"
    numEvents = 10000


if __name__ == "__main__":
    logger.info("Creating kafka producer ...")

    producer = KafkaProducer(
        bootstrap_servers=AppConfigs.bootstrapServers,
        key_serializer=lambda k: k.to_bytes(
            4, 'big'),  # Integer key serializer
        value_serializer=lambda v: v.encode('utf-8')  # String value serializer
    )

    logger.info("Start sending message !!!!")
    for i in range(AppConfigs.numEvents):
        producer.send(AppConfigs.topicName, key=i,
                      value=f"Simple message - {i}")

    producer.flush()
    logger.info("Finished - Closing kafka producer !!!")
    producer.close()
