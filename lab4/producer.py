import time
import random
import config
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from kafka.serializer import DefaultSerializer, JsonSerializer


class Producer:
    TOPICS = config.TOPICS
    TIME_RANGES = config.TIME_RANGES
    CLIENT_COUNT = config.CLIENT_COUNT
    CLIENT_PREFIX = config.CLIENT_PREFIX

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            key_serializer=DefaultSerializer(config.ENCODING),
            value_serializer=JsonSerializer(config.ENCODING),
        )

    @staticmethod
    def create_topics():
        admin = KafkaAdminClient(bootstrap_servers=config.BOOTSTRAP_SERVERS)

        try:
            existing_topics = set(admin.list_topics())

            missing_topics = {
                topic: {
                    "num_partitions": config.PARTITIONS,
                    "replication_factor": config.REPLICATION_FACTOR,
                }
                for topic in config.TOPICS if topic not in existing_topics
            }

            if missing_topics:
                admin.create_topics(new_topics=missing_topics, wait_for_metadata=True)
        finally:
            admin.close()

    @classmethod
    def generate_message(cls):
        topic = random.choice(cls.TOPICS)
        client_id = f"{cls.CLIENT_PREFIX}{random.randint(1, cls.CLIENT_COUNT)}"
        time_range = cls.TIME_RANGES[topic]

        data = {
            config.CLIENT_ID: client_id,
            config.PROCESSING_TIME: random.randint(*time_range),
        }

        return topic, client_id, data

    def send_message(self, topic, client_id, data):
        result = self.producer.send(topic, key=client_id, value=data).get(timeout=config.SEND_TIMEOUT)
        print(config.SENT_MESSAGE.format(topic=topic, partition=result.partition, data=data))

    def run(self):
        try:
            self.create_topics()

            for _ in range(config.MESSAGE_COUNT):
                topic, client_id, data = self.generate_message()
                self.send_message(topic, client_id, data)
                time.sleep(config.SEND_INTERVAL)
        finally:
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    producer = Producer()
    producer.run()