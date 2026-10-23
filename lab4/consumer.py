import config
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer


class Consumer:
    def __init__(self):
        self.totals = {topic: 0 for topic in config.TOPICS}
        self.counts = {topic: 0 for topic in config.TOPICS}

        self.consumer = KafkaConsumer(
            *config.TOPICS,
            bootstrap_servers=config.BOOTSTRAP_SERVERS,
            group_id=config.CONSUMER_GROUP,
            auto_offset_reset=config.OFFSET_RESET,
            value_deserializer=JsonSerializer(config.ENCODING),
        )

    def calculate_average(self, topic):
        count = self.counts[topic]
        return self.totals[topic] / count if count else 0

    def process_message(self, message):
        topic = message.topic
        data = message.value

        self.totals[topic] += data[config.PROCESSING_TIME]
        self.counts[topic] += 1

        average = self.calculate_average(topic)

        print(config.RECEIVED_MESSAGE.format(
            topic=topic, partition=message.partition, client=data[config.CLIENT_ID],
            average=average, unit=config.TOPIC_UNITS[topic], count=self.counts[topic]
        ))

    def print_summary(self):
        print(config.SUMMARY_HEADER)

        for topic in config.TOPICS:
            count = self.counts[topic]

            if count == 0:
                print(config.EMPTY_SUMMARY.format(topic=topic))
                continue

            average = self.calculate_average(topic)
            print(config.SUMMARY_LINE.format(topic=topic, count=count, average=average, unit=config.TOPIC_UNITS[topic]))

    def run(self):
        print(config.WAITING_MESSAGE)

        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            print(config.STOPPED_MESSAGE)
        finally:
            self.consumer.close()
            self.print_summary()


if __name__ == "__main__":
    consumer = Consumer()
    consumer.run()