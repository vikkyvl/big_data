BOOTSTRAP_SERVERS = "localhost:9092"

API_TOPIC = "api-request-time"
SUPPORT_TOPIC = "support-ticket-time"
TOPICS = (API_TOPIC, SUPPORT_TOPIC)

PARTITIONS = 2
REPLICATION_FACTOR = 1
CONSUMER_GROUP = "lab4-metrics"
OFFSET_RESET = "earliest"
ENCODING = "utf-8"
SEND_TIMEOUT = 10

CLIENT_ID = "client_id"
PROCESSING_TIME = "processing_time"
CLIENT_PREFIX = "client-"

MESSAGE_COUNT = 20
SEND_INTERVAL = 0.5
CLIENT_COUNT = 10

API_TIME_RANGE = (50, 500)
SUPPORT_TIME_RANGE = (5, 120)

TIME_RANGES = {
    API_TOPIC: API_TIME_RANGE,
    SUPPORT_TOPIC: SUPPORT_TIME_RANGE,
}

TOPIC_UNITS = {
    API_TOPIC: "ms",
    SUPPORT_TOPIC: "min",
}

SENT_MESSAGE = "Sent to {topic}, partition {partition}: {data}"
RECEIVED_MESSAGE = "{topic}, partition {partition}, client {client}: average = {average:.2f} {unit} ({count} records)"

WAITING_MESSAGE = "Waiting for messages. Press Ctrl+C to stop."
STOPPED_MESSAGE = "Consumer stopped."

SUMMARY_HEADER = "\nSUMMARY REPORT"
SUMMARY_LINE = "{topic}: {count} records, average = {average:.2f} {unit}"
EMPTY_SUMMARY = "{topic}: no messages processed"