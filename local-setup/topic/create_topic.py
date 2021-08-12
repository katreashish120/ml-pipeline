from confluent_kafka.admin import AdminClient, NewTopic
import sys

admin_client = AdminClient({
    "bootstrap.servers": "localhost:9092"
})

topic_list = []
topic_list.append(NewTopic(sys.argv[1], 1, 1))
admin_client.create_topics(topic_list)