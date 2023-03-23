from typing import List

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def create_topics(admin, topics: List[NewTopic]):
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/#confluent_kafka.admin.AdminClient.create_topics
    # Returns
    # A dict of futures for each topic, keyed by the topic name. The future result() method returns None
    results_dict = admin.create_topics(topics)

    for topic, future in results_dict.items():
        try:
            future.result()  # The result itself is None
            print(f"Topic created: {topic}")
        except KafkaException as ex:
            print(f"Failed to create topic {topic}: {ex}")


def filter_new_topics(admin: AdminClient, topics: List[NewTopic]) -> List[NewTopic]:
    # metadata: confluent_kafka.admin._metadata.ClusterMetadata
    metadata = admin.list_topics()

    existing_topics = set(t.topic for t in metadata.topics.values())
    new_topics = [t for t in topics if t.topic not in existing_topics]

    return new_topics


if __name__ == "__main__":
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/#module-confluent_kafka.admin
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})

    topics = [
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/#newtopic
        # NewTopic(topic, num_partitions[, replication_factor][, replica_assignment][, config])
        NewTopic("rides", num_partitions=2, replication_factor=1),
        NewTopic("rides_metrics", num_partitions=2, replication_factor=1),
        NewTopic("rides_mirror", num_partitions=2, replication_factor=1),
    ]

    # Creating topics
    new_topics = filter_new_topics(admin, topics)
    if len(new_topics) > 0:
        create_topics(admin, new_topics)
