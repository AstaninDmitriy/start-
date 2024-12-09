import logging
import random
import typing
from argparse import ArgumentParser

from confluent_kafka import Consumer, Producer, KafkaException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)

class Config(BaseModel):
    hosts: str
    topic: str
    client_id: int
    group_id: str 

    def model_dump(self):
        return {
            'bootstrap.servers': self.hosts,
            'client.id': self.client_id,
            'group.id': self.group_id
        }

class Message(BaseModel):
    message: typing.Any

class BaseKafkaClient:
    def __init__(self, config: Config):
        self._config = config

class KafkaProducer(BaseKafkaClient):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._producer = Producer(self._config.model_dump())

    def send_message(self, topic: str, message: Message) -> None:
        try:
            self._producer.produce(topic=topic, value=message.message.encode('utf-8'))
            self._producer.flush()
            logging.info(f"Message sent to topic {topic}: {message.message}")
        except KafkaException as e:
            logging.error(f"Failed to send message: {e}")

class KafkaConsumer(BaseKafkaClient):
    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self._consumer = Consumer(self._config.model_dump())

    def consume(self) -> None:
        try:
            self._consumer.subscribe([self._config.topic])
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    logging.info(f"Received message: {msg.value().decode('utf-8')}")
        except KeyboardInterrupt:
            logging.info('Program stopped.')
        finally:
            self._consumer.close()

def main():
    parser = ArgumentParser()
    parser.add_argument('mode', type=str, choices=['produce', 'consume'])
    parser.add_argument('--message', type=str, required=False)
    parser.add_argument('--topic', type=str, required=True)
    parser.add_argument('--kafka', type=str, required=True)
    params = parser.parse_args()

    # kafka_hosts = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    # if params.kafka not in kafka_hosts:
    #     raise ValueError('That host doesn\'t exist')

    config = Config(hosts=params.kafka, topic=params.topic, client_id=random.randint(1, 512), group_id='my_group')

    if params.mode == 'consume':
        consumer = KafkaConsumer(config=config)
        consumer.consume()
    else:
        if not params.message:
            raise ValueError('Message is required for produce mode')
        producer = KafkaProducer(config=config)
        producer.send_message(params.topic, Message(message=params.message))

if __name__ == '__main__':
    main()