# Kafka Producer/Consumer с Docker

Этот проект предоставляет Python-скрипт для взаимодействия с Apache Kafka с использованием Docker.

Предварительные требования

Docker
Docker Compose

Установка
git clone <repository_url>
cd <repository_directory>

Сборка образа
docker build -t kafka-client .

Запуск
docker-compose up -d

Использование

Отправка сообщения
docker run --network host kafka-client produce --message 'Hello World!' --topic 'hello_topic' --kafka '127.0.0.1:9092'

Получение сообщений
docker run --network host kafka-client consume --topic 'hello_topic' --kafka '127.0.0.1:9092'


