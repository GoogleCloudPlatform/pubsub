FROM gcr.io/distroless/java
ARG ROUTING_HOST

ADD mq-republisher/target/mq-republisher-main.jar /mq-republisher/mq-republisher-main.jar
EXPOSE 1883
ADD mq-republisher/config.xml /mq-republisher/conf/config.xml
ENV HIVE_MQ_CONFIG_FOLDER /mq-republisher/conf/

ADD kafka-republisher/target/kafka-republisher-main.jar /kafka-republisher/kafka-republisher-main.jar
EXPOSE 9092
ENV KAFKA_ROUTING_HOST=$ROUTING_HOST
# The port for kafka on the routing host.
ARG KAFKA_ROUTING_PORT=9092
ENV KAFKA_ROUTING_PORT=$KAFKA_ROUTING_PORT

ADD run.sh /run.sh
ENTRYPOINT /run.sh
