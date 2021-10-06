FROM gcr.io/distroless/java

ADD mq-republisher/target/mq-republisher-main.jar /mq-republisher/mq-republisher-main.jar
EXPOSE 1883
ADD mq-republisher/config.xml /mq-republisher/conf/config.xml
ENV HIVE_MQ_CONFIG_FOLDER /mq-republisher/conf/

ADD run.sh /run.sh
ENTRYPOINT /run.sh
