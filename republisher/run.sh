#!/bin/sh

java -jar /jars/mq-republisher-main.jar &
java -jar /jars/kafka-republisher-main.jar
