FROM bde2020/spark-java-template:1.5.1-hadoop2.6

MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

ENV SPARK_APPLICATION_MAIN_CLASS com.tenforce.bde.spark.demo.sensors.Application
ENV SPARK_APPLICATION_JAR_NAME spark-sensor-demo-1.1-with-dependencies

ENV HDFS_URL=hdfs://hdfs:9000

ENV APP_ARGS_OWNER=localhost
ENV APP_ARGS_MAX_DETAIL=128
ENV APP_ARGS_INPUT=/input
ENV APP_ARGS_OUTPUT=/output

ADD demo.sh /

RUN chmod +x /demo.sh

CMD ["/bin/bash", "/demo.sh"]