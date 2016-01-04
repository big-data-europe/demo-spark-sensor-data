FROM bde2020/spark-submit:1.5.1-hadoop2.6

MAINTAINER Erika Pauwels <erika.pauwels@tenforce.com>

ENV SPARK_APPLICATION_MAIN_CLASS com.tenforce.bde.spark.demo.sensors.Application

ENV APP_ARGS_OWNER=localhost
ENV APP_ARGS_INPUT=/data/input
ENV APP_ARGS_OUTPUT=/data/output

ADD demo.sh /
ADD . /usr/src/app

RUN apt-get install -y maven \
      && chmod +x /demo.sh \
      && update-java-alternatives -s java-1.8.0-openjdk-amd64 \
      && mkdir -p /app \
      && cd /usr/src/app \
      && mvn clean package \
      && cp target/spark-sensor-demo-1.1-SNAPSHOT-with-dependencies.jar /app/application.jar

CMD ["/bin/bash", "/demo.sh"]