#------------------------
# SPARK 2.1.0 SCALA
#------------------------

FROM srdc/scala:2.11.7

MAINTAINER yennj12 

ENV SPARK_VERSION 2.1.0

# spark
RUN wget --no-check-certificate https://archive.apache.org/dist/spark/spark-2.1.0/spark-$SPARK_VERSION-bin-hadoop2.7.tgz 
RUN tar -xf spark-2.1.0-bin-hadoop2.7.tgz  -C /opt
RUN cd /opt && ln -s ./spark-$SPARK_VERSION-bin-hadoop2.7 spark

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

CMD ["bash"]