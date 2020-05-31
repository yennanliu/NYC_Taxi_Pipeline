#------------------------
# SPARK 2.1.0 SCALA
#------------------------

FROM srdc/scala:2.11.7
MAINTAINER yennj12 

ENV SPARK_VERSION 2.1.0

# update ca-certificates 
# https://stackoverflow.com/questions/58338266/curl-certificate-fail-in-docker-container
# https://stackoverflow.com/questions/42292444/how-do-i-add-a-ca-root-certificate-inside-a-docker-image
#RUN curl https://curl.haxx.se/ca/cacert.pem > my_ca_root.crt
#ADD my_ca_root.crt /usr/local/share/ca-certificates/my_ca_root.crt
#RUN chmod 644 /usr/local/share/ca-certificates/my_ca_root.crt && update-ca-certificates

# spark
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz | tar -xz -C /opt
RUN cd /opt && ln -s ./spark-$SPARK_VERSION-bin-hadoop2.7 spark

ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

CMD ["bash"]