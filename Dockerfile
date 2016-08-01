FROM quay.io/orgsync/java:1.8.0_66-b17

WORKDIR /code
COPY . /code/

RUN apt-get update \
  && apt-get install -y maven \
  && mvn package -DskipTests \
  && mkdir -p /opt/oskr-events \
  && cp /code/target/oskr-events.jar /opt/oskr-events/ \
  && apt-get remove --purge -y maven \
  && apt-get autoremove -y \
  && apt-get clean \
  && rm -Rf /var/lib/apt/lists/* /tmp/* /var/tmp/* /root/.m2 /code

WORKDIR /opt/oskr-events
