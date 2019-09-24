FROM java:8-jre

ENV VERTICLE_FILE target/queue_service-1.0.0-SNAPSHOT-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /opt/verticles

EXPOSE 8888

COPY $VERTICLE_FILE $VERTICLE_HOME/


WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["java -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory -jar queue_service-1.0.0-SNAPSHOT-fat.jar"]

