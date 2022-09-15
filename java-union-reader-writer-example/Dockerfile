FROM adoptopenjdk/openjdk11:alpine-slim

ARG JAVA_MAIN_CLASS
ENV JAVA_MAIN_CLASS=$JAVA_MAIN_CLASS

RUN mkdir /opt/app
RUN cd /opt/app

COPY . .
RUN ./mvnw clean install

WORKDIR /opt/app

CMD java -cp ./target/union-reader-writer-example.jar $JAVA_MAIN_CLASS