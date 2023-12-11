FROM amazoncorretto:17.0.9-alpine3.17
WORKDIR /app
COPY ./build/libs/rest-raft-0.0.1.jar .
ENTRYPOINT ["java", "-jar", "./rest-raft-0.0.1.jar"]
