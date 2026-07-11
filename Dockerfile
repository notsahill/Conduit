# syntax=docker/dockerfile:1
# Multi-stage: build the Spring Boot jar with a JDK, run it on a slim JRE.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
# Warm the dependency cache separately from source so code changes don't refetch deps.
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies -q || true
COPY src ./src
RUN ./gradlew --no-daemon clean bootJar -x test

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
RUN groupadd -r conduit && useradd -r -g conduit conduit
# bootJar emits build/libs/conduit-<version>.jar (the -plain jar is not built by this task).
COPY --from=build /workspace/build/libs/*-SNAPSHOT.jar app.jar
USER conduit
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
