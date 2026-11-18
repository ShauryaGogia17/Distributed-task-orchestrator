FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /build
COPY services/worker-java/pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp dependency:go-offline
COPY services/worker-java/src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget fontconfig fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home worker
COPY --from=build /build/target/classes /app/classes
COPY --from=build /build/target/dependency /app/lib
USER worker
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -Djava.awt.headless=true"
EXPOSE 8007
ENTRYPOINT ["java", "-cp", "/app/classes:/app/lib/*", "io.taskscheduler.worker.WorkerApplication"]
