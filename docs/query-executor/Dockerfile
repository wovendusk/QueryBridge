# ==============================================================================
# query-executor/Dockerfile   (guide step 16)
# ==============================================================================
# A Dockerfile is a recipe for building a container IMAGE. docker-compose.yml
# says `build: ./query-executor`, so `docker compose up` runs this recipe with
# the query-executor folder as the "build context" (the files Docker can see).
#
# All three app Dockerfiles in this project are IDENTICAL (same 4 lines). They
# work because each one runs in its own module folder, so `target/*.jar`
# picks up that module's own jar.
#
# ⚠ IMPORTANT: this Dockerfile does NOT compile anything. It copies a jar that
# must ALREADY exist in target/. So you must run `mvn clean package
# -DskipTests` on your machine BEFORE `docker compose up --build`, and re-run
# it every time you change code — otherwise Docker happily packages your OLD
# jar. (A "multi-stage" Dockerfile with a Maven build stage would remove that
# manual step, at the cost of slower image builds.)
# ==============================================================================

# Base image: Eclipse Temurin (a popular free OpenJDK build), Java 21, JRE only.
# JRE = just enough to RUN Java, no compiler — smaller than a full JDK image.
# Must be >= 21 because the code was compiled for Java 21 (see <java.version>).
FROM eclipse-temurin:21-jre

# All following commands run in /app inside the image (created if missing).
WORKDIR /app

# Copy the Spring Boot fat jar from the build context into the image as app.jar.
# The glob matches target/query-executor-1.0-SNAPSHOT.jar. (The original thin
# jar is renamed to *.jar.original by the Boot plugin, so it doesn't match.)
COPY target/*.jar app.jar

# The command run when a container starts. The "exec form" (JSON array) runs
# java directly as process #1, so it receives Docker's stop signal (SIGTERM)
# and Spring can shut down cleanly (closing Kafka consumers etc.).
ENTRYPOINT ["java", "-jar", "app.jar"]
