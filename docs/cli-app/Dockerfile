# ==============================================================================
# cli-app/Dockerfile   (guide step 39)
# ==============================================================================
# Same 4 lines as query-executor/Dockerfile (see there for the line-by-line
# explanation). Build the jar with `mvn clean package -DskipTests` first.
#
# Running an INTERACTIVE program in a container needs two things, and they're
# in docker-compose.yml, not here:
#   stdin_open: true  (= docker run -i)  keep standard input open so you can type
#   tty: true         (= docker run -t)  give the container a pseudo-terminal, so
#                                        JLine can do line editing, arrow keys, etc.
# And you start it with `docker compose run --rm cli` (attached to your
# terminal), not `docker compose up` (which runs services in the background and
# merges their logs).
#
# Remember the networking consequence: inside this container "localhost" is
# the container itself, so `connect --host localhost` won't reach a database
# running on your machine.
# ==============================================================================
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
