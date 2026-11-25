# ==============================================================================
# llm-service/Dockerfile   (guide step 23)
# ==============================================================================
# Byte-for-byte the same as query-executor/Dockerfile — read the comments
# there for the line-by-line explanation. Short version:
#   - start from a Java 21 runtime image
#   - copy the ALREADY-BUILT fat jar from target/ (run `mvn package` first!)
#   - run it with `java -jar`
#
# The API key is NOT baked into the image. docker-compose injects it at
# container start as the env var OPENAI_API_KEY (read from your .env file),
# and application.yml maps it to llm.api-key. Keeping secrets out of images is
# the right habit: images get pushed, shared and cached.
# ==============================================================================
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
