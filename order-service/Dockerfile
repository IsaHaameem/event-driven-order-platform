# ── Stage 1: Build the jar with Maven (Java 25) ──
FROM maven:3.9-eclipse-temurin-25-alpine AS build
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# ── Stage 2: Slim runtime image, non-root user ──
FROM eclipse-temurin:25-jre-alpine
RUN addgroup -S spring && adduser -S spring -G spring
WORKDIR /app
COPY --from=build /build/target/order-service-0.0.1-SNAPSHOT.jar app.jar
RUN chown spring:spring app.jar
USER spring
EXPOSE 8081
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8081/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]