FROM eclipse-temurin:21-jdk AS builder

WORKDIR /app

# Clonează și construiește aplicația
RUN apt-get update && apt-get install -y git && \
    rm -rf ./target && \
    git clone https://github.com/viamandi/hs.git . && \
    ./mvnw clean package -DskipTests

FROM eclipse-temurin:21-jre

WORKDIR /app

# Copiază JAR-ul construît
COPY --from=builder /app/target/*.jar app.jar

# Expune portul (schimbă dacă e necesar)
EXPOSE 8080

# Pornește aplicația
ENTRYPOINT ["java", "-jar", "/app/app.jar"]