# Use the official Eclipse Temurin OpenJDK runtime as a parent image
FROM eclipse-temurin:17.0.17_10-jre

# Set the working directory inside the container
WORKDIR /app

# Copy the application's JAR file to the working directory
COPY build/libs/app-all.jar /app/app.jar

# Copy the entrypoint script to the working directory
COPY entrypoint.sh /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Run as an unprivileged user
RUN useradd --system --uid 10001 --no-create-home appuser
USER 10001

# Run the entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]
