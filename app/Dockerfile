# Use an official OpenJDK runtime as a parent image
FROM eclipse-temurin:17.0.17_10-jre

# Set the working directory inside the container
WORKDIR /app

# Copy the application's JAR file to the working directory
COPY build/libs/app-all.jar /app/app.jar

# Copy the entrypoint script to the working directory
COPY entrypoint.sh /app/entrypoint.sh

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Run the entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]
