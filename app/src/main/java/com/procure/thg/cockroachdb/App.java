package com.procure.thg.cockroachdb;

import static java.util.logging.Level.INFO;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.S3EndpointProvider;

public class App {

  private static final Logger LOGGER = Logger.getLogger(App.class.getName());

  private static final String THRESHOLD_SECONDS = "THRESHOLD_SECONDS";
  private static final Region REGION = Region.US_EAST_1; // Change to your bucket's region

  public static void main(String[] args) {
    LOGGER.log(INFO, "Initialising S3 client...");
    final S3Client s3Client = S3Client.builder()
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .endpointProvider(S3EndpointProvider.defaultProvider())
        .region(REGION)
        .build();

    final S3Cleaner cleaner = new S3Cleaner(s3Client, getThresholdSeconds());
    cleaner.cleanOldObjects();

    s3Client.close();
    LOGGER.log(INFO, "S3 client closed");
  }

  public static long getThresholdSeconds() {
    final var thresholdEnv = System.getenv(THRESHOLD_SECONDS);
    if (thresholdEnv != null) {
      return Long.parseLong(thresholdEnv);
    }

    var propertyName = "threshold_seconds";
    var propertyValue = System.getProperty(propertyName);
    if (propertyValue != null) {
      return Long.parseLong(propertyValue);
    }

    final var properties = new Properties();
    try (InputStream input = App.class.getClassLoader().getResourceAsStream("config.properties")) {
      if (input == null) {
        throw new IllegalArgumentException("config.properties file not found");
      }
      properties.load(input);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load config.properties", e);
    }

    final String thresholdProp = properties.getProperty(propertyName);
    if (thresholdProp != null) {
      return Long.parseLong(thresholdProp);
    }

    throw new IllegalArgumentException("Threshold hours not found in environment or properties file");
  }
}
