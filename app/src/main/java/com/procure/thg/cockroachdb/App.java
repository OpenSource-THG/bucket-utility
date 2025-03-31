package com.procure.thg.cockroachdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

public class App {

  private static final Logger LOGGER = Logger.getLogger(App.class.getName());

  private static final String THRESHOLD_SECONDS = "THRESHOLD_SECONDS";
  private static final String FOLDER = "FOLDER";
  private static final String ENABLE_MOVE = "ENABLE_MOVE";
  private static final String TARGET_AWS_ACCESS_KEY_ID = "TARGET_AWS_ACCESS_KEY_ID";
  private static final String TARGET_AWS_SECRET_ACCESS_KEY = "TARGET_AWS_SECRET_ACCESS_KEY";
  private static final String TARGET_AWS_ENDPOINT_URL = "TARGET_AWS_ENDPOINT_URL";
  private static final String TARGET_BUCKET_NAME = "TARGET_BUCKET_NAME";
  private static final String TARGET_FOLDER = "TARGET_FOLDER";
  private static final Region REGION = Region.EU_WEST_1; // Adjust to your region
  private static final String AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL";

  public static void main(String[] args) {
    S3Client sourceClient = null;
    S3Client targetClient = null;
    try {
      LOGGER.log(INFO, "Initialising source S3 client...");
      sourceClient = S3Client.builder()
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .endpointOverride(getEndpointUri())
        .region(REGION)
        .forcePathStyle(true)
        .build();

      String enableMoveStr = System.getenv(ENABLE_MOVE);
      boolean enableMove = Boolean.parseBoolean(enableMoveStr);

      long thresholdSeconds = getThresholdSeconds();
      String folder = getFolderPrefix();

      if (enableMove) {
        String targetAccessKey = System.getenv(TARGET_AWS_ACCESS_KEY_ID);
        String targetSecretKey = System.getenv(TARGET_AWS_SECRET_ACCESS_KEY);
        String targetEndpoint = System.getenv(TARGET_AWS_ENDPOINT_URL);
        String targetBucket = System.getenv(TARGET_BUCKET_NAME);
        String targetFolder = System.getenv(TARGET_FOLDER); // Can be null

        if (targetAccessKey == null || targetSecretKey == null || targetEndpoint == null || targetBucket == null) {
          throw new IllegalArgumentException("Required target environment variables (TARGET_AWS_ACCESS_KEY_ID, TARGET_AWS_SECRET_ACCESS_KEY, TARGET_AWS_ENDPOINT_URL, TARGET_BUCKET_NAME) must be set when ENABLE_MOVE is true");
        }

        LOGGER.log(INFO, "Initialising target S3 client...");
        targetClient = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(targetAccessKey, targetSecretKey)))
                .endpointOverride(URI.create(targetEndpoint))
                .forcePathStyle(true)
                .region(REGION)
                .build();

        S3Copier copier = new S3Copier(sourceClient, System.getenv("BUCKET_NAME"), folder,
                targetClient, targetBucket, targetFolder);
        copier.copyRecentObjects(thresholdSeconds);
      }

      S3Cleaner cleaner = new S3Cleaner(sourceClient, thresholdSeconds, folder);
      cleaner.cleanOldObjects();
    } catch (Exception e) {
      LOGGER.log(SEVERE, "Application failed", e);
      throw e;
    } finally {
      if (targetClient != null) {
        targetClient.close();
      }
      if (sourceClient != null) {
        sourceClient.close();
      }
      LOGGER.log(INFO, "S3 clients closed");
    }
  }

  private static URI getEndpointUri() {
    final var uri = System.getenv(AWS_ENDPOINT_URL);
    if (uri == null || uri.isEmpty()) {
      var msg = AWS_ENDPOINT_URL + " environment variable not set";
      LOGGER.log(SEVERE, msg);
      throw new IllegalArgumentException(msg);
    }
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      LOGGER.log(SEVERE, "Invalid endpoint URI: {0}", uri);
      throw new IllegalArgumentException(e);
    }
  }

  private static long getThresholdSeconds() {
    final var thresholdEnv = System.getenv(THRESHOLD_SECONDS);
    if (thresholdEnv == null || thresholdEnv.isEmpty()) {
      var msg = THRESHOLD_SECONDS + " environment variable not set";
      LOGGER.log(SEVERE, msg);
      throw new IllegalArgumentException(msg);
    }
    return Long.parseLong(thresholdEnv);
  }

  private static String getFolderPrefix() {
      return System.getenv(FOLDER);
  }
}