package com.procure.thg.cockroachdb;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class App {

  private static final Logger LOGGER = Logger.getLogger(App.class.getName());

  private static final String THRESHOLD_SECONDS = "THRESHOLD_SECONDS";
  private static final String FOLDER = "FOLDER";
  private static final Region REGION = Region.EU_WEST_1; // Change to your bucket's region
  private static final String AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL";

  public static void main(String[] args) {
    LOGGER.log(INFO, "Initialising S3 client...");
    final S3Client s3Client = S3Client.builder()
        .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
        .endpointOverride(getEndpointUri())
        .region(REGION)
        .forcePathStyle(true)
        .build();

    final S3Cleaner cleaner = new S3Cleaner(s3Client, getThresholdSeconds(), getFolderPrefix());
    cleaner.cleanOldObjects();

    s3Client.close();
    LOGGER.log(INFO, "S3 client closed");
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
    final var folderPrefix = System.getenv(FOLDER);
    if (folderPrefix == null || folderPrefix.isEmpty()) {
      var msg = FOLDER + " environment variable not set";
      LOGGER.log(SEVERE, msg);
      throw new IllegalArgumentException(msg);
    }
    return folderPrefix;
  }
}
