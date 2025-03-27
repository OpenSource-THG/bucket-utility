package com.procure.thg.cockroachdb;

import static java.util.logging.Level.INFO;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Cleaner {

  private static final Logger LOGGER = Logger.getLogger(S3Cleaner.class.getName());
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private final S3Client s3Client;
  private final long thresholdSeconds;
  private final String folderPrefix;  // New field for folder prefix

  public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folderPrefix) {
    this.s3Client = s3Client;
    this.thresholdSeconds = thresholdSeconds;
    this.folderPrefix = folderPrefix != null && !folderPrefix.isEmpty() ?
            folderPrefix.endsWith("/") ? folderPrefix : folderPrefix + "/"
            : null;
  }

  public void cleanOldObjects() {
    LOGGER.log(INFO, "Starting cleaner...");
    final var bucket = System.getenv(BUCKET_NAME);
    LOGGER.log(INFO, "Cleaning bucket: {0}", bucket);
    LOGGER.log(INFO, "Cleaning objects older than {0} seconds", thresholdSeconds);
    if (folderPrefix != null) {
      LOGGER.log(INFO, "Cleaning only within folder: {0}", folderPrefix);
    }

    final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

    ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder()
        .bucket(bucket)
        .prefix(folderPrefix)
        .build();

    ListObjectsV2Response listObjectsV2Response;
    do {
      listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);

      for (S3Object s3Object : listObjectsV2Response.contents()) {
        final String key = s3Object.key();

        if (key.endsWith("/") || (folderPrefix != null && key.equals(folderPrefix))) {
          continue;
        }


        if (s3Object.lastModified().isBefore(threshold)) {
          deleteObject(s3Client, bucket, key);
        }
      }

      listObjectsV2Request = ListObjectsV2Request.builder()
          .bucket(bucket)
          .prefix(folderPrefix)
          .continuationToken(listObjectsV2Response.nextContinuationToken())
          .build();
    } while (Boolean.TRUE.equals(listObjectsV2Response.isTruncated()));
    LOGGER.log(INFO, "Cleaning finished.");
  }

  private void deleteObject(final S3Client s3Client, final String bucket, final String key) {
    DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();

    s3Client.deleteObject(deleteObjectRequest);
    LOGGER.log(INFO, () -> "Deleted object: " + key);
  }
}