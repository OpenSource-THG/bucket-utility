package com.procure.thg.cockroachdb;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeParseException;
import java.util.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3Cleaner {

  private static final Logger LOGGER = Logger.getLogger(S3Cleaner.class.getName());
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private final S3Client s3Client;
  private final long thresholdSeconds;
  private final String folder;

  public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folder) {
    this.s3Client = s3Client;
    this.thresholdSeconds = thresholdSeconds;
    this.folder = folder != null && !folder.isEmpty() ?
            folder.endsWith("/") ? folder : folder + "/"
            : null;
  }

  public void cleanOldObjects() {
    LOGGER.log(INFO, "Starting cleaner...");
    final var bucket = System.getenv(BUCKET_NAME);
    LOGGER.log(INFO, "Cleaning bucket: {0}", bucket);
    LOGGER.log(INFO, "Cleaning objects older than {0} seconds", thresholdSeconds);
    if (folder != null) {
      LOGGER.log(INFO, "Cleaning only within folder: {0}", folder);
    }

    final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

    ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucket);
    if (folder != null) {
      requestBuilder.prefix(folder);
    }
    ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

    ListObjectsV2Response listObjectsV2Response;
    do {
      listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);

      for (S3Object s3Object : listObjectsV2Response.contents()) {
        final String key = s3Object.key();

        if (key.endsWith("/") || (folder != null && key.equals(folder))) {
          continue;
        }

        // Fetch object metadata to x-amz-meta-last-modified
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        try {
          var headResponse = s3Client.headObject(headRequest);
          String createdDate = headResponse.metadata().get("x-amz-meta-last-modified");
          if (createdDate != null) {
            try {
              Instant createdInstant = Instant.parse(createdDate);
              if (createdInstant.isBefore(threshold)) {
                deleteObject(s3Client, bucket, key);
              }
            } catch (DateTimeParseException e) {
              LOGGER.log(WARNING, "Invalid x-amz-meta-created format for {0}: {1}",
                      new Object[]{key, createdDate});
              // Fallback to lastModified
              if (s3Object.lastModified().isBefore(threshold)) {
                deleteObject(s3Client, bucket, key);
              }
            }
          } else {
            // Fallback to lastModified if x-amz-meta-created is missing
            if (s3Object.lastModified().isBefore(threshold)) {
              deleteObject(s3Client, bucket, key);
            }
          }
        } catch (Exception e) {
          LOGGER.log(WARNING, "Failed to fetch metadata for {0}: {1}",
                  new Object[]{key, e.getMessage()});
          // Fallback to lastModified
          if (s3Object.lastModified().isBefore(threshold)) {
            deleteObject(s3Client, bucket, key);
          }
        }
      }

      requestBuilder = ListObjectsV2Request.builder()
              .bucket(bucket)
              .continuationToken(listObjectsV2Response.nextContinuationToken());
      if (folder != null) {
        requestBuilder.prefix(folder);
      }
      listObjectsV2Request = requestBuilder.build();

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