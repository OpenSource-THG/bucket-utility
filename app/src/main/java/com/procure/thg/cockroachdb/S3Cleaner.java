package com.procure.thg.cockroachdb;

import static java.util.logging.Level.FINE;
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
    } else {
      LOGGER.log(INFO, "No folder prefix specified, cleaning entire bucket");
    }

    final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);
    LOGGER.log(INFO, "Threshold timestamp: {0}", threshold);

    ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucket);
    if (folder != null) {
      requestBuilder.prefix(folder);
    }
    ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

    ListObjectsV2Response listObjectsV2Response;
    int pageCount = 0;
    do {
      pageCount++;
      try {
        listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
        int objectCount = listObjectsV2Response.contents().size();
        LOGGER.log(INFO, "Page {0}: Listed {1} objects", new Object[]{pageCount, objectCount});
        if (objectCount == 0) {
          LOGGER.log(INFO, "No objects found in page {0}", pageCount);
        }

        for (S3Object s3Object : listObjectsV2Response.contents()) {
          final String key = s3Object.key();
          LOGGER.log(FINE, "Processing key: {0}", key);

          if (key.endsWith("/") || (folder != null && key.equals(folder))) {
            LOGGER.log(FINE, "Skipping key {0}: directory or folder prefix", key);
            continue;
          }

          // Fetch object metadata to last-modified
          HeadObjectRequest headRequest = HeadObjectRequest.builder()
                  .bucket(bucket)
                  .key(key)
                  .build();
          try {
            var headResponse = s3Client.headObject(headRequest);
            LOGGER.log(FINE, "Metadata for {0}: {1}", new Object[]{key, headResponse.metadata()});
            String createdDate = headResponse.metadata().get("last-modified");
            if (createdDate != null) {
              try {
                Instant createdInstant = Instant.parse(createdDate);
                LOGGER.log(FINE, "last-modified for {0}: {1}", new Object[]{key, createdDate});
                if (createdInstant.isBefore(threshold)) {
                  deleteObject(s3Client, bucket, key);
                } else {
                  LOGGER.log(FINE, "Skipping {0}: last-modified {1} is after threshold {2}",
                          new Object[]{key, createdInstant, threshold});
                }
              } catch (DateTimeParseException e) {
                LOGGER.log(WARNING, "Invalid last-modified format for {0}: {1}",
                        new Object[]{key, createdDate});
                // Fallback to lastModified
                Instant lastModified = s3Object.lastModified();
                LOGGER.log(FINE, "Falling back to LastModified for {0}: {1}", new Object[]{key, lastModified});
                if (lastModified.isBefore(threshold)) {
                  deleteObject(s3Client, bucket, key);
                } else {
                  LOGGER.log(FINE, "Skipping {0}: LastModified {1} is after threshold {2}",
                          new Object[]{key, lastModified, threshold});
                }
              }
            } else {
              // Fallback to lastModified if last-modified is missing
              LOGGER.log(FINE, "No last-modified for {0}, using LastModified", key);
              Instant lastModified = s3Object.lastModified();
              LOGGER.log(FINE, "LastModified for {0}: {1}", new Object[]{key, lastModified});
              if (lastModified.isBefore(threshold)) {
                deleteObject(s3Client, bucket, key);
              } else {
                LOGGER.log(FINE, "Skipping {0}: LastModified {1} is after threshold {2}",
                        new Object[]{key, lastModified, threshold});
              }
            }
          } catch (Exception e) {
            LOGGER.log(WARNING, "Failed to fetch metadata for {0}: {1}",
                    new Object[]{key, e.getMessage()});
            // Fallback to lastModified
            Instant lastModified = s3Object.lastModified();
            LOGGER.log(FINE, "Falling back to LastModified for {0}: {1}", new Object[]{key, lastModified});
            if (lastModified.isBefore(threshold)) {
              deleteObject(s3Client, bucket, key);
            } else {
              LOGGER.log(FINE, "Skipping {0}: LastModified {1} is after threshold {2}",
                      new Object[]{key, lastModified, threshold});
            }
          }
        }

        if (listObjectsV2Response.isTruncated()) {
          LOGGER.log(INFO, "Page {0} truncated, using continuation token: {1}",
                  new Object[]{pageCount, listObjectsV2Response.nextContinuationToken()});
        }
      } catch (Exception e) {
        LOGGER.log(WARNING, "Error listing objects in page {0}: {1}",
                new Object[]{pageCount, e.getMessage()});
        break; // Stop processing to avoid infinite loop on persistent errors
      }

      requestBuilder = ListObjectsV2Request.builder()
              .bucket(bucket)
              .continuationToken(listObjectsV2Response.nextContinuationToken());
      if (folder != null) {
        requestBuilder.prefix(folder);
      }
      listObjectsV2Request = requestBuilder.build();

    } while (listObjectsV2Response != null && Boolean.TRUE.equals(listObjectsV2Response.isTruncated()));
    LOGGER.log(INFO, "Cleaning finished. Processed {0} pages.", pageCount);
  }

  private void deleteObject(final S3Client s3Client, final String bucket, final String key) {
    try {
      DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build();
      s3Client.deleteObject(deleteObjectRequest);
      LOGGER.log(FINE, "Deleted object: {0}", key);
    } catch (Exception e) {
      LOGGER.log(WARNING, "Failed to delete object {0}: {1}", new Object[]{key, e.getMessage()});
    }
  }
}
