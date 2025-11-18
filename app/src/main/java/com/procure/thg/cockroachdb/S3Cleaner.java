package com.procure.thg.cockroachdb;

import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Deletes S3 objects older than a specified threshold.
 *
 * Objects are evaluated based on their lastModified timestamp.
 * Deletion threshold = current time - thresholdSeconds.
 *
 * Objects with lastModified < threshold are deleted.
 *
 * @see #cleanOldObjects() for execution details
 */
public class S3Cleaner {

  private static final Logger LOGGER = Logger.getLogger(S3Cleaner.class.getName());
  private static final String BUCKET_NAME = "BUCKET_NAME";
  private final S3Client s3Client;
  private final long thresholdSeconds;
  private final String folder;
  private final boolean dryRun;

  /**
   * Creates a cleaner for S3 objects.
   *
   * @param s3Client AWS S3 client (should be closed by caller)
   * @param thresholdSeconds Age threshold in seconds (objects older than this are deleted)
   * @param folder Optional folder prefix to scope deletions (auto-adds trailing slash if provided)
   */
  public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folder) {
    this(s3Client, thresholdSeconds, folder, false);
  }

  /**
   * Creates a cleaner for S3 objects with optional dry-run mode.
   *
   * @param s3Client AWS S3 client (should be closed by caller)
   * @param thresholdSeconds Age threshold in seconds (must be >= 0)
   * @param folder Optional folder prefix to scope deletions (auto-adds trailing slash if provided)
   * @param dryRun If true, logs what would be deleted instead of actually deleting
   */
  public S3Cleaner(final S3Client s3Client, final long thresholdSeconds,
                   final String folder, final boolean dryRun) {
    if (thresholdSeconds < 0) {
      throw new IllegalArgumentException("thresholdSeconds must be >= 0");
    }

    if (folder != null) {
      if (folder.isEmpty()) {
        throw new IllegalArgumentException("folder must not be empty");
      }
      if (folder.length() > 1024) {
        throw new IllegalArgumentException("folder exceeds S3 key length limit (1024 bytes)");
      }
      if (folder.contains("\0")) {
        throw new IllegalArgumentException("folder contains null bytes");
      }
      // Normalize: ensure trailing slash
      this.folder = folder.endsWith("/") ? folder : folder + "/";
    } else {
      this.folder = null;
    }

    this.s3Client = s3Client;
    this.thresholdSeconds = thresholdSeconds;
    this.dryRun = dryRun;
  }

  /**
   * Executes the deletion operation.
   *
   * Processes all objects in the bucket (or folder), comparing lastModified
   * against the calculated threshold, and deleting those that are older.
   *
   * Handles pagination automatically. Logs detailed progress and errors.
   * Calculates threshold as: threshold = now() - thresholdSeconds
   * Objects with lastModified < threshold will be deleted.
   */
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
    if (dryRun) {
      LOGGER.log(INFO, "[DRY RUN MODE] No objects will be deleted");
    }

    // threshold = now() - thresholdSeconds
    // Objects with lastModified < threshold will be deleted
    final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);
    LOGGER.log(INFO, "Threshold timestamp: {0}", threshold);

    ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucket);
    if (folder != null) {
      requestBuilder.prefix(folder);
    }
    ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

    // Initialize to null to detect first iteration failures
    ListObjectsV2Response listObjectsV2Response = null;
    int pageCount = 0;
    int totalObjectsProcessed = 0;
    int totalObjectsDeleted = 0;
    int totalObjectsSkipped = 0;
    int totalErrorsEncountered = 0;

    do {
      pageCount++;
      try {
        listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
        int objectCount = listObjectsV2Response.contents().size();
        LOGGER.log(FINE, "Page {0}: Listed {1} objects", new Object[]{pageCount, objectCount});
        if (objectCount == 0) {
          LOGGER.log(INFO, "No objects found in page {0}", pageCount);
        }

        for (S3Object s3Object : listObjectsV2Response.contents()) {
          totalObjectsProcessed++;
          processObject(s3Object, bucket, threshold, totalObjectsDeleted, totalObjectsSkipped, totalErrorsEncountered);
        }

        if (listObjectsV2Response.isTruncated()) {
          LOGGER.log(FINE, "Page {0} truncated, using continuation token: {1}",
                  new Object[]{pageCount, listObjectsV2Response.nextContinuationToken()});
        } else {
          // Not truncated, we're done
          break;
        }
      } catch (Exception e) {
        LOGGER.log(WARNING, "Error listing objects in page {0}: {1}",
                new Object[]{pageCount, e.getMessage()});
        totalErrorsEncountered++;
        break; // Stop processing to avoid infinite loop on persistent errors
      }

      // Guard continuation token usage - only rebuild request if response is valid
      if (listObjectsV2Response != null && listObjectsV2Response.isTruncated()) {
        String token = listObjectsV2Response.nextContinuationToken();
        if (token != null) {
          requestBuilder = ListObjectsV2Request.builder()
                  .bucket(bucket)
                  .continuationToken(token);
          if (folder != null) {
            requestBuilder.prefix(folder);
          }
          listObjectsV2Request = requestBuilder.build();
        } else {
          // No token available even though truncated - should not happen
          LOGGER.log(WARNING, "Page {0}: Truncated but no continuation token available", pageCount);
          break;
        }
      } else {
        // Not truncated or response is null - exit loop
        break;
      }

    } while (true);

    // Log comprehensive final result
    if (dryRun) {
      LOGGER.log(INFO, "[DRY RUN] Would have deleted {0} objects. Cleaned finished. Pages: {1}, Objects Processed: {2}, Deleted: {3}, Skipped: {4}, Errors: {5}",
              new Object[]{totalObjectsDeleted, pageCount, totalObjectsProcessed, totalObjectsDeleted, totalObjectsSkipped, totalErrorsEncountered});
    } else {
      LOGGER.log(INFO, "Cleaning finished. Pages: {0}, Objects Processed: {1}, Deleted: {2}, Skipped: {3}, Errors: {4}",
              new Object[]{pageCount, totalObjectsProcessed, totalObjectsDeleted, totalObjectsSkipped, totalErrorsEncountered});
    }
  }

  /**
   * Processes a single S3 object and deletes it if older than threshold.
   *
   * @param s3Object the S3 object to process
   * @param bucket the bucket name
   * @param threshold the deletion threshold (objects older than this are deleted)
   * @param totalObjectsDeleted counter for deleted objects (incremented in place)
   * @param totalObjectsSkipped counter for skipped objects (incremented in place)
   * @param totalErrorsEncountered counter for errors (incremented in place)
   */
  private void processObject(final S3Object s3Object, final String bucket, final Instant threshold,
                             final int totalObjectsDeleted, final int totalObjectsSkipped,
                             final int totalErrorsEncountered) {
    final String key = s3Object.key();
    LOGGER.log(FINE, "Processing key: {0}", key);

    // Skip directory markers (keys ending with /)
    if (key.endsWith("/")) {
      LOGGER.log(FINE, "Skipping key {0}: directory marker", key);
      return;
    }

    // Use lastModified directly from ListObjects response - no need for separate HeadObject call
    Instant lastModified = s3Object.lastModified();
    LOGGER.log(FINE, "Processing key: {0}, lastModified: {1}", new Object[]{key, lastModified});

    if (lastModified.isBefore(threshold)) {
      deleteObject(s3Client, bucket, key);
    } else {
      LOGGER.log(FINE, "Skipping {0}: lastModified {1} is after threshold {2}",
              new Object[]{key, lastModified, threshold});
    }
  }

  /**
   * Deletes an S3 object or logs the deletion if in dry-run mode.
   *
   * @param s3Client the S3 client to use for deletion
   * @param bucket the bucket name
   * @param key the object key
   */
  private void deleteObject(final S3Client s3Client, final String bucket, final String key) {
    try {
      if (dryRun) {
        LOGGER.log(INFO, "[DRY RUN] Would delete object: {0}", key);
      } else {
        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3Client.deleteObject(deleteObjectRequest);
        LOGGER.log(FINE, "Deleted object: {0}", key);
      }
    } catch (Exception e) {
      LOGGER.log(WARNING, "Failed to delete object {0}: {1}", new Object[]{key, e.getMessage()});
    }
  }
}
