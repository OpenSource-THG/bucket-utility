package com.procure.thg.cockroachdb;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import static java.util.logging.Level.*;

public class S3Copier {

    private static final Logger LOGGER = Logger.getLogger(S3Copier.class.getName());
    private final S3Client sourceClient;
    private final String sourceBucket;
    private final String sourceFolder;
    private final S3Client targetClient;
    private final String targetBucket;
    private final String targetFolder;

    public S3Copier(final S3Client sourceClient, final String sourceBucket, final String sourceFolder,
                    final S3Client targetClient, final String targetBucket, final String targetFolder) {
        this.sourceClient = sourceClient;
        this.sourceBucket = sourceBucket;
        this.sourceFolder = suffixFolderName(sourceFolder);
        this.targetClient = targetClient;
        this.targetBucket = targetBucket;
        this.targetFolder = suffixFolderName(targetFolder);
    }

    private String suffixFolderName(final String folder) {
        if (folder != null && !folder.isEmpty()) {
            if (folder.endsWith("/")) {
                return folder;
            }
            return folder + "/";
        } else {
            return "";
        }
    }

    public void copyRecentObjects(final long thresholdSeconds) {
        LOGGER.log(INFO, "Starting to copy objects from {0}/{1} to {2}/{3}",
                new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});
        final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder().bucket(sourceBucket);
        if (!sourceFolder.isEmpty()) {
            requestBuilder.prefix(sourceFolder);
        }
        ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

        ListObjectsV2Response listObjectsV2Response = null;
        boolean isFirstPage = true;

        do {
            try {
                listObjectsV2Response = sourceClient.listObjectsV2(listObjectsV2Request);
                LOGGER.log(FINE, "Successfully listed page of objects from {0}/{1}",
                        new Object[]{sourceBucket, sourceFolder});
            } catch (Exception e) {
                LOGGER.log(SEVERE, "Failed to list objects in {0}/{1} (continuationToken={2}): {3}"
                );
                if (isFirstPage) {
                    LOGGER.log(SEVERE, "Aborting: Failed to list the first page of objects.");
                    return; // Exit if the first page fails, as we have no data to process
                }
                // Skip this page and try the next one if possible
                if (listObjectsV2Response != null && Boolean.TRUE.equals(listObjectsV2Response.isTruncated())) {
                    LOGGER.log(WARNING, "Skipping malformed page, attempting to continue with next page.");
                } else {
                    LOGGER.log(INFO, "No more pages to process after error. Finishing with what was copied.");
                    break;
                }
            }

            // Process objects if the response is valid
            if (listObjectsV2Response != null && listObjectsV2Response.contents() != null) {
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    final String key = s3Object.key();
                    if (s3Object.lastModified().isAfter(threshold)) {
                        try {
                            copyObject(key);
                        } catch (Exception e) {
                            LOGGER.log(SEVERE, "Failed to copy object {0}: {1}"
                            );
                            // Continue with the next object even if this one fails
                        }
                    }
                }
            }

            if (listObjectsV2Response != null && Boolean.TRUE.equals(listObjectsV2Response.isTruncated())) {
                requestBuilder = ListObjectsV2Request.builder()
                        .bucket(sourceBucket)
                        .continuationToken(listObjectsV2Response.nextContinuationToken());
                if (!sourceFolder.isEmpty()) {
                    requestBuilder.prefix(sourceFolder);
                }
                listObjectsV2Request = requestBuilder.build();
            } else {
                break; // No more pages to process
            }

            isFirstPage = false;
        } while (true); // Loop until explicitly broken

        LOGGER.log(INFO, "Finished copying objects. Some pages may have been skipped due to errors.");
    }

    private void copyObject(final String sourceKey) throws IOException {
        if (!sourceKey.startsWith(sourceFolder)) {
            LOGGER.log(WARNING, "Object key {0} does not start with expected prefix {1}, skipping",
                    new Object[]{sourceKey, sourceFolder});
            return;
        }

        String relativeKey = sourceKey.substring(sourceFolder.length());
        String targetKey = targetFolder + relativeKey;

        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(sourceBucket)
                .key(sourceKey)
                .build();
        try (ResponseInputStream<GetObjectResponse> objectStream = sourceClient.getObject(getRequest)) {
            Long contentLength = objectStream.response().contentLength();
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(targetKey)
                    .build();

            if (contentLength != null && contentLength >= 0) {
                targetClient.putObject(putRequest, RequestBody.fromInputStream(objectStream, contentLength));
            } else {
                byte[] content = objectStream.readAllBytes();
                targetClient.putObject(putRequest, RequestBody.fromBytes(content));
            }

            LOGGER.log(INFO, "Copied object from {0}/{1} to {2}/{3}",
                    new Object[]{sourceBucket, sourceKey, targetBucket, targetKey});
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to copy object from %s/%s to %s/%s: %s",
                    sourceBucket, sourceKey, targetBucket, targetKey, e.getMessage()), e);
            throw e;
        }
    }
}