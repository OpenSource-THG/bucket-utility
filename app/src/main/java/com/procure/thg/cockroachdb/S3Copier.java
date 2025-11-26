package com.procure.thg.cockroachdb;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

public class S3Copier {

    private static final Logger LOGGER = Logger.getLogger(S3Copier.class.getName());

    // Buffer small files in memory (under 500 MB), stream larger ones
    private static final long MEMORY_BUFFER_THRESHOLD = 500 * 1024 * 1024;

    private final S3Client sourceClient;
    private final String sourceBucket;
    private final String sourceFolder;
    private final S3Client targetClient;
    private final String targetBucket;
    private final String targetFolder;
    private final boolean copyModified;

    public S3Copier(S3Client sourceClient, String sourceBucket, String sourceFolder,
                    S3Client targetClient, String targetBucket, String targetFolder,
                    boolean copyModified) {
        this.sourceClient = sourceClient;
        this.sourceBucket = sourceBucket;
        this.sourceFolder = suffixFolderName(sourceFolder);
        this.targetClient = targetClient;
        this.targetBucket = targetBucket;
        this.targetFolder = suffixFolderName(targetFolder);
        this.copyModified = copyModified;
    }

    private String suffixFolderName(String folder) {
        if (folder != null && !folder.isEmpty()) {
            return folder.endsWith("/") ? folder : folder + "/";
        }
        return "";
    }

    public void copyRecentObjects(long thresholdSeconds) {
        LOGGER.log(INFO, "Starting copy process - version 11 FINAL (Ceph-ready)");
        LOGGER.log(INFO, "Copying recent objects from {0}/{1} → {2}/{3}",
                new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});

        Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

        ListObjectsV2Request request = ListObjectsV2Request.builder()
                .bucket(sourceBucket)
                .prefix(sourceFolder.isEmpty() ? null : sourceFolder)
                .encodingType(EncodingType.URL)
                .build();

        String continuationToken = null;
        do {
            if (continuationToken != null) {
                request = request.toBuilder().continuationToken(continuationToken).build();
            }

            ListObjectsV2Response response = sourceClient.listObjectsV2(request);

            for (S3Object s3Object : response.contents()) {
                if (s3Object.lastModified().isAfter(threshold)) {
                    try {
                        copyObject(s3Object.key());
                    } catch (Exception e) {
                        LOGGER.log(SEVERE, "Failed to copy " + s3Object.key() + ": " + e.getMessage(), e);
                    }
                }
            }

            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);

        LOGGER.log(INFO, "Finished copying recent objects.");
    }

    private void copyObject(String sourceKey) throws IOException {
        if (!sourceKey.startsWith(sourceFolder)) {
            LOGGER.log(WARNING, "Skipping key outside prefix: {0}", sourceKey);
            return;
        }

        String relativeKey = sourceKey.substring(sourceFolder.length());
        String targetKey = targetFolder + relativeKey;

        // === 1. Check if we even need to copy ===
        boolean shouldCopy = true;
        try {
            HeadObjectResponse targetHead = targetClient.headObject(HeadObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(targetKey)
                    .build());

            if (!copyModified) {
                LOGGER.log(INFO, "Object exists and copyModified=false → skipping {0}/{1}", new Object[]{targetBucket, targetKey});
                return;
            }

            HeadObjectResponse sourceHead = sourceClient.headObject(HeadObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build());

            // Primary check: ETag and size match. This is fast but often unreliable across different S3 implementations (like Ceph).
            boolean sameETag = sourceHead.eTag() != null && targetHead.eTag() != null && sourceHead.eTag().equals(targetHead.eTag());
            boolean sameSize = sourceHead.contentLength() == targetHead.contentLength();

            if (sameETag && sameSize) {
                LOGGER.log(INFO, "Object unchanged (ETag/Size match) → skipping {0}/{1}", new Object[]{targetBucket, targetKey});
                shouldCopy = false;
            } else {
                // Secondary check: If ETag/Size comparison fails (e.g., due to different ETag calculation),
                // check the custom metadata timestamp, which is more reliable for idempotency.
                Instant sourceLastModified = sourceHead.lastModified();
                // AWS SDK usually returns user metadata keys lowercased and without 'x-amz-meta-' prefix.
                String targetSourceLastModifiedMeta = targetHead.metadata().get("last-modified");

                if (sourceLastModified != null && targetSourceLastModifiedMeta != null) {
                    try {
                        Instant targetMetaTime = Instant.parse(targetSourceLastModifiedMeta);

                        if (sourceLastModified.equals(targetMetaTime)) {
                            // If the source's last modified time matches the time we recorded on the target, skip.
                            LOGGER.log(INFO, "Object unchanged (Timestamp match) → skipping {0}/{1}", new Object[]{targetBucket, targetKey});
                            shouldCopy = false;
                        } else {
                            // ETag failed AND timestamp changed. This is a real update.
                            LOGGER.log(INFO, "ETag/Size mismatch detected, and source timestamp has changed ({0} != {1}). Proceeding with copy.",
                                    new Object[]{sourceLastModified, targetMetaTime});
                            shouldCopy = true;
                        }
                    } catch (Exception e) {
                        // If Instant parsing fails, log a warning and proceed with copy to be safe.
                        LOGGER.log(WARNING, "Metadata timestamp check failed for {0}. Proceeding with copy to be safe.", targetKey);
                        shouldCopy = true;
                    }
                } else {
                    // ETag failed, and metadata is missing/incomplete (maybe object predates this feature). Assume change and copy.
                    LOGGER.log(INFO, "ETag mismatch detected, metadata missing or incomplete. Proceeding with copy of {0}", targetKey);
                    shouldCopy = true;
                }
            }
        } catch (NoSuchKeyException ignored) {
            // Target doesn't exist → must copy
        }

        if (!shouldCopy) return;

        // === 2. Delete existing object first (the only 100% reliable Ceph overwrite fix) ===
        try {
            targetClient.headObject(HeadObjectRequest.builder().bucket(targetBucket).key(targetKey).build());
            LOGGER.log(INFO, "Deleting existing object before upload (Ceph workaround): {0}/{1}", new Object[]{targetBucket, targetKey});
            LOGGER.log(INFO, "skiiip");

            /*targetClient.deleteObject(DeleteObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(targetKey)
                    .build());*/
        } catch (NoSuchKeyException ignored) {
            // Target doesn't exist
            LOGGER.log(INFO, "Target object does not exist. Proceeding with upload.");

        } catch (Exception e) {
            // Log the error but continue to attempt the PUT, as this might be the reason for the initial 403 on PUT.
            LOGGER.log(WARNING, "Failed to delete existing object before upload, continuing to PUT: " + e.getMessage(), e);
        }

        // === 3. Download from source and Upload to target ===
        try (ResponseInputStream<GetObjectResponse> stream = sourceClient.getObject(
                GetObjectRequest.builder().bucket(sourceBucket).key(sourceKey).build())) {

            GetObjectResponse getResponse = stream.response();
            Long contentLength = getResponse.contentLength();

            // CRITICAL CEPH FIX: We are intentionally NOT copying full metadata from getResponse here
            // to avoid sending headers Ceph might reject (like x-amz-tagging or other internal AWS headers).

            // Manually extract and include the source's LastModified timestamp as user metadata
            Map<String, String> requiredMetadata = new HashMap<>();
            if (getResponse.lastModified() != null) {
                // Using a custom meta key to store the source's last modified time.
                // NOTE: This must match the key used for comparison in Section 1 (lowercase, no prefix).
                requiredMetadata.put("last-modified", getResponse.lastModified().toString());
            }

            // Build the PutObjectRequest with minimal, standard headers
            PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(targetKey)
                    .contentLength(contentLength); // Critical for non-chunked

            // Apply specific, safe metadata
            if (!requiredMetadata.isEmpty()) {
                putRequestBuilder.metadata(requiredMetadata);
            }

            // Use the standard Content-Type, or a safe default
            String contentType = getResponse.contentType();
            if (contentType == null || contentType.isEmpty()) {
                contentType = "application/octet-stream";
            }
            putRequestBuilder.contentType(contentType);

            PutObjectRequest putRequest = putRequestBuilder.build();

            // === 4. Upload (buffered or streamed) ===
            if (contentLength != null && contentLength < MEMORY_BUFFER_THRESHOLD) {
                // Buffered
                byte[] bytes = stream.readAllBytes();
                targetClient.putObject(putRequest, RequestBody.fromBytes(bytes));
                LOGGER.log(INFO, "Uploaded (buffered) {0} ({1} bytes)", new Object[]{targetKey, bytes.length});
            } else if (contentLength != null) {
                // Streamed (Large file)
                targetClient.putObject(putRequest, RequestBody.fromInputStream(stream, contentLength));
                LOGGER.log(INFO, "Uploaded (streamed) {0} ({1} bytes)", new Object[]{targetKey, contentLength});
            } else {
                // Fallback: If length is unknown, throw or buffer. We should buffer if source is S3.
                LOGGER.log(WARNING, "Object length is missing for large file. Aborting copy for {0}", sourceKey);
                throw new IOException("Cannot copy stream with unknown length.");
            }
        }
    }

    // Optional: keep your metadata sync if needed — but CopyObject often fails on Ceph too.
    // Most people just re-upload the whole object (which we now do above).
}