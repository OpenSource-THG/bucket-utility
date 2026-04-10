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

    // Threshold: 100 MB. Objects smaller than this are buffered to fix Ceph 403 issues.
    // Objects larger than this are streamed to prevent OOM.
    private static final long MEMORY_BUFFER_THRESHOLD = 100 * 1024 * 1024;

    private static final long PADDING_MINUTES = 10;

    private final S3Client sourceClient;
    private final String sourceBucket;
    private final String sourceFolder;
    private final S3Client targetClient;
    private final String targetBucket;
    private final String targetFolder;
    private final boolean copyModified;

    public S3Copier(final S3Client sourceClient, final String sourceBucket, final String sourceFolder,
                    final S3Client targetClient, final String targetBucket, final String targetFolder,
                    final boolean copyModified) {
        this.sourceClient = sourceClient;
        this.sourceBucket = sourceBucket;
        this.sourceFolder = suffixFolderName(sourceFolder);
        this.targetClient = targetClient;
        this.targetBucket = targetBucket;
        this.targetFolder = suffixFolderName(targetFolder);
        this.copyModified = copyModified;
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

    /**
     * Computes the effective last modified timestamp in the new manner:
     * max(copied-when, last-modified meta, built-in LastModified).
     * If copied-when < built-in, use built-in; else use the manual one.
     */
    private Instant getEffectiveLastModified(GetObjectResponse response) {
        Instant builtIn = response.lastModified() != null ? response.lastModified() : Instant.EPOCH;

        Map<String, String> metadata = response.metadata() != null ? response.metadata() : new HashMap<>();

        Instant copiedWhen = Instant.EPOCH;
        String cwStr = metadata.get("copied-when");
        if (cwStr != null) {
            try {
                copiedWhen = Instant.parse(cwStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid copied-when format: " + cwStr, e);
            }
        }

        Instant lastModMeta = Instant.EPOCH;
        String lmStr = metadata.get("last-modified");
        if (lmStr != null) {
            try {
                lastModMeta = Instant.parse(lmStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid last-modified format: " + lmStr, e);
            }
        }

        // Max of manual timestamps
        Instant maxManual = copiedWhen.isAfter(lastModMeta) ? copiedWhen : lastModMeta;

        // Max with built-in
        return maxManual.isAfter(builtIn) ? maxManual : builtIn;
    }

    /**
     * Overloaded for HeadObjectResponse.
     */
    private Instant getEffectiveLastModified(HeadObjectResponse head) {
        Instant builtIn = head.lastModified() != null ? head.lastModified() : Instant.EPOCH;

        Map<String, String> metadata = head.metadata() != null ? head.metadata() : new HashMap<>();

        Instant copiedWhen = Instant.EPOCH;
        String cwStr = metadata.get("copied-when");
        if (cwStr != null) {
            try {
                copiedWhen = Instant.parse(cwStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid copied-when format: " + cwStr, e);
            }
        }

        Instant lastModMeta = Instant.EPOCH;
        String lmStr = metadata.get("last-modified");
        if (lmStr != null) {
            try {
                lastModMeta = Instant.parse(lmStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid last-modified format: " + lmStr, e);
            }
        }

        // Max of manual timestamps
        Instant maxManual = copiedWhen.isAfter(lastModMeta) ? copiedWhen : lastModMeta;

        // Max with built-in
        return maxManual.isAfter(builtIn) ? maxManual : builtIn;
    }

    public void copyRecentObjects(final long thresholdSeconds) {
        LOGGER.log(INFO, "Starting to copy objects from {0}/{1} to {2}/{3}",
                new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});
        final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(sourceBucket)
                .encodingType(EncodingType.URL);
        if (!sourceFolder.isEmpty()) {
            requestBuilder.prefix(sourceFolder);
        }
        ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

        ListObjectsV2Response listObjectsV2Response;
        try {
            listObjectsV2Response = sourceClient.listObjectsV2(listObjectsV2Request);
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to list objects in %s/%s: %s", sourceBucket, sourceFolder, e.getMessage()), e);
            return;
        }

        do {
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                final String key = s3Object.key();
                if (s3Object.lastModified().isAfter(threshold)) {
                    try {
                        copyObject(key);
                    } catch (Exception e) {
                        LOGGER.log(SEVERE, String.format("Failed to copy object %s: %s", key, e.getMessage()), e);
                    }
                }
            }

            if (Boolean.TRUE.equals(listObjectsV2Response.isTruncated())) {
                requestBuilder = ListObjectsV2Request.builder()
                        .bucket(sourceBucket)
                        .encodingType(EncodingType.URL)
                        .continuationToken(listObjectsV2Response.nextContinuationToken());
                if (!sourceFolder.isEmpty()) {
                    requestBuilder.prefix(sourceFolder);
                }
                listObjectsV2Request = requestBuilder.build();

                try {
                    listObjectsV2Response = sourceClient.listObjectsV2(listObjectsV2Request);
                } catch (Exception e) {
                    LOGGER.log(SEVERE, String.format("Failed to list next page of objects in %s/%s: %s", sourceBucket, sourceFolder, e.getMessage()), e);
                    break;
                }
            }
        } while (Boolean.TRUE.equals(listObjectsV2Response.isTruncated()));
        LOGGER.log(INFO, "Finished copying objects.");
    }

    private void copyObject(final String sourceKey) throws IOException {
        if (!sourceKey.startsWith(sourceFolder)) {
            LOGGER.log(WARNING, "Object key {0} does not start with expected prefix {1}, skipping",
                    new Object[]{sourceKey, sourceFolder});
            return;
        }

        final String relativeKey = sourceKey.substring(sourceFolder.length());
        final String targetKey = targetFolder + relativeKey;

        // Fetch source metadata first to use for comparison and writing
        HeadObjectResponse sourceHead;
        try {
            sourceHead = sourceClient.headObject(
                    HeadObjectRequest.builder().bucket(sourceBucket).key(sourceKey).build());
        } catch (Exception e) {
            LOGGER.log(SEVERE, "Failed to fetch source metadata for {0}/{1}: {2}. Cannot proceed with copy.",
                    new Object[]{sourceBucket, sourceKey, e.getMessage()});
            throw new IOException("Failed to fetch source object metadata.", e);
        }


        // === 1. Check if we even need to copy ===
        boolean shouldCopy = true;
        HeadObjectRequest targetHeadRequest = HeadObjectRequest.builder()
                .bucket(targetBucket)
                .key(targetKey)
                .build();

        try {
            final HeadObjectResponse targetHead = targetClient.headObject(targetHeadRequest);

            if (!copyModified) {
                LOGGER.log(FINE, "Object {0}/{1} already exists, skipping (copyModified=false)",
                        new Object[]{targetBucket, targetKey});
                return;
            }

            final boolean sameETag = sourceHead.eTag() != null && targetHead.eTag() != null && sourceHead.eTag().equals(targetHead.eTag());
            final boolean sameSize = sourceHead.contentLength() == targetHead.contentLength();

            if (sameETag && sameSize) {
                LOGGER.log(FINE, "Object {0}/{1} unchanged (ETag/Size match), skipping", new Object[]{targetBucket, targetKey});
                shouldCopy = false;
            } else {
                // Secondary check: If ETag/Size comparison fails (e.g., due to different ETag calculation),
                // check the custom metadata timestamp, which is more reliable for cross-S3 idempotency.

                final Instant sourceLastModified = getEffectiveLastModified(sourceHead);
                // AWS SDK retrieves user metadata keys lowercased and without 'x-amz-meta-' prefix.
                // We assume the key we wrote is available as 'last-modified'.
                final Instant targetLastModified = getEffectiveLastModified(targetHead);

                if (sourceLastModified != null && targetLastModified != null) {
                    try {
                        if (sourceLastModified.equals(targetLastModified)) {
                            // If the source's last modified time matches the time we recorded on the target, skip.
                            LOGGER.log(FINE, "Object unchanged (Timestamp match via metadata) → skipping {0}/{1}", new Object[]{targetBucket, targetKey});
                            shouldCopy = false;
                        } else {
                            // Timestamp changed, proceed with copy.
                            LOGGER.log(FINE, "Source timestamp changed or metadata mismatch. Proceeding with copy.");
                        }
                    } catch (Exception parseException) {
                        LOGGER.log(WARNING, "Metadata timestamp check failed for {0}. Proceeding with copy to be safe.", targetKey);
                        shouldCopy = true;
                    }
                } else {
                    // ETag failed, and metadata is missing/incomplete. Assume change and copy.
                    LOGGER.log(FINE, "ETag mismatch detected, proceeding with copy of {0}", targetKey);
                    shouldCopy = true;
                }
            }
        } catch (NoSuchKeyException e) {
            shouldCopy = true; // doesn't exist, copy it
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error checking existence of {0}/{1}: {2}",
                    new Object[]{targetBucket, targetKey, e.getMessage()});
            shouldCopy = true; // on any error, attempt copy
        }

        if (!shouldCopy) return;

        GetObjectRequest getRequest = GetObjectRequest.builder()
                .bucket(sourceBucket)
                .key(sourceKey)
                .build();

        try (ResponseInputStream<GetObjectResponse> objectStream = sourceClient.getObject(getRequest)) {
            final Long contentLength = objectStream.response().contentLength();
            final GetObjectResponse getResponse = objectStream.response();

            // Prepare metadata (we only include necessary fields to avoid Ceph rejection)
            Map<String, String> metadata = new HashMap<>();

            // CRITICAL: Copy only the user metadata from source
            if (getResponse.metadata() != null) {
                // Note: copy the map but ensure all keys are lowercase for standard S3 behavior
                getResponse.metadata().forEach((key, value) -> {
                    // We only want user-defined metadata (starts with x-amz-meta-)
                    // The AWS SDK usually strips this prefix upon retrieval, but we re-add it if needed.
                    if (key.toLowerCase().startsWith("x-amz-meta-")) {
                        metadata.put(key, value);
                    }
                });
            }

            // CRITICAL: Add the source's effective LastModified timestamp as custom metadata for idempotency check
            Instant effectiveLastModified = getEffectiveLastModified(getResponse);
            if (effectiveLastModified != Instant.EPOCH) {
                metadata.put("last-modified", effectiveLastModified.toString());
            }

            PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(targetBucket)
                    .key(targetKey);
            if (!metadata.isEmpty()) {
                builder.metadata(metadata);
            }

            String contentType = getResponse.contentType();
            if (contentType != null) {
                builder.contentType(contentType);
            }
            PutObjectRequest putRequest = builder.build();

            // HYBRID STRATEGY (Buffer small files for robustness, stream large files to prevent OOM)
            if (contentLength != null && contentLength >= 0 && contentLength < MEMORY_BUFFER_THRESHOLD) {
                byte[] objectContent = objectStream.readAllBytes();
                targetClient.putObject(putRequest, RequestBody.fromBytes(objectContent));

                LOGGER.log(FINE, "Copied object (buffered) from {0}/{1} to {2}/{3} [Size: {4}]",
                        new Object[]{sourceBucket, sourceKey, targetBucket, targetKey, contentLength});
            } else {
                // If file is large (or length unknown), stream it to avoid OOM.
                LOGGER.log(INFO, "Streaming large object (>100MB) from {0}/{1} to {2}/{3} [Size: {4}]",
                        new Object[]{sourceBucket, sourceKey, targetBucket, targetKey, contentLength});

                if (contentLength != null) {
                    targetClient.putObject(putRequest, RequestBody.fromInputStream(objectStream, contentLength));
                } else {
                    // Fallback if length is missing (rare in S3)
                    LOGGER.log(WARNING, "Warning: object length is missing, buffering entire content as a fallback.");

                    byte[] content = objectStream.readAllBytes();
                    targetClient.putObject(putRequest, RequestBody.fromBytes(content));
                }
            }
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to copy object from %s/%s to %s/%s: %s",
                    sourceBucket, sourceKey, targetBucket, targetKey, e.getMessage()), e);
            throw e;
        }
    }

    public void syncMetaDataRecentObjects(final long thresholdSeconds) {
        LOGGER.log(INFO, "Starting to sync meta data objects from {0}/{1} to {2}/{3}",
                new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});
        final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(sourceBucket)
                .encodingType(EncodingType.URL);
        if (!sourceFolder.isEmpty()) {
            requestBuilder.prefix(sourceFolder);
        }
        ListObjectsV2Request listObjectsV2Request = requestBuilder.build();

        ListObjectsV2Response listObjectsV2Response;
        try {
            listObjectsV2Response = sourceClient.listObjectsV2(listObjectsV2Request);
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to list objects in %s/%s: %s", sourceBucket, sourceFolder, e.getMessage()), e);
            return;
        }

        do {
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                final String key = s3Object.key();
                if (s3Object.lastModified().isAfter(threshold)) {
                    try {
                        syncObjectMetadata(key);
                    } catch (Exception e) {
                        LOGGER.log(SEVERE, String.format("Failed to copy object %s: %s", key, e.getMessage()), e);
                    }
                }
            }

            if (Boolean.TRUE.equals(listObjectsV2Response.isTruncated())) {
                requestBuilder = ListObjectsV2Request.builder()
                        .bucket(sourceBucket)
                        .encodingType(EncodingType.URL)
                        .continuationToken(listObjectsV2Response.nextContinuationToken());
                if (!sourceFolder.isEmpty()) {
                    requestBuilder.prefix(sourceFolder);
                }
                listObjectsV2Request = requestBuilder.build();

                try {
                    listObjectsV2Response = sourceClient.listObjectsV2(listObjectsV2Request);
                } catch (Exception e) {
                    LOGGER.log(SEVERE, String.format("Failed to list next page of objects in %s/%s: %s", sourceBucket, sourceFolder, e.getMessage()), e);
                    break;
                }
            }
        } while (Boolean.TRUE.equals(listObjectsV2Response.isTruncated()));
        LOGGER.log(INFO, "Finished copying objects.");
    }

    public void syncObjectMetadata(final String sourceKey) {
        if (!sourceKey.startsWith(sourceFolder)) {
            LOGGER.log(WARNING, "Object key {0} does not start with expected prefix {1}, skipping",
                    new Object[]{sourceKey, sourceFolder});
            return;
        }

        String relativeKey = sourceKey.substring(sourceFolder.length());
        String targetKey = targetFolder + relativeKey;

        // Check if the target object exists in Ceph
        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(targetBucket)
                .key(targetKey)
                .build();
        try {
            targetClient.headObject(headRequest);
        } catch (NoSuchKeyException e) {
            LOGGER.log(FINE, "Object {0}/{1} does not exist in target bucket, skipping",
                    new Object[]{targetBucket, targetKey});
            return;
        } catch (Exception e) {
            LOGGER.log(WARNING, "Error checking existence of {0}/{1}: {2}",
                    new Object[]{targetBucket, targetKey, e.getMessage()});
            return;
        }

        // Fetch source object metadata from S3
        HeadObjectRequest sourceHeadRequest = HeadObjectRequest.builder()
                .bucket(sourceBucket)
                .key(sourceKey)
                .build();
        HeadObjectResponse sourceHeadResponse;
        try {
            sourceHeadResponse = sourceClient.headObject(sourceHeadRequest);
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to fetch metadata for source object %s/%s: %s",
                    sourceBucket, sourceKey, e.getMessage()), e);
            return;
        }

        // Prepare metadata (excluding lastModified)
        Map<String, String> metadata = new HashMap<>(sourceHeadResponse.metadata());
        Instant effectiveLastModified = getEffectiveLastModified(sourceHeadResponse);
        if (effectiveLastModified != Instant.EPOCH) {
            metadata.put("last-modified", effectiveLastModified.toString());
        }

        // Use CopyObject to update metadata in-place on Ceph
        CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                .sourceBucket(targetBucket) // Copy from Ceph to itself
                .sourceKey(targetKey)
                .destinationBucket(targetBucket)
                .destinationKey(targetKey)
                .metadataDirective(MetadataDirective.REPLACE)
                .metadata(metadata)
                .build();

        try {
            targetClient.copyObject(copyRequest);
            LOGGER.log(FINE, "Synced metadata for object {0}/{1} using source metadata from {2}/{3}",
                    new Object[]{targetBucket, targetKey, sourceBucket, sourceKey});
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to sync metadata for object %s/%s: %s",
                    targetBucket, targetKey, e.getMessage()), e);
            throw e;
        }
    }
}