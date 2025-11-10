package com.procure.thg.cockroachdb;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
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

        HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(targetBucket)
                .key(targetKey)
                .build();

        boolean shouldCopy = true;
        try {
            final HeadObjectResponse targetHead = targetClient.headObject(headRequest);
            if (!copyModified) {
                LOGGER.log(INFO, "Object {0}/{1} already exists, skipping (copyModified=false)",
                        new Object[]{targetBucket, targetKey});
                return;
            }

            // compare ETag or size as a heuristic for modified detection
            final HeadObjectResponse sourceHead = sourceClient.headObject(
                    HeadObjectRequest.builder().bucket(sourceBucket).key(sourceKey).build());

            final boolean sameETag = sourceHead.eTag() != null && targetHead.eTag() != null && sourceHead.eTag().equals(targetHead.eTag());
            final boolean sameSize = sourceHead.contentLength() == targetHead.contentLength();

            if (sameETag || sameSize) {
                LOGGER.log(INFO, "Object {0}/{1} unchanged, skipping (copyModified=true)", new Object[]{targetBucket, targetKey});
                shouldCopy = false;
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
            LOGGER.log(INFO, "Object {0}/{1} does not exist in target bucket, skipping",
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
        metadata.put("x-amz-meta-last-modified", sourceHeadResponse.lastModified().toString());

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
            LOGGER.log(INFO, "Synced metadata for object {0}/{1} using source metadata from {2}/{3}",
                    new Object[]{targetBucket, targetKey, sourceBucket, sourceKey});
        } catch (Exception e) {
            LOGGER.log(SEVERE, String.format("Failed to sync metadata for object %s/%s: %s",
                    targetBucket, targetKey, e.getMessage()), e);
            throw e;
        }
    }
}
