package com.procure.thg.cockroachdb;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

public class S3Comparator {

    private static final Logger LOGGER = Logger.getLogger(S3Comparator.class.getName());
    private static final long PADDING_MINUTES = 10;

    private final S3Client sourceClient;
    private final String sourceBucket;
    private final String sourceFolder;
    private final S3Client targetClient;
    private final String targetBucket;
    private final String targetFolder;

    public S3Comparator(final S3Client sourceClient, final String sourceBucket, final String sourceFolder,
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

    // Enum to categorize the result of a comparison operation
    private enum ComparisonAction {
        MATCH,
        DIFF_TIMESTAMP,
        DIFF_MISSING_TARGET,
        DIFF_MISSING_SOURCE,
        ERROR
    }

    // Class to aggregate the results and generate a summary
    private class ComparisonSummary {
        long totalSourceObjects = 0; // Total files scanned in Pass 1
        long totalTargetObjects = 0; // Total files scanned in Pass 2

        long diffTimestamp = 0;
        long diffMissingTarget = 0;
        long diffMissingSource = 0; // Orphans
        long errors = 0;

        void record(ComparisonAction action) {
            switch (action) {
                case MATCH:
                    break;
                case DIFF_TIMESTAMP:
                    diffTimestamp++;
                    break;
                case DIFF_MISSING_TARGET:
                    diffMissingTarget++;
                    break;
                case DIFF_MISSING_SOURCE:
                    diffMissingSource++;
                    break;
                case ERROR:
                    errors++;
                    break;
            }
        }

        void incrementTotalSource() {
            totalSourceObjects++;
        }

        void incrementTotalTarget() {
            totalTargetObjects++;
        }

        void printSummary() {
            LOGGER.log(INFO, "--- Comparison Job Summary ({0}/{1} vs {2}/{3}) ---",
                    new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});

            long diffFiles = diffTimestamp + diffMissingTarget;
            double totalSource = (double) totalSourceObjects;
            String diffPct = totalSource > 0 ? String.format("%.2f%%", (diffFiles / totalSource) * 100) : "N/A";

            LOGGER.log(INFO, "Source Objects Scanned (Pass 1): {0}", totalSourceObjects);
            LOGGER.log(INFO, "Target Objects Scanned (Pass 2): {0}", totalTargetObjects);
            LOGGER.log(INFO, "--------------------------------------------------------");

            LOGGER.log(WARNING, "Sync Difference Summary (Based on Source Objects):");
            LOGGER.log(WARNING, "  Total Diff (Need Copy/Update): {0} ({1} of Source)",
                    new Object[]{diffFiles, diffPct});
            LOGGER.log(WARNING, "    - Timestamp Mismatch: {0}", diffTimestamp);
            LOGGER.log(WARNING, "    - Missing in Target: {0}", diffMissingTarget);

            String orphanPct = totalTargetObjects > 0 ?
                    String.format("%.2f%%", (diffMissingSource / (double)totalTargetObjects) * 100) : "N/A";

            LOGGER.log(WARNING, "Target Orphan Summary (Missing in Source):");
            LOGGER.log(WARNING, "  Orphan Files Found: {0} ({1} of Target)",
                    new Object[]{diffMissingSource, orphanPct});

            LOGGER.log(SEVERE, "Errors Encountered: {0}", errors);
            LOGGER.log(INFO, "--------------------------------------------------------");
        }
    }

    public void compareBuckets() {
        final ComparisonSummary summary = new ComparisonSummary();

        LOGGER.log(INFO, "Starting comparison between Source: {0}/{1} and Target: {2}/{3}",
                new Object[]{sourceBucket, sourceFolder, targetBucket, targetFolder});
        LOGGER.log(INFO, "Logic: Effective Timestamp = max(copied-when, last-modified meta, built-in) + 10min padding (on target).");

        // 1. Iterate Source to find missing in Target OR timestamp mismatches
        compareSourceToTarget(summary);

        // 2. Iterate Target to find files that exist there but not in Source
        compareTargetToSource(summary);

        LOGGER.log(INFO, "Comparison finished.");
        summary.printSummary();
    }

    private void compareSourceToTarget(final ComparisonSummary summary) {
        LOGGER.log(INFO, ">>> PASS 1: Checking Source against Target...");

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(sourceBucket)
                .encodingType(EncodingType.URL);

        if (!sourceFolder.isEmpty()) {
            requestBuilder.prefix(sourceFolder);
        }

        ListObjectsV2Response listResponse;

        do {
            listResponse = sourceClient.listObjectsV2(requestBuilder.build());

            for (S3Object srcObj : listResponse.contents()) {
                String srcKey = srcObj.key();
                summary.incrementTotalSource();

                // Calculate Target Key
                String relativeKey = srcKey.substring(sourceFolder.length());
                String targetKey = targetFolder + relativeKey;

                ComparisonAction action = ComparisonAction.ERROR;

                try {
                    // HEAD both source and target for full metadata
                    HeadObjectResponse sourceHead = sourceClient.headObject(req -> req.bucket(sourceBucket).key(srcKey));
                    HeadObjectResponse targetHead = targetClient.headObject(req -> req.bucket(targetBucket).key(targetKey));

                    // Compare using a looser tolerance for operational diffing.
                    action = areTimestampsSynced(sourceHead, targetHead, relativeKey);

                    if (action == ComparisonAction.MATCH) {
                        LOGGER.log(FINE, "MATCH: {0}", relativeKey);
                    }

                } catch (NoSuchKeyException e) {
                    action = ComparisonAction.DIFF_MISSING_TARGET;
                    LOGGER.log(WARNING, "DIFF: Missing in Target Bucket.\n\tFile: {0}\n\tFolder: {1}",
                            new Object[]{targetKey, targetFolder});
                } catch (Exception e) {
                    action = ComparisonAction.ERROR;
                    LOGGER.log(SEVERE, "Error checking key " + srcKey + " / " + targetKey, e);
                }
                summary.record(action);
            }

            requestBuilder.continuationToken(listResponse.nextContinuationToken());

        } while (Boolean.TRUE.equals(listResponse.isTruncated()));
    }

    private void compareTargetToSource(final ComparisonSummary summary) {
        LOGGER.log(INFO, ">>> PASS 2: Checking Target against Source (Finding Orphans)...");

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(targetBucket)
                .encodingType(EncodingType.URL);

        if (!targetFolder.isEmpty()) {
            requestBuilder.prefix(targetFolder);
        }

        ListObjectsV2Response listResponse;

        do {
            listResponse = targetClient.listObjectsV2(requestBuilder.build());

            for (S3Object targetObj : listResponse.contents()) {
                String targetKey = targetObj.key();
                summary.incrementTotalTarget();

                // Calculate expected Source Key
                String relativeKey = targetKey.substring(targetFolder.length());
                String sourceKey = sourceFolder + relativeKey;

                ComparisonAction action = ComparisonAction.ERROR;

                try {
                    sourceClient.headObject(req -> req.bucket(sourceBucket).key(sourceKey));
                    action = ComparisonAction.MATCH;
                } catch (NoSuchKeyException e) {
                    action = ComparisonAction.DIFF_MISSING_SOURCE;
                    LOGGER.log(WARNING, "DIFF: Missing in Source Bucket (Orphan in Target).\n\tFile: {0}\n\tFolder: {1}",
                            new Object[]{sourceKey, sourceFolder});
                } catch (Exception e) {
                    action = ComparisonAction.ERROR;
                    LOGGER.log(SEVERE, "Error checking source key " + sourceKey, e);
                }

                if (action != ComparisonAction.MATCH) {
                    summary.record(action);
                }
            }

            requestBuilder.continuationToken(listResponse.nextContinuationToken());

        } while (Boolean.TRUE.equals(listResponse.isTruncated()));
    }

    private ComparisonAction areTimestampsSynced(HeadObjectResponse sourceHead, HeadObjectResponse targetHead, String fileName) {
        boolean sameETag = sourceHead.eTag() != null
                && targetHead.eTag() != null
                && sourceHead.eTag().equals(targetHead.eTag());
        boolean sameSize = sourceHead.contentLength() == targetHead.contentLength();
        if (sameETag && sameSize) {
            LOGGER.log(FINE, "MATCH (ETag/Size match): {0}", fileName);
            return ComparisonAction.MATCH;
        }

        Instant sourceEffective = getEffectiveTimestamp(sourceHead, "source");
        Instant targetEffective = getEffectiveTimestamp(targetHead, "target");
        Instant targetEffectivePadded = targetEffective.plus(PADDING_MINUTES, ChronoUnit.MINUTES);

        if (!sourceEffective.isAfter(targetEffectivePadded)) {
            LOGGER.log(FINE, "MATCH (target within {0} minute tolerance): {1}",
                    new Object[]{PADDING_MINUTES, fileName});
            return ComparisonAction.MATCH;
        }

        LOGGER.log(WARNING,
                "DIFF: Timestamp mismatch.\n\tFile: {0}\n\tSource Effective: {1}\n\tTarget Effective: {2} [padded to {3}]\n\tDiff: {4} minutes",
                new Object[]{fileName, sourceEffective, targetEffective, targetEffectivePadded,
                        Duration.between(sourceEffective, targetEffectivePadded).abs().toMinutes()});
        return ComparisonAction.DIFF_TIMESTAMP;
    }

    /**
     * Computes the effective timestamp: max(copied-when, last-modified meta, built-in LastModified).
     * If copied-when < built-in, use built-in; else use the manual one (copied-when or last-modified).
     */
    private Instant getEffectiveTimestamp(HeadObjectResponse head, String context) {
        Instant builtIn = head.lastModified() != null ? head.lastModified() : Instant.EPOCH;

        java.util.Map<String, String> metadata = head.metadata() != null ? head.metadata() : java.util.Collections.emptyMap();

        Instant copiedWhen = Instant.EPOCH;
        String cwStr = metadata.get("copied-when");
        if (cwStr != null) {
            try {
                copiedWhen = Instant.parse(cwStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid copied-when in {0}: {1}", new Object[]{context, cwStr});
            }
        }

        Instant lastModMeta = Instant.EPOCH;
        String lmStr = metadata.get("last-modified");
        if (lmStr != null) {
            try {
                lastModMeta = Instant.parse(lmStr);
            } catch (Exception e) {
                LOGGER.log(FINE, "Invalid last-modified in {0}: {1}", new Object[]{context, lmStr});
            }
        }

        // Max of manual timestamps
        Instant maxManual = copiedWhen.isAfter(lastModMeta) ? copiedWhen : lastModMeta;

        // If manual < built-in, use built-in; else manual
        return maxManual.isAfter(builtIn) ? maxManual : builtIn;
    }
}
