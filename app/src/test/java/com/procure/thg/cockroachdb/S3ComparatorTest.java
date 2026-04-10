package com.procure.thg.cockroachdb;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3ComparatorTest {

    @Mock
    private S3Client sourceClient;

    @Mock
    private S3Client targetClient;

    private AutoCloseable closeable;
    private TestLogHandler logHandler;
    private Logger logger;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        logger = Logger.getLogger(S3Comparator.class.getName());
        logHandler = new TestLogHandler();
        logger.addHandler(logHandler);
        logger.setUseParentHandlers(false);
        logger.setLevel(Level.ALL);
    }

    @AfterEach
    void tearDown() throws Exception {
        logger.removeHandler(logHandler);
        logger.setUseParentHandlers(true);
        closeable.close();
    }

    @Test
    void compareBucketsTreatsMatchingEtagAndSizeAsMatch() {
        stubSingleObjectPass("source/file.txt", "target/file.txt");
        when(sourceClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder().eTag("same").contentLength(5L).build());
        when(targetClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder().eTag("same").contentLength(5L).build());

        S3Comparator comparator = new S3Comparator(
                sourceClient, "source-bucket", "source",
                targetClient, "target-bucket", "target");

        comparator.compareBuckets();

        assertTrue(logHandler.contains("Total Diff (Need Copy/Update): {0} ({1} of Source) 0"),
                () -> "Expected no diffs but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Errors Encountered: {0} 0"),
                () -> "Expected no errors but got logs:\n" + logHandler.joined());
    }

    @Test
    void compareBucketsTreatsEqualEffectiveTimestampsAsMatch() {
        stubSingleObjectPass("source/file.txt", "target/file.txt");
        String timestamp = "2026-04-10T09:00:00Z";
        when(sourceClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("source")
                        .contentLength(5L)
                        .metadata(Map.of("copied-when", timestamp))
                        .build());
        when(targetClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("target")
                        .contentLength(5L)
                        .metadata(Map.of("last-modified", timestamp))
                        .build());

        S3Comparator comparator = new S3Comparator(
                sourceClient, "source-bucket", "source",
                targetClient, "target-bucket", "target");

        comparator.compareBuckets();

        assertTrue(logHandler.contains("Total Diff (Need Copy/Update): {0} ({1} of Source) 0"),
                () -> "Expected no diffs but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Errors Encountered: {0} 0"),
                () -> "Expected no errors but got logs:\n" + logHandler.joined());
    }

    @Test
    void compareBucketsTreatsSourceWithinTenMinuteToleranceAsMatch() {
        stubSingleObjectPass("source/file.txt", "target/file.txt");
        when(sourceClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("source")
                        .contentLength(5L)
                        .metadata(Map.of("last-modified", "2026-04-10T09:05:00Z"))
                        .build());
        when(targetClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("target")
                        .contentLength(5L)
                        .metadata(Map.of("last-modified", "2026-04-10T09:00:00Z"))
                        .build());

        S3Comparator comparator = new S3Comparator(
                sourceClient, "source-bucket", "source",
                targetClient, "target-bucket", "target");

        comparator.compareBuckets();

        assertTrue(logHandler.contains("Total Diff (Need Copy/Update): {0} ({1} of Source) 0"),
                () -> "Expected no diffs inside the tolerance window but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Timestamp Mismatch: {0} 0"),
                () -> "Expected no timestamp mismatch inside the tolerance window but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Errors Encountered: {0} 0"),
                () -> "Expected no errors but got logs:\n" + logHandler.joined());
    }

    @Test
    void compareBucketsReportsTimestampMismatchWhenSourceIsOutsideTenMinuteTolerance() {
        stubSingleObjectPass("source/file.txt", "target/file.txt");
        when(sourceClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("source")
                        .contentLength(5L)
                        .metadata(Map.of("last-modified", "2026-04-10T09:11:00Z"))
                        .build());
        when(targetClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("target")
                        .contentLength(5L)
                        .metadata(Map.of("last-modified", "2026-04-10T09:00:00Z"))
                        .build());

        S3Comparator comparator = new S3Comparator(
                sourceClient, "source-bucket", "source",
                targetClient, "target-bucket", "target");

        comparator.compareBuckets();

        assertTrue(logHandler.contains("Timestamp Mismatch: {0} 1"),
                () -> "Expected one timestamp mismatch outside the tolerance window but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Errors Encountered: {0} 0"),
                () -> "Expected no errors but got logs:\n" + logHandler.joined());
    }

    @Test
    void compareBucketsReportsMissingTargetAndOrphanCounts() {
        ListObjectsV2Response sourceList = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("source/file.txt").build())
                .isTruncated(false)
                .build();
        ListObjectsV2Response targetList = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("target/orphan.txt").build())
                .isTruncated(false)
                .build();
        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(sourceList);
        when(targetClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(targetList);
        when(sourceClient.headObject(anyHeadRequest()))
                .thenReturn(HeadObjectResponse.builder().build())
                .thenThrow(NoSuchKeyException.builder().message("missing").build());
        when(targetClient.headObject(anyHeadRequest()))
                .thenThrow(NoSuchKeyException.builder().message("missing").build());

        S3Comparator comparator = new S3Comparator(
                sourceClient, "source-bucket", "source",
                targetClient, "target-bucket", "target");

        comparator.compareBuckets();

        assertTrue(logHandler.contains("Missing in Target: {0} 1"),
                () -> "Expected one missing target but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Orphan Files Found: {0} ({1} of Target) 1"),
                () -> "Expected one orphan but got logs:\n" + logHandler.joined());
        assertTrue(logHandler.contains("Errors Encountered: {0} 0"),
                () -> "Expected no errors but got logs:\n" + logHandler.joined());
    }

    private void stubSingleObjectPass(String sourceKey, String targetKey) {
        ListObjectsV2Response sourceList = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(sourceKey).build())
                .isTruncated(false)
                .build();
        ListObjectsV2Response targetList = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(targetKey).build())
                .isTruncated(false)
                .build();
        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(sourceList);
        when(targetClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(targetList);
    }

    @SuppressWarnings("unchecked")
    private Consumer<HeadObjectRequest.Builder> anyHeadRequest() {
        return (Consumer<HeadObjectRequest.Builder>) any(Consumer.class);
    }

    private static class TestLogHandler extends Handler {
        private final StringBuilder buffer = new StringBuilder();

        @Override
        public void publish(LogRecord record) {
            buffer.append(record.getMessage());
            Object[] parameters = record.getParameters();
            if (parameters != null && parameters.length > 0) {
                for (Object parameter : parameters) {
                    buffer.append(" ").append(parameter);
                }
            }
            buffer.append('\n');
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }

        private boolean contains(String text) {
            return buffer.toString().contains(text);
        }

        private String joined() {
            return buffer.toString();
        }
    }
}
