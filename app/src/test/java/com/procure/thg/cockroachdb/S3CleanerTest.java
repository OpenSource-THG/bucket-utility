package com.procure.thg.cockroachdb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.*;

class S3CleanerTest {

    @Mock
    private S3Client s3Client;

    private S3Cleaner s3Cleaner;

    private AutoCloseable closeable;

    private static final String BUCKET_NAME = "thg-procurement-data";
    private static final String FOLDER = "shared/non-compliance/";
    private static final long THRESHOLD_SECONDS = 90000; // ~25 hours
    private static final Instant NOW = Instant.now();
    private static final Instant THRESHOLD = NOW.minusSeconds(THRESHOLD_SECONDS);

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        // Set environment variable for BUCKET_NAME using reflection to modify env
        setEnv("BUCKET_NAME", BUCKET_NAME);
        // Initialize S3Cleaner with mocks
        s3Cleaner = new S3Cleaner(s3Client, THRESHOLD_SECONDS, FOLDER);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    // Utility method to set environment variables for testing
    @SuppressWarnings("unchecked")
    private void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            java.lang.reflect.Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set environment variable", e);
        }
    }

    @Test
    void testCleanOldObjectsWithValidMetadataOlderThanThreshold() {
        // Arrange
        Instant oldDate = THRESHOLD.minusSeconds(3600); // 1 hour older than threshold
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(oldDate) // Use lastModified directly
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert - no more HeadObject calls, deletion is based on lastModified
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsWithLastModifiedNewer() {
        // Arrange
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(NOW) // Newer than threshold - should not be deleted
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert - no deletion because lastModified is newer than threshold
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsWithValidMetadataNewerThanThreshold() {
        Instant newDate = THRESHOLD.plusSeconds(5600);
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(newDate) // Newer than threshold
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        s3Cleaner.cleanOldObjects();

        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsUsesLastModifiedDirectly() {
        // Arrange - verify we use lastModified directly from ListObjects
        Instant oldLastModified = THRESHOLD.minusSeconds(3600); // 1 hour older
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(oldLastModified)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert - no HeadObject calls, deletion uses lastModified from ListObjects
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsSkipsDirectoryMarkers() {
        // Arrange - test that objects ending with / are skipped
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/")
                .lastModified(THRESHOLD.minusSeconds(3600))
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert - directory marker should be skipped
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }


    @Test
    void testCleanOldObjectsEmptyBucket() {
        // Arrange
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(Collections.emptyList())
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsDryRun() {
        // Arrange
        Instant oldDate = THRESHOLD.minusSeconds(3600);
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(oldDate)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        S3Cleaner dryRunCleaner = new S3Cleaner(s3Client, THRESHOLD_SECONDS, FOLDER, true);

        // Act
        dryRunCleaner.cleanOldObjects();

        // Assert - should not call deleteObject in dry-run mode
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsWithPagination() {
        // Arrange
        Instant oldDate = THRESHOLD.minusSeconds(3600);
        S3Object s3Object1 = S3Object.builder()
                .key("shared/non-compliance/omega/test1.jpg")
                .lastModified(oldDate)
                .build();
        S3Object s3Object2 = S3Object.builder()
                .key("shared/non-compliance/omega/test2.jpg")
                .lastModified(oldDate)
                .build();
        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
                .contents(s3Object1)
                .isTruncated(true)
                .nextContinuationToken("token")
                .build();
        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
                .contents(s3Object2)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2((ListObjectsV2Request) argThat(req -> req instanceof ListObjectsV2Request && ((ListObjectsV2Request) req).continuationToken() == null))).thenReturn(page1);
        when(s3Client.listObjectsV2((ListObjectsV2Request) argThat(req -> req instanceof ListObjectsV2Request && "token".equals(((ListObjectsV2Request) req).continuationToken())))).thenReturn(page2);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert - no HeadObject calls, both objects deleted based on lastModified
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test1.jpg").build()));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test2.jpg").build()));
    }

    @Test
    void testCleanOldObjectsWithValidation() {
        // Arrange - test that constructor validates thresholdSeconds
        try {
            new S3Cleaner(s3Client, -1, FOLDER);
            throw new AssertionError("Should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
    }

    @Test
    void testCleanOldObjectsDeleteObjectFails() {
        // Arrange - test graceful handling of delete failures
        Instant oldDate = THRESHOLD.minusSeconds(3600);
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(oldDate)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        doThrow(new RuntimeException("Delete failed")).when(s3Client).deleteObject(any(DeleteObjectRequest.class));

        // Act - should not throw even if delete fails
        s3Cleaner.cleanOldObjects();

        // Assert - deletion was attempted even though it failed
        verify(s3Client, times(1)).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsWithNoFolderPrefix() {
        // Arrange
        s3Cleaner = new S3Cleaner(s3Client, THRESHOLD_SECONDS, null); // No folder
        Instant oldDate = THRESHOLD.minusSeconds(3600);
        S3Object s3Object = S3Object.builder()
                .key("test.jpg")
                .lastModified(oldDate)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, never()).headObject(any(HeadObjectRequest.class));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("test.jpg").build()));
    }
}