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
import java.util.HashMap;
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
    private static final Instant NOW = Instant.parse("2025-04-29T12:00:00Z");
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
                .lastModified(oldDate) // Ensure LastModified is also old
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", oldDate.toString());
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsWithMetadataOlderThanThresholdButLastModifiedNewer() {
        // Arrange
        Instant oldMetadataDate = THRESHOLD.minusSeconds(3600); // 1 hour older than threshold
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(NOW) // Newer than threshold
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", "2024-04-28T05:35:08Z");
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsWithValidMetadataNewerThanThreshold() {
        Instant newDate = THRESHOLD.plusSeconds(5600); // 2025-04-28T14:14:58Z
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(NOW) // 2025-04-29T14:14:58Z
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", newDate.toString());
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);

        s3Cleaner.cleanOldObjects();

        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void testCleanOldObjectsWithMissingMetadataFallbackToLastModified() {
        // Arrange
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

        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(Collections.emptyMap()) // No last-modified
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsWithInvalidMetadataFormat() {
        // Arrange
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

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", "invalid-date-format"); // Invalid format
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsSkipsDirectory() {
        // Arrange
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

        // Assert
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

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", oldDate.toString());
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test1.jpg").build()));
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test2.jpg").build()));
    }

    @Test
    void testCleanOldObjectsHeadObjectFails() {
        // Arrange
        Instant oldLastModified = THRESHOLD.minusSeconds(3600);
        S3Object s3Object = S3Object.builder()
                .key("shared/non-compliance/omega/test.jpg")
                .lastModified(oldLastModified)
                .build();
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(s3Object)
                .isTruncated(false)
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(s3Client.headObject(any(HeadObjectRequest.class))).thenThrow(new RuntimeException("HeadObject failed"));

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build()));
    }

    @Test
    void testCleanOldObjectsDeleteObjectFails() {
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

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", oldDate.toString());
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("shared/non-compliance/omega/test.jpg").build())))
                .thenReturn(headResponse);
        doThrow(new RuntimeException("Delete failed")).when(s3Client).deleteObject(any(DeleteObjectRequest.class));

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
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

        Map<String, String> metadata = new HashMap<>();
        metadata.put("last-modified", oldDate.toString());
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .build();
        when(s3Client.headObject(eq(HeadObjectRequest.builder().bucket(BUCKET_NAME).key("test.jpg").build())))
                .thenReturn(headResponse);

        // Act
        s3Cleaner.cleanOldObjects();

        // Assert
        verify(s3Client, times(1)).deleteObject(eq(DeleteObjectRequest.builder().bucket(BUCKET_NAME).key("test.jpg").build()));
    }
}