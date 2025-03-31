package com.procure.thg.cockroachdb;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3CopierTest {

    @Mock
    private S3Client sourceClient;

    @Mock
    private S3Client targetClient;

    private AutoCloseable closeable;

    private final String sourceBucket = "source-bucket";
    private final String targetBucket = "target-bucket";

    @BeforeEach
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    /** Tests basic copying of recent objects to the target bucket. */
    @Test
    void testCopyRecentObjectsBasic() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600; // 10 hours
        List<S3Object> objects = List.of(
                S3Object.builder().key("old-file.txt").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("new-file.txt").lastModified(now.minusSeconds(5 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(sourceClient.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength(7L).build(),
                        new ByteArrayInputStream("content".getBytes())
                ));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).putObject(any(PutObjectRequest.class), (RequestBody) any());
        verify(targetClient).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("new-file.txt").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("old-file.txt").build()),
                (RequestBody) any()
        );
    }

    /** Tests copying recent objects from a specific folder, preserving the folder structure. */
    @Test
    void testCopyRecentObjectsWithFolder() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600;
        List<S3Object> objects = List.of(
                S3Object.builder().key("images/old.jpg").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("images/new.jpg").lastModified(now.minusSeconds(5 * 3600)).build(),
                S3Object.builder().key("documents/doc.pdf").lastModified(now.minusSeconds(15 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(sourceClient.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength(7L).build(),
                        new ByteArrayInputStream("content".getBytes())
                ));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive");
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).putObject(any(PutObjectRequest.class), (RequestBody) any());
        verify(targetClient).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("archive/old.jpg").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("archive/documents/doc.pdf").build()),
                (RequestBody) any()
        );
    }

    /** Tests copying recent objects when no folder is specified, effectively copying the entire bucket. */
    @Test
    void testCopyRecentObjectsEntireBucket() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600;
        List<S3Object> objects = List.of(
                S3Object.builder().key("file1.txt").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("subdir/file2.txt").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("new-file.txt").lastModified(now.minusSeconds(5 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(sourceClient.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength(7L).build(),
                        new ByteArrayInputStream("content".getBytes())
                ));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "backup");
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).putObject(any(PutObjectRequest.class), (RequestBody) any());
        verify(targetClient).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("backup/new-file.txt").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("backup/file1.txt").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("backup/subdir/file2.txt").build()),
                (RequestBody) any()
        );
    }

    /** Tests behavior when there are no recent objects to copy. */
    @Test
    void testCopyRecentObjectsNoRecentObjects() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600;
        List<S3Object> objects = List.of(
                S3Object.builder().key("old-file.txt").lastModified(now.minusSeconds(15 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, never()).putObject(any(PutObjectRequest.class), (RequestBody) any());
    }

    /** Tests that folder markers (keys ending with '/') are not copied. */
    @Test
    void testCopyRecentObjectsWithFolderMarkers() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600;
        List<S3Object> objects = List.of(
                S3Object.builder().key("images/").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("images/new.jpg").lastModified(now.minusSeconds(5 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(sourceClient.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength(7L).build(),
                        new ByteArrayInputStream("content".getBytes())
                ));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive");
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).putObject(any(PutObjectRequest.class), (RequestBody) any());
        verify(targetClient).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build()),
                (RequestBody) any()
        );
        verify(targetClient, never()).putObject(
                eq(PutObjectRequest.builder().bucket(targetBucket).key("archive/").build()),
                (RequestBody) any()
        );
    }
}