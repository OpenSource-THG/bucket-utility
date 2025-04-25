package com.procure.thg.cockroachdb;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

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
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null, false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("new-file.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("new-file.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("old-file.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("old-file.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
    }

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
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive", false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("images/new.jpg")
                        .destinationBucket(targetBucket)
                        .destinationKey("archive/new.jpg")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("images/old.jpg")
                        .destinationBucket(targetBucket)
                        .destinationKey("archive/old.jpg")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("documents/doc.pdf")
                        .destinationBucket(targetBucket)
                        .destinationKey("archive/documents/doc.pdf")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
    }

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
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "backup", false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("new-file.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/new-file.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("file1.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/file1.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("subdir/file2.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/subdir/file2.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
    }

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

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null, false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, never()).copyObject(any(CopyObjectRequest.class));
    }

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
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive", false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("images/new.jpg")
                        .destinationBucket(targetBucket)
                        .destinationKey("archive/new.jpg")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("images/")
                        .destinationBucket(targetBucket)
                        .destinationKey("archive/")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
    }

    @Test
    void testCopyRecentObjectsEntireBucketNoFolder() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600; // 10 hours
        List<S3Object> objects = List.of(
                S3Object.builder().key("file1.txt").lastModified(now.minusSeconds(15 * 3600)).build(),
                S3Object.builder().key("subdir/file2.txt").lastModified(now.minusSeconds(5 * 3600)).build(),
                S3Object.builder().key("new-file.txt").lastModified(now.minusSeconds(5 * 3600)).build(),
                S3Object.builder().key("subdir/").lastModified(now.minusSeconds(5 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/subdir/file2.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/subdir/").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "backup", false);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(3)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("subdir/file2.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/subdir/file2.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("new-file.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/new-file.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("subdir/")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/subdir/")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("file1.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("backup/file1.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
    }

    @Test
    void testCopyRecentObjectsWithCopyExisting() {
        final var now = Instant.now();
        final int thresholdSeconds = 10 * 3600; // 10 hours
        List<S3Object> objects = List.of(
                S3Object.builder().key("new-file.txt").lastModified(now.minusSeconds(5 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
        when(targetClient.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(CopyObjectResponse.builder().build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null, true);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, times(1)).copyObject(any(CopyObjectRequest.class));
        verify(targetClient).copyObject(
                eq(CopyObjectRequest.builder()
                        .sourceBucket(sourceBucket)
                        .sourceKey("new-file.txt")
                        .destinationBucket(targetBucket)
                        .destinationKey("new-file.txt")
                        .metadataDirective(MetadataDirective.COPY)
                        .build())
        );
        verify(targetClient, never()).headObject(any(HeadObjectRequest.class)); // Skips existence check when copyExisting is true
    }
}