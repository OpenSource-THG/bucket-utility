package com.procure.thg.cockroachdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

class S3CopierTest {

    private static final Instant NOW = Instant.parse("2026-04-10T10:15:30Z");

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
        final Instant now = Instant.now();
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
        stubSourceHeadObject("new-file.txt", 7L, now.minusSeconds(5 * 3600));
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null, true);
        copier.copyRecentObjects(thresholdSeconds);

        assertPutObjectKeys("new-file.txt");
    }

    @Test
    void testCopyRecentObjectsWithFolder() {
        final Instant now = Instant.now();
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
        stubSourceHeadObject("images/new.jpg", 7L, now.minusSeconds(5 * 3600));
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(thresholdSeconds);

        assertPutObjectKeys("archive/new.jpg");
    }

    @Test
    void testCopyRecentObjectsEntireBucket() {
        final Instant now = Instant.now();
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
        stubSourceHeadObject("new-file.txt", 7L, now.minusSeconds(5 * 3600));
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "backup", true);
        copier.copyRecentObjects(thresholdSeconds);

        assertPutObjectKeys("backup/new-file.txt");
    }

    @Test
    void testCopyRecentObjectsNoRecentObjects() {
        final Instant now = Instant.now();
        final int thresholdSeconds = 10 * 3600;
        List<S3Object> objects = List.of(
                S3Object.builder().key("old-file.txt").lastModified(now.minusSeconds(15 * 3600)).build()
        );

        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .isTruncated(false)
                .build();

        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, null, true);
        copier.copyRecentObjects(thresholdSeconds);

        verify(targetClient, never()).putObject(any(PutObjectRequest.class), (RequestBody) any());
    }

    @Test
    void testCopyRecentObjectsWithFolderMarkers() {
        final Instant now = Instant.now();
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
        stubSourceHeadObject("images/new.jpg", 7L, now.minusSeconds(5 * 3600));
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/new.jpg").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, "images", targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(thresholdSeconds);

        assertPutObjectKeys("archive/new.jpg");
    }

    @Test
    void testCopyRecentObjectsEntireBucketNoFolder() {
        final Instant now = Instant.now();
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
        when(sourceClient.getObject(any(GetObjectRequest.class)))
                .thenReturn(new ResponseInputStream<>(
                        GetObjectResponse.builder().contentLength(7L).build(),
                        new ByteArrayInputStream("content".getBytes())
                ));
        stubSourceHeadObject("subdir/file2.txt", 7L, now.minusSeconds(5 * 3600));
        stubSourceHeadObject("new-file.txt", 7L, now.minusSeconds(5 * 3600));
        stubSourceHeadObject("subdir/", 7L, now.minusSeconds(5 * 3600));
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/subdir/file2.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/new-file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("backup/subdir/").build())))
                .thenThrow(NoSuchKeyException.builder().message("Object not found").build());

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "backup", true);
        copier.copyRecentObjects(thresholdSeconds);

        assertPutObjectKeys("backup/subdir/file2.txt", "backup/new-file.txt", "backup/subdir/");
    }

    @Test
    void testCopyRecentObjectsSkipsWhenTargetETagAndSizeMatch() {
        stubRecentObject("file.txt");
        HeadObjectResponse sourceHead = HeadObjectResponse.builder()
                .eTag("same-etag")
                .contentLength(42L)
                .lastModified(NOW)
                .build();
        HeadObjectResponse targetHead = HeadObjectResponse.builder()
                .eTag("same-etag")
                .contentLength(42L)
                .lastModified(NOW.minusSeconds(600))
                .build();

        when(sourceClient.headObject(eq(HeadObjectRequest.builder().bucket(sourceBucket).key("file.txt").build())))
                .thenReturn(sourceHead);
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/file.txt").build())))
                .thenReturn(targetHead);

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(10 * 3600);

        verify(sourceClient, never()).getObject(any(GetObjectRequest.class));
        verify(targetClient, never()).putObject(any(PutObjectRequest.class), (RequestBody) any());
    }

    @Test
    void testCopyRecentObjectsSkipsWhenEffectiveTimestampsMatch() {
        stubRecentObject("file.txt");
        Instant effectiveTimestamp = Instant.parse("2026-04-10T09:00:00Z");

        HeadObjectResponse sourceHead = HeadObjectResponse.builder()
                .eTag("source-etag")
                .contentLength(42L)
                .lastModified(effectiveTimestamp)
                .metadata(Map.of("copied-when", effectiveTimestamp.toString()))
                .build();
        HeadObjectResponse targetHead = HeadObjectResponse.builder()
                .eTag("target-etag")
                .contentLength(42L)
                .lastModified(effectiveTimestamp)
                .metadata(Map.of("last-modified", effectiveTimestamp.toString()))
                .build();

        when(sourceClient.headObject(eq(HeadObjectRequest.builder().bucket(sourceBucket).key("file.txt").build())))
                .thenReturn(sourceHead);
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/file.txt").build())))
                .thenReturn(targetHead);

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(10 * 3600);

        verify(sourceClient, never()).getObject(any(GetObjectRequest.class));
        verify(targetClient, never()).putObject(any(PutObjectRequest.class), (RequestBody) any());
    }

    @Test
    void testCopyRecentObjectsCopiesWhenEffectiveTimestampsDiffer() {
        stubRecentObject("file.txt");
        HeadObjectResponse sourceHead = HeadObjectResponse.builder()
                .eTag("source-etag")
                .contentLength(42L)
                .lastModified(NOW)
                .metadata(Map.of("last-modified", NOW.toString()))
                .build();
        HeadObjectResponse targetHead = HeadObjectResponse.builder()
                .eTag("target-etag")
                .contentLength(42L)
                .lastModified(NOW.minusSeconds(300))
                .metadata(Map.of("last-modified", NOW.minusSeconds(300).toString()))
                .build();

        when(sourceClient.headObject(eq(HeadObjectRequest.builder().bucket(sourceBucket).key("file.txt").build())))
                .thenReturn(sourceHead);
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/file.txt").build())))
                .thenReturn(targetHead);
        when(sourceClient.getObject(any(GetObjectRequest.class))).thenReturn(responseStream(
                GetObjectResponse.builder()
                        .contentLength(7L)
                        .lastModified(NOW)
                        .metadata(new HashMap<>())
                        .build()));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(10 * 3600);

        verify(targetClient, times(1)).putObject(any(PutObjectRequest.class), (RequestBody) any());
    }

    @Test
    void testCopyRecentObjectsWritesEffectiveLastModifiedMetadata() {
        stubRecentObject("file.txt");
        Instant copiedWhen = Instant.parse("2026-04-10T09:00:00Z");
        Instant lastModified = Instant.parse("2026-04-10T08:30:00Z");

        when(sourceClient.headObject(eq(HeadObjectRequest.builder().bucket(sourceBucket).key("file.txt").build())))
                .thenReturn(HeadObjectResponse.builder()
                        .eTag("source-etag")
                        .contentLength(7L)
                        .lastModified(lastModified)
                        .build());
        when(targetClient.headObject(eq(HeadObjectRequest.builder().bucket(targetBucket).key("archive/file.txt").build())))
                .thenThrow(NoSuchKeyException.builder().message("missing").build());
        when(sourceClient.getObject(any(GetObjectRequest.class))).thenReturn(responseStream(
                GetObjectResponse.builder()
                        .contentLength(7L)
                        .lastModified(lastModified)
                        .metadata(Map.of("copied-when", copiedWhen.toString()))
                        .build()));

        S3Copier copier = new S3Copier(sourceClient, sourceBucket, null, targetClient, targetBucket, "archive", true);
        copier.copyRecentObjects(10 * 3600);

        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(targetClient).putObject(requestCaptor.capture(), (RequestBody) any());
        assertEquals(copiedWhen.toString(), requestCaptor.getValue().metadata().get("last-modified"));
    }

    private void stubRecentObject(String key) {
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(key).lastModified(NOW.minusSeconds(60)).build())
                .isTruncated(false)
                .build();
        when(sourceClient.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(response);
    }

    private void stubSourceHeadObject(String key, long contentLength, Instant lastModified) {
        when(sourceClient.headObject(eq(HeadObjectRequest.builder().bucket(sourceBucket).key(key).build())))
                .thenReturn(HeadObjectResponse.builder()
                        .contentLength(contentLength)
                        .lastModified(lastModified)
                        .metadata(Map.of())
                        .build());
    }

    private ResponseInputStream<GetObjectResponse> responseStream(GetObjectResponse response) {
        return new ResponseInputStream<>(response, new ByteArrayInputStream("content".getBytes()));
    }

    private void assertPutObjectKeys(String... expectedKeys) {
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(targetClient, times(expectedKeys.length)).putObject(requestCaptor.capture(), (RequestBody) any());
        List<String> actualKeys = requestCaptor.getAllValues().stream().map(PutObjectRequest::key).toList();
        assertEquals(List.of(expectedKeys), actualKeys);
        assertTrue(requestCaptor.getAllValues().stream().allMatch(request -> targetBucket.equals(request.bucket())));
    }
}
