package com.procure.thg.cockroachdb;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

class AppTest {

  @Mock
  private S3Client s3Client;

  private AutoCloseable closeable;

  @BeforeEach
  public void openMocks() {
    closeable = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  public void releaseMocks() throws Exception {
    closeable.close();
  }

  private static Stream<Arguments> arguments() {
    return Stream.of(
        Arguments.of(1, 12*3600, "/images"),
        Arguments.of(0, 20*3600, "/images/"),
        Arguments.of(2, 2*3600, null)
    );
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void testDeleteOldObjects(final int numOfInvocations, final int thresholdSeconds, final String folder) {
    final var s3Objects = new ArrayList<S3Object>();
    final var now = Instant.now();
    s3Objects.add(S3Object.builder().key("old-object").lastModified(now.minusSeconds(13 * 3600)).build());
    s3Objects.add(S3Object.builder().key("new-object").lastModified(now.minusSeconds(5 * 3600)).build());

    ListObjectsV2Response listObjectsV2Response = ListObjectsV2Response.builder()
        .contents(s3Objects)
        .isTruncated(false)
        .build();

    when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Response);

    final S3Cleaner app = new S3Cleaner(s3Client, thresholdSeconds, folder);
    app.cleanOldObjects();

    verify(s3Client, times(numOfInvocations)).deleteObject(any(DeleteObjectRequest.class));
  }
}
