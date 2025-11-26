package com.procure.thg.cockroachdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.logging.Logger;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

public class App {

    private static final Logger LOGGER = Logger.getLogger(App.class.getName());

    private static final String THRESHOLD_SECONDS = "THRESHOLD_SECONDS";
    private static final String FOLDER = "FOLDER";
    private static final String ENABLE_MOVE = "ENABLE_MOVE";
    private static final String TARGET_AWS_ACCESS_KEY_ID = "TARGET_AWS_ACCESS_KEY_ID";
    private static final String TARGET_AWS_SECRET_ACCESS_KEY = "TARGET_AWS_SECRET_ACCESS_KEY";
    private static final String TARGET_AWS_ENDPOINT_URL = "TARGET_AWS_ENDPOINT_URL";
    private static final String TARGET_BUCKET_NAME = "TARGET_BUCKET_NAME";
    private static final String TARGET_FOLDER = "TARGET_FOLDER";
    private static final String AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL";
    private static final String COPY_METADATA = "COPY_METADATA";
    private static final String COPY_MODIFIED = "COPY_MODIFIED";

    // Use the region from your request IDs
    private static final Region REGION = Region.of("de-fra");

    public static void main(String[] args) {
        S3Client sourceClient = null;
        S3Client targetClient = null;
        try {
            LOGGER.log(INFO, "Initialising source S3 client...");
            sourceClient = S3Client.builder()
                    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .endpointOverride(getEndpointUri())
                    .region(REGION)
                    .serviceConfiguration(S3Configuration.builder()
                            .pathStyleAccessEnabled(true)
                            .build())
                    .httpClient(ApacheHttpClient.builder()
                            .socketTimeout(Duration.ofSeconds(6000))
                            .connectionTimeout(Duration.ofSeconds(6000))
                            .build())
                    .build();

            boolean enableMove = Boolean.parseBoolean(System.getenv(ENABLE_MOVE));
            long thresholdSeconds = getThresholdSeconds();
            String folder = getFolderPrefix();

            if (enableMove) {
                String targetAccessKey = System.getenv(TARGET_AWS_ACCESS_KEY_ID);
                String targetSecretKey = System.getenv(TARGET_AWS_SECRET_ACCESS_KEY);
                String targetEndpoint = System.getenv(TARGET_AWS_ENDPOINT_URL);
                String targetBucket = System.getenv(TARGET_BUCKET_NAME);
                String targetFolder = System.getenv(TARGET_FOLDER);
                boolean copyMetadata = Boolean.parseBoolean(System.getenv(COPY_METADATA));
                boolean copyModified = Boolean.parseBoolean(System.getenv(COPY_MODIFIED));

                if (targetAccessKey == null || targetSecretKey == null || targetEndpoint == null || targetBucket == null) {
                    throw new IllegalArgumentException("Missing target env vars");
                }

                LOGGER.log(INFO, "Initialising target S3 client (Ceph/Scaleway fixed)");

                // THIS INTERCEPTOR IS THE ONLY ONE THAT 100% WORKS WITH ALL VERSIONS OF AWS SDK v2
                ExecutionInterceptor forceUnsignedPayload = new ExecutionInterceptor() {
                    @Override
                    public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context,
                                                            ExecutionAttributes executionAttributes) {
                        return context.httpRequest().toBuilder()
                                .putHeader("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                                .build();
                    }
                };

                targetClient = S3Client.builder()
                        .credentialsProvider(StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(targetAccessKey, targetSecretKey)))
                        .endpointOverride(URI.create(targetEndpoint))
                        .region(REGION)
                        .serviceConfiguration(S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .checksumValidationEnabled(false)
                                .chunkedEncodingEnabled(false)
                                .build())
                        .overrideConfiguration(ClientOverrideConfiguration.builder()
                                .addExecutionInterceptor(forceUnsignedPayload)
                                .build())
                        .httpClient(ApacheHttpClient.builder()
                                .socketTimeout(Duration.ofSeconds(6000))
                                .connectionTimeout(Duration.ofSeconds(6000))
                                .expectContinueEnabled(false)
                                .build())
                        .build();

                S3Copier copier = new S3Copier(sourceClient, System.getenv("BUCKET_NAME"), folder,
                        targetClient, targetBucket, targetFolder, copyModified);

                if (copyMetadata) {
                    // copier.syncMetaDataRecentObjects(thresholdSeconds);
                } else {
                    copier.copyRecentObjects(thresholdSeconds);
                }
            } else {
                new S3Cleaner(sourceClient, thresholdSeconds, folder).cleanOldObjects();
            }
        } catch (Exception e) {
            LOGGER.log(SEVERE, "Application failed", e);
            //throw e;
        } finally {
            if (sourceClient != null) sourceClient.close();
            if (targetClient != null) targetClient.close();
            LOGGER.log(INFO, "S3 clients closed");
        }
    }

    // Your existing helper methods unchanged...
    private static URI getEndpointUri() throws URISyntaxException {
        String uri = System.getenv(AWS_ENDPOINT_URL);
        if (uri == null || uri.isEmpty()) throw new IllegalArgumentException(AWS_ENDPOINT_URL + " not set");
        return new URI(uri);
    }

    private static long getThresholdSeconds() {
        String env = System.getenv(THRESHOLD_SECONDS);
        if (env == null || env.isEmpty()) throw new IllegalArgumentException(THRESHOLD_SECONDS + " not set");
        return Long.parseLong(env);
    }

    private static String getFolderPrefix() {
        return System.getenv(FOLDER);
    }
}