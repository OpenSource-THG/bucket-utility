package com.procure.thg.cockroachdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.logging.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

public class App {

    private static final Logger LOGGER = Logger.getLogger(App.class.getName());

    private static final String THRESHOLD_SECONDS = "THRESHOLD_SECONDS";
    private static final String FOLDER = "FOLDER";
    private static final String ENABLE_MOVE = "ENABLE_MOVE";
    // New Environment Variable
    private static final String ENABLE_DIFF = "ENABLE_DIFF";

    private static final String TARGET_AWS_ACCESS_KEY_ID = "TARGET_AWS_ACCESS_KEY_ID";
    private static final String TARGET_AWS_SECRET_ACCESS_KEY = "TARGET_AWS_SECRET_ACCESS_KEY";
    private static final String TARGET_AWS_ENDPOINT_URL = "TARGET_AWS_ENDPOINT_URL";
    private static final String TARGET_BUCKET_NAME = "TARGET_BUCKET_NAME";
    private static final String TARGET_FOLDER = "TARGET_FOLDER";
    private static final Region REGION = Region.EU_WEST_1;
    private static final String AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL";
    private static final String COPY_METADATA = "COPY_METADATA";
    private static final String COPY_MODIFIED = "COPY_MODIFIED";

    public static void main(String[] args) {
        S3Client sourceClient = null;
        S3Client targetClient = null;
        try {
            LOGGER.log(INFO, "Initialising source S3 client...");
            sourceClient = S3Client.builder()
                    .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                    .endpointOverride(getEndpointUri())
                    .region(REGION)
                    .forcePathStyle(true)
                    .httpClientBuilder(ApacheHttpClient.builder()
                            .socketTimeout(Duration.ofSeconds(6000))
                            .connectionTimeout(Duration.ofSeconds(6000)))
                    .build();

            boolean enableMove = Boolean.parseBoolean(System.getenv(ENABLE_MOVE));
            boolean enableDiff = Boolean.parseBoolean(System.getenv(ENABLE_DIFF));

            long thresholdSeconds = getThresholdSeconds();
            String folder = getFolderPrefix();

            // === 1. DIFF MODE ===
            if (enableDiff) {
                LOGGER.log(INFO, "Running in DIFF mode...");
                targetClient = createTargetClient(); // Reused method for target init
                String targetBucket = System.getenv(TARGET_BUCKET_NAME);
                String targetFolder = System.getenv(TARGET_FOLDER);

                if (targetBucket == null) throw new IllegalArgumentException("TARGET_BUCKET_NAME is required for Diff.");

                S3Comparator comparator = new S3Comparator(
                        sourceClient, System.getenv("BUCKET_NAME"), folder,
                        targetClient, targetBucket, targetFolder
                );
                comparator.compareBuckets();

                // === 2. MOVE/COPY MODE ===
            } else if (enableMove) {
                targetClient = createTargetClient();
                String targetBucket = System.getenv(TARGET_BUCKET_NAME);
                String targetFolder = System.getenv(TARGET_FOLDER);
                boolean copyMetadata = Boolean.parseBoolean(System.getenv(COPY_METADATA));
                boolean copyModified = Boolean.parseBoolean(System.getenv(COPY_MODIFIED));

                S3Copier copier = new S3Copier(sourceClient, System.getenv("BUCKET_NAME"), folder,
                        targetClient, targetBucket, targetFolder, copyModified);
                if (copyMetadata) {
                    copier.syncMetaDataRecentObjects(thresholdSeconds);
                } else {
                    copier.copyRecentObjects(thresholdSeconds);
                }

                // === 3. CLEAN MODE ===
            } else {
                S3Cleaner cleaner = new S3Cleaner(sourceClient, thresholdSeconds, folder);
                cleaner.cleanOldObjects();
            }
        } catch (Exception e) {
            LOGGER.log(SEVERE, "Application failed", e);
            throw e;
        } finally {
            if (targetClient != null) {
                targetClient.close();
            }
            if (sourceClient != null) {
                sourceClient.close();
            }
            LOGGER.log(INFO, "S3 clients closed");
        }
    }

    // Extracted method to avoid duplication
    private static S3Client createTargetClient() {
        String targetAccessKey = System.getenv(TARGET_AWS_ACCESS_KEY_ID);
        String targetSecretKey = System.getenv(TARGET_AWS_SECRET_ACCESS_KEY);
        String targetEndpoint = System.getenv(TARGET_AWS_ENDPOINT_URL);

        if (targetAccessKey == null || targetSecretKey == null || targetEndpoint == null) {
            throw new IllegalArgumentException("Required target environment variables (TARGET_AWS_ACCESS_KEY_ID, TARGET_AWS_SECRET_ACCESS_KEY, TARGET_AWS_ENDPOINT_URL) must be set.");
        }

        LOGGER.log(INFO, "Initialising target S3 client...");
        return S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(targetAccessKey, targetSecretKey)))
                .endpointOverride(URI.create(targetEndpoint))
                .forcePathStyle(true)
                .region(REGION)
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofSeconds(6000))
                        .connectionTimeout(Duration.ofSeconds(6000)))
                .build();
    }

    private static URI getEndpointUri() {
        final var uri = System.getenv(AWS_ENDPOINT_URL);
        if (uri == null || uri.isEmpty()) {
            var msg = AWS_ENDPOINT_URL + " environment variable not set";
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            LOGGER.log(SEVERE, "Invalid endpoint URI: {0}", uri);
            throw new IllegalArgumentException(e);
        }
    }

    private static long getThresholdSeconds() {
        final var thresholdEnv = System.getenv(THRESHOLD_SECONDS);
        // Threshold might not be strictly needed for Diff, but good to keep validation if you want to reuse it
        if (thresholdEnv == null || thresholdEnv.isEmpty()) {
            // For diff, we might default to 0 (all time) if not provided,
            // but let's stick to existing validation to avoid breaking changes.
            var msg = THRESHOLD_SECONDS + " environment variable not set";
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
        return Long.parseLong(thresholdEnv);
    }

    private static String getFolderPrefix() {
        return System.getenv(FOLDER);
    }
}