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
import static java.util.logging.Level.WARNING;

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
    private static final String BUCKET_NAME = "BUCKET_NAME";
    private static final String AWS_REGION = "AWS_REGION";
    private static final String AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL";
    private static final String COPY_METADATA = "COPY_METADATA";
    private static final String COPY_MODIFIED = "COPY_MODIFIED";
    private static final Duration S3_CLIENT_TIMEOUT = Duration.ofSeconds(6000);
    private static final Region REGION = getRegion();

    public static void main(String[] args) {
        S3Client sourceClient = null;
        S3Client targetClient = null;
        try {
            // Validate source credentials
            validateSourceCredentials();

            LOGGER.log(INFO, "Initialising source S3 client...");
            sourceClient = createS3Client(
                    getRequiredEnvVar(AWS_ENDPOINT_URL),
                    EnvironmentVariableCredentialsProvider.create());

            boolean enableMove = Boolean.parseBoolean(System.getenv(ENABLE_MOVE));
            long thresholdSeconds = getThresholdSeconds();
            String folder = getFolderPrefix();

            LOGGER.log(INFO, "Configuration: ENABLE_MOVE={0}, THRESHOLD_SECONDS={1}, FOLDER={2}",
                    new Object[]{enableMove, thresholdSeconds, folder});

            if (enableMove) {
                String targetAccessKey = System.getenv(TARGET_AWS_ACCESS_KEY_ID);
                String targetSecretKey = System.getenv(TARGET_AWS_SECRET_ACCESS_KEY);
                String targetEndpoint = System.getenv(TARGET_AWS_ENDPOINT_URL);
                String targetBucket = System.getenv(TARGET_BUCKET_NAME);
                String targetFolder = System.getenv(TARGET_FOLDER);
                boolean copyMetadata = Boolean.parseBoolean(System.getenv(COPY_METADATA));
                boolean copyModified = Boolean.parseBoolean(System.getenv(COPY_MODIFIED));

                validateTargetCredentials(targetAccessKey, targetSecretKey, targetEndpoint, targetBucket);

                LOGGER.log(INFO, "Copy mode: COPY_METADATA={0}, COPY_MODIFIED={1}, TARGET_BUCKET={2}",
                        new Object[]{copyMetadata, copyModified, targetBucket});

                LOGGER.log(INFO, "Initialising target S3 client...");
                targetClient = createS3Client(
                        targetEndpoint,
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(targetAccessKey, targetSecretKey)));

                String sourceBucket = getRequiredEnvVar(BUCKET_NAME);
                S3Copier copier = new S3Copier(sourceClient, sourceBucket, folder,
                        targetClient, targetBucket, targetFolder, copyModified);
                if (copyMetadata) {
                    copier.syncMetaDataRecentObjects(thresholdSeconds);
                } else {
                    copier.copyRecentObjects(thresholdSeconds);
                }
            } else {
                String sourceBucket = getRequiredEnvVar(BUCKET_NAME);
                S3Cleaner cleaner = new S3Cleaner(sourceClient, thresholdSeconds, folder);
                cleaner.cleanOldObjects();
            }
        } catch (IllegalArgumentException | URISyntaxException e) {
            LOGGER.log(SEVERE, "Application failed: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            LOGGER.log(SEVERE, "Application failed with unexpected error", e);
            System.exit(1);
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
        final var thresholdEnv = getRequiredEnvVar(THRESHOLD_SECONDS);
        try {
            long threshold = Long.parseLong(thresholdEnv);
            if (threshold < 0) {
                String msg = THRESHOLD_SECONDS + " must be non-negative, got: " + threshold;
                LOGGER.log(SEVERE, msg);
                throw new IllegalArgumentException(msg);
            }
            return threshold;
        } catch (NumberFormatException e) {
            String msg = THRESHOLD_SECONDS + " must be a valid number, got: " + thresholdEnv;
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg, e);
        }
    }

    private static String getFolderPrefix() {
        return System.getenv(FOLDER);
    }

    private static String getRequiredEnvVar(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            String msg = name + " environment variable not set";
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
        return value;
    }

    private static boolean isNullOrEmpty(String value) {
        return value == null || value.isEmpty();
    }

    private static void validateSourceCredentials() {
        String sourceAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String sourceSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (isNullOrEmpty(sourceAccessKey) || isNullOrEmpty(sourceSecretKey)) {
            String msg = "Source AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) must be set";
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private static void validateTargetCredentials(String targetAccessKey, String targetSecretKey,
                                                  String targetEndpoint, String targetBucket) {
        if (isNullOrEmpty(targetAccessKey) || isNullOrEmpty(targetSecretKey) ||
            isNullOrEmpty(targetEndpoint) || isNullOrEmpty(targetBucket)) {
            StringBuilder missingVars = new StringBuilder();
            if (isNullOrEmpty(targetAccessKey)) missingVars.append("TARGET_AWS_ACCESS_KEY_ID ");
            if (isNullOrEmpty(targetSecretKey)) missingVars.append("TARGET_AWS_SECRET_ACCESS_KEY ");
            if (isNullOrEmpty(targetEndpoint)) missingVars.append("TARGET_AWS_ENDPOINT_URL ");
            if (isNullOrEmpty(targetBucket)) missingVars.append("TARGET_BUCKET_NAME");
            String msg = "Missing required environment variables: " + missingVars.toString();
            LOGGER.log(SEVERE, msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private static S3Client createS3Client(String endpointUrl,
                                           software.amazon.awssdk.auth.credentials.AwsCredentialsProvider credentialsProvider) throws URISyntaxException {
        return S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .endpointOverride(new URI(endpointUrl))
                .region(REGION)
                .forcePathStyle(true)
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(S3_CLIENT_TIMEOUT)
                        .connectionTimeout(S3_CLIENT_TIMEOUT))
                .build();
    }

    private static Region getRegion() {
        String regionEnv = System.getenv(AWS_REGION);
        if (regionEnv != null && !regionEnv.isEmpty()) {
            try {
                return Region.of(regionEnv);
            } catch (IllegalArgumentException e) {
                LOGGER.log(WARNING, "Invalid AWS_REGION: {0}, using default EU_WEST_1", regionEnv);
                return Region.EU_WEST_1;
            }
        }
        return Region.EU_WEST_1;
    }

    private static void validateBucketName(String bucketName) {
        if (bucketName == null || bucketName.isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        if (bucketName.length() < 3 || bucketName.length() > 63) {
            throw new IllegalArgumentException("Bucket name must be 3-63 characters, got: " + bucketName);
        }
        if (!bucketName.matches("[a-z0-9.-]+")) {
            throw new IllegalArgumentException("Bucket name contains invalid characters: " + bucketName);
        }
    }

}