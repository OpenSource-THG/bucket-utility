# S3 Object Management Tool

This Java application provides functionality to clean old objects from an AWS S3 bucket or copy/sync objects between S3 buckets based on a time threshold.

It supports two main modes:

* Cleaning old objects
* Copying recent objects (with optional metadata synchronization)

The project is built with Gradle, containerized using Docker, and includes a GitHub Actions workflow for CI/CD.

---

## Features

* **S3Cleaner**: Deletes objects from an S3 bucket older than a specified threshold (in seconds).
* **S3Copier**: Copies recent objects from a source S3 bucket to a target bucket or synchronizes metadata for objects newer than the threshold.
* Configurable via environment variables.
* Handles pagination for large S3 buckets.
* Robust error handling and logging using Java's `java.util.logging`.
* Supports folder-specific operations within buckets.
* Built with Gradle and containerized with Docker.
* Automated CI/CD pipeline with GitHub Actions for building, testing, and deploying Docker images.

---

## Prerequisites

* Java: JDK 17
* Gradle: Version compatible with the `build.gradle` configuration (uses Gradle wrapper)
* Docker: For building and running the containerized application
* AWS SDK for Java: Version 2.20.0 (managed by Gradle)
* AWS Credentials: Configured via environment variables for the source bucket
* Target Bucket Credentials (if copying): Access key, secret key, and endpoint URL for the target bucket
* GitHub Actions: For CI/CD (optional)

### Dependencies:

* `software.amazon.awssdk:s3:2.20.0`
* `software.amazon.awssdk:apache-client:2.20.0`
* `org.slf4j:slf4j-jdk14:1.7.30` (runtime)
* **Test dependencies:** `junit-jupiter:5.8.1`, `mockito-core:3.6.0`

---

## Setup

### Clone the Repository

```sh
git clone <repository-url>
cd <repository-directory>
```

### Verify Gradle Configuration

The project uses:

* Java 17 toolchain
* Dependencies for AWS SDK, SLF4J, JUnit, and Mockito
* Shadow plugin for creating a fat JAR (`app-all.jar`)
* Application plugin with main class `com.procure.thg.cockroachdb.App`

To sync dependencies:

```sh
./gradlew build
```

### Configure Environment Variables

Set the required environment variables based on the operation mode (cleaning or copying).

#### Common Variables:

```sh
export AWS_ENDPOINT_URL="https://s3.eu-west-1.amazonaws.com"
export BUCKET_NAME="my-source-bucket"
export THRESHOLD_SECONDS="86400" # 1 day
export FOLDER="my-folder/" # optional
```

#### For Cleaning Mode:

No additional variables required.

#### For Copying Mode:

```sh
export ENABLE_MOVE="true"
export TARGET_AWS_ACCESS_KEY_ID="your-target-access-key"
export TARGET_AWS_SECRET_ACCESS_KEY="your-target-secret-key"
export TARGET_AWS_ENDPOINT_URL="https://target-s3-endpoint.com"
export TARGET_BUCKET_NAME="my-target-bucket"
export TARGET_FOLDER="target-folder/" # optional
export COPY_METADATA="true" # optional
```

### Build the Project

```sh
./gradlew shadowJar
```

The output JAR will be located at `build/libs/app-all.jar`.

---

## Docker Setup

The provided Dockerfile creates a container based on `openjdk:17-jdk-slim`:

* Copies the fat JAR (`app-all.jar`) and an `entrypoint.sh` script
* Sets the working directory to `/app`
* Makes the entrypoint script executable

### To build the Docker image:

```sh
docker build -t s3-object-manager .
```

### To run the Docker container:

```sh
docker run --rm \
-e AWS_ENDPOINT_URL="https://s3.eu-west-1.amazonaws.com" \
-e BUCKET_NAME="my-source-bucket" \
-e THRESHOLD_SECONDS="86400" \
-e ENABLE_MOVE="true" \
-e TARGET_AWS_ACCESS_KEY_ID="your-target-access-key" \
-e TARGET_AWS_SECRET_ACCESS_KEY="your-target-secret-key" \
-e TARGET_AWS_ENDPOINT_URL="https://target-s3-endpoint.com" \
-e TARGET_BUCKET_NAME="my-target-bucket" \
-e TARGET_FOLDER="target-folder/" \
-e COPY_METADATA="true" \
s3-object-manager
```

> **Note:** Ensure `entrypoint.sh` exists and contains:

```bash
#!/bin/bash
java -jar /app/app.jar
```

---

## Usage

You can run the application using one of the following methods:

### Directly with Gradle:

```sh
./gradlew run
```

### Using the JAR:

```sh
java -jar build/libs/app-all.jar
```

### Using Docker:

See Docker run command above.

---

## Behavior Based on Mode

* **Cleaning Mode** (`ENABLE_MOVE=false` or unset):

    * Deletes objects older than `THRESHOLD_SECONDS` from the source bucket (optionally within `FOLDER`)

* **Copying Mode** (`ENABLE_MOVE=true`):

    * If `COPY_METADATA=true`: Synchronizes metadata for objects newer than `THRESHOLD_SECONDS`
    * If `COPY_METADATA=false`: Copies objects newer than `THRESHOLD_SECONDS` to the target bucket

---

## Code Structure

* `App.java`: Main entry point, initializes S3 clients, and orchestrates cleaning or copying
* `S3Cleaner.java`: Deletes old objects from the source bucket
* `S3Copier.java`: Copies objects or syncs metadata between buckets

---

## CI/CD with GitHub Actions

The project includes a GitHub Actions workflow (Java CI with Gradle) for automated building, testing, and deployment:

### Triggers

* Push/pull requests to the `main` branch
* Tags like `v*.*.*`

### Jobs

* **Build**: Sets up JDK 17, builds the project with Gradle, creates a Docker image, and pushes it to GitHub Container Registry (GHCR) for non-PR events
* **Dependency Submission**: Generates and submits a dependency graph for Dependabot alerts

### Permissions

* `contents:read`, `packages:write`, `id-token:write` (for image signing and registry access)

### Environment Variables

* `REGISTRY`: `ghcr.io`
* `IMAGE_NAME`: Derived from `github.repository`

### Secrets

* `GITHUB_TOKEN`: For registry authentication

To use the workflow:

1. Ensure your repository is set up on GitHub
2. Store the `entrypoint.sh` and `Dockerfile` in the app directory (as referenced by the workflow)
3. Push changes to trigger the workflow

---

## Logging

* Uses `java.util.logging` with SLF4J binding (`slf4j-jdk14`)
* Logs key events (client initialization, object processing, errors) to the console
* Customize logging via a `logging.properties` file or system properties

---

## Error Handling

* Validates environment variables, throwing `IllegalArgumentException` for missing/invalid values
* Handles S3 operation exceptions (e.g., `NoSuchKeyException`, network errors) with appropriate logging
* Ensures S3 clients are closed in a `finally` block to prevent resource leaks

---

## Notes

* **Region**: Hardcoded to `EU_WEST_1` in `App.java`. Update if needed.
* **Timeouts**: S3 clients use 6000-second socket/connection timeouts.
* **Path Style**: Enabled for compatibility with non-AWS S3 endpoints (e.g., MinIO, Ceph)
* **Metadata Sync**: Uses `CopyObject` with `MetadataDirective.REPLACE` for metadata updates
* **Docker**: Assumes an `entrypoint.sh` script exists. Example:

```bash
#!/bin/bash
java -jar /app/app.jar
```

---

## Troubleshooting

| Issue                         | Solution                                                                                                                 |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Missing Environment Variables | Verify all required variables are set                                                                                    |
| Permission Issues             | Ensure AWS credentials have appropriate permissions (`s3:ListBucket`, `s3:GetObject`, `s3:DeleteObject`, `s3:PutObject`) |
| Docker Build Fails            | Check `entrypoint.sh` and ensure directory structure is correct                                                          |
| Workflow Errors               | Verify `GITHUB_TOKEN` and GHCR setup                                                                                     |

---

## License

todo