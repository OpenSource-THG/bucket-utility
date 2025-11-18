# Code Review: S3Cleaner.java

**File**: `app/src/main/java/com/procure/thg/cockroachdb/S3Cleaner.java`
**Date**: 2025-11-18
**Status**: Issues Identified - Ready for Fix Implementation

---

## Executive Summary

The S3Cleaner class implements core deletion logic for old S3 objects. While functionally correct for typical use cases, it contains:
- **2 Critical issues** that could cause crashes on large buckets or during error scenarios
- **4 Major issues** affecting reliability and maintainability
- **4 Medium issues** impacting monitoring and validation
- **5 Minor issues** related to code quality

---

## 🔴 CRITICAL ISSUES

### Issue 1: Uninitialized Variable with Null Reference Risk
**Location**: Line 55 (variable declaration), Line 153 (loop condition)
**Severity**: CRITICAL
**File Reference**: S3Cleaner.java:55, S3Cleaner.java:153

**Problem**:
```java
// Line 55 - declared but not initialized
ListObjectsV2Response listObjectsV2Response;

// ... Line 142 - break exits loop on error
} catch (Exception e) {
  LOGGER.log(WARNING, "Error listing objects in page {0}: {1}",
          new Object[]{pageCount, e.getMessage()});
  break; // Stop processing to avoid infinite loop on persistent errors
}

// Line 153 - uses uninitialized variable
} while (listObjectsV2Response != null && Boolean.TRUE.equals(listObjectsV2Response.isTruncated()));
```

If an exception occurs on the first iteration before `listObjectsV2Response` is assigned, the loop condition will throw `NullPointerException`. Additionally, after a `break` statement on line 142, the next iteration rebuilds the request but `listObjectsV2Response` may be stale.

**Impact**:
- Application crashes when first API call fails
- Partial pagination results in stale token usage

**Fix Strategy**:
1. Initialize `ListObjectsV2Response listObjectsV2Response = null;` on line 55
2. Wrap the loop continuation logic in a proper guard:
```java
if (listObjectsV2Response == null || !listObjectsV2Response.isTruncated()) {
  break;
}
// Only rebuild request if we have valid continuation token
if (listObjectsV2Response.nextContinuationToken() != null) {
  listObjectsV2Request = ListObjectsV2Request.builder()...build();
}
```

---

### Issue 2: Variable Reinitialization with Potentially Null ContinuationToken
**Location**: Lines 145-151
**Severity**: CRITICAL
**File Reference**: S3Cleaner.java:145-151

**Problem**:
```java
// Lines 145-151 - called after potential break
requestBuilder = ListObjectsV2Request.builder()
        .bucket(bucket)
        .continuationToken(listObjectsV2Response.nextContinuationToken());
if (folder != null) {
  requestBuilder.prefix(folder);
}
listObjectsV2Request = requestBuilder.build();
```

After a `break` on line 142, `listObjectsV2Response` reference may be:
1. Null (first iteration failure)
2. Stale with a null `nextContinuationToken()` if pagination is complete
3. Invalid if the exception occurred mid-response

**Impact**:
- NullPointerException on `listObjectsV2Response.nextContinuationToken()`
- Next iteration with null continuation token fails silently

**Fix Strategy**:
1. Guard the reinitialization block:
```java
if (listObjectsV2Response != null && listObjectsV2Response.isTruncated()) {
  String token = listObjectsV2Response.nextContinuationToken();
  if (token != null) {
    requestBuilder = ListObjectsV2Request.builder()
            .bucket(bucket)
            .continuationToken(token);
    if (folder != null) {
      requestBuilder.prefix(folder);
    }
    listObjectsV2Request = requestBuilder.build();
  } else {
    break;
  }
}
```

---

## 🟠 MAJOR ISSUES

### Issue 3: Incorrect Metadata API Usage
**Location**: Line 84
**Severity**: MAJOR
**File Reference**: S3Cleaner.java:84, S3Cleaner.java:77-83

**Problem**:
```java
// Lines 77-83 - fetch metadata
HeadObjectRequest headRequest = HeadObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();
var headResponse = s3Client.headObject(headRequest);

// Line 84 - INCORRECT - HeadObjectResponse metadata map doesn't contain "last-modified"
String createdDate = headResponse.metadata().get("last-modified");
if (createdDate != null) {
  // ... parse and use
}
```

**AWS SDK Behavior**:
- `HeadObjectResponse.metadata()` returns **only custom user-defined metadata**, not system properties
- `last-modified` is a system header, accessed via `headResponse.lastModified()` (returns `Instant`)
- The condition always evaluates to false, skipping the parsing logic and falling back to `lastModified()`

**Impact**:
- HeadObject call overhead is wasted (adds network latency)
- Primary code path (lines 85-107) never executes
- Falls back to `s3Object.lastModified()` which is already available from ListObjects response

**Fix Strategy**:
1. Remove the HeadObject call entirely (it's redundant)
2. Use `s3Object.lastModified()` directly:
```java
Instant lastModified = s3Object.lastModified();
LOGGER.log(FINE, "Processing key: {0}, lastModified: {1}", new Object[]{key, lastModified});

if (lastModified.isBefore(threshold)) {
  deleteObject(s3Client, bucket, key);
} else {
  LOGGER.log(FINE, "Skipping {0}: lastModified {1} is after threshold {2}",
          new Object[]{key, lastModified, threshold});
}
```

**Alternative** (if HeadObject is intentionally used for custom metadata):
```java
var headResponse = s3Client.headObject(headRequest);
Instant lastModified = headResponse.lastModified();
Map<String, String> customMetadata = headResponse.metadata();
```

---

### Issue 4: Code Duplication - Redundant Nested Error Handling
**Location**: Lines 81-132
**Severity**: MAJOR
**File Reference**: S3Cleaner.java:81-132

**Problem**:
The same fallback logic appears in three places:
1. **Path 1 (lines 86-107)**: Parse `createdDate`, catch `DateTimeParseException`, fallback to `lastModified`
2. **Path 2 (lines 108-119)**: No `createdDate`, use `lastModified` directly
3. **Path 3 (lines 120-132)**: Exception fetching metadata, fallback to `lastModified`

All three paths execute nearly identical logic for evaluating `lastModified`:
```java
if (lastModified.isBefore(threshold)) {
  deleteObject(s3Client, bucket, key);
} else {
  LOGGER.log(FINE, "Skipping {0}: LastModified {1} is after threshold {2}", ...);
}
```

**Impact**:
- 52 lines (81-132) could be 8-10 lines
- Hard to maintain - changes must be made in 3 places
- Difficult to test
- Introduces inconsistencies

**Fix Strategy**:
Extract into a helper method:
```java
private void processObject(S3Object s3Object, String bucket, Instant threshold) {
  final String key = s3Object.key();
  LOGGER.log(FINE, "Processing key: {0}", key);

  if (key.endsWith("/") || (folder != null && key.equals(folder))) {
    LOGGER.log(FINE, "Skipping key {0}: directory or folder prefix", key);
    return;
  }

  Instant lastModified = s3Object.lastModified();
  if (lastModified.isBefore(threshold)) {
    deleteObject(s3Client, bucket, key);
  } else {
    LOGGER.log(FINE, "Skipping {0}: LastModified {1} is after threshold {2}",
            new Object[]{key, lastModified, threshold});
  }
}

// In cleanOldObjects(), replace lines 67-132 with:
for (S3Object s3Object : listObjectsV2Response.contents()) {
  processObject(s3Object, bucket, threshold);
}
```

---

### Issue 5: Incomplete Error Logging and Silent Partial Failures
**Location**: Lines 139-142
**Severity**: MAJOR
**File Reference**: S3Cleaner.java:139-142

**Problem**:
```java
} catch (Exception e) {
  LOGGER.log(WARNING, "Error listing objects in page {0}: {1}",
          new Object[]{pageCount, e.getMessage()});
  break; // Stop processing to avoid infinite loop on persistent errors
}
```

When pagination fails mid-operation:
- No indication of how many objects were not processed
- No distinction between retriable errors (timeout) and permanent errors (permission denied)
- Calling code doesn't know if operation completed successfully or failed partially
- No way to resume from last token

**Impact**:
- Silent partial failures go undetected
- Monitoring systems can't distinguish between full and partial runs
- Audit trail is incomplete

**Fix Strategy**:
1. Return a summary object from `cleanOldObjects()`:
```java
public class CleanerResult {
  public final int pagesProcessed;
  public final int objectsDeleted;
  public final int objectsSkipped;
  public final int errorCount;
  public final boolean completedSuccessfully;
  public final String lastError;
}
```

2. Track metrics throughout the method
3. Log comprehensive final result:
```java
} catch (Exception e) {
  LOGGER.log(WARNING, "Error listing objects in page {0}: {1}",
          new Object[]{pageCount, e.getMessage()});
  result.errorCount++;
  result.lastError = e.getMessage();
  break;
}
// At end:
LOGGER.log(INFO, "Cleaning finished. Pages: {0}, Deleted: {1}, Skipped: {2}, Errors: {3}",
        new Object[]{pageCount, deletedCount, skippedCount, errorCount});
```

---

### Issue 6: Missing Metrics and Monitoring Data
**Location**: Entire method (lines 34-155)
**Severity**: MAJOR
**File Reference**: S3Cleaner.java:34-155

**Problem**:
The method only logs `pageCount` at the end (line 154). Missing metrics:
- Total objects processed
- Objects deleted (no counter)
- Objects skipped (no counter)
- Errors encountered per page/object
- Total API calls made
- Exceptions caught

**Impact**:
- Impossible to verify correct operation after the fact
- Cannot implement alerts for partial failures
- Hard to debug issues
- No SLA metrics

**Fix Strategy**:
Create and populate a result object throughout execution:
```java
// At start of cleanOldObjects()
int totalObjectsProcessed = 0;
int totalObjectsDeleted = 0;
int totalObjectsSkipped = 0;
int totalErrorsEncountered = 0;

// Increment in appropriate locations:
for (S3Object s3Object : listObjectsV2Response.contents()) {
  totalObjectsProcessed++;
  // ... process object
  // if deleted: totalObjectsDeleted++
  // if skipped: totalObjectsSkipped++
}

// Log with all metrics
LOGGER.log(INFO, "Cleaning finished. Pages: {0}, Objects Processed: {1}, Deleted: {2}, Skipped: {3}, Errors: {4}",
        new Object[]{pageCount, totalObjectsProcessed, totalObjectsDeleted, totalObjectsSkipped, totalErrorsEncountered});
```

---

## 🟡 MEDIUM ISSUES

### Issue 7: Lack of Input Validation for Folder Parameter
**Location**: Constructor (lines 26-32)
**Severity**: MEDIUM
**File Reference**: S3Cleaner.java:26-32

**Problem**:
```java
public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folder) {
  // ...
  this.folder = folder != null && !folder.isEmpty() ?
          folder.endsWith("/") ? folder : folder + "/"
          : null;
}
```

No validation for:
- Invalid S3 key characters (e.g., leading/trailing spaces, null bytes)
- Extremely long folder names (S3 limit: 1024 bytes per key)
- Reserved characters or patterns
- Case sensitivity issues (S3 is case-sensitive)

**Impact**:
- Invalid folder paths silently accepted, leading to confusing ListObjects results
- Potential security issues if folder is user-supplied

**Fix Strategy**:
```java
public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folder) {
  if (thresholdSeconds < 0) {
    throw new IllegalArgumentException("thresholdSeconds must be >= 0");
  }

  if (folder != null) {
    if (folder.isEmpty()) {
      throw new IllegalArgumentException("folder must not be empty");
    }
    if (folder.length() > 1024) {
      throw new IllegalArgumentException("folder exceeds S3 key length limit (1024 bytes)");
    }
    if (folder.contains("\0")) {
      throw new IllegalArgumentException("folder contains null bytes");
    }
    // Normalize: ensure trailing slash
    this.folder = folder.endsWith("/") ? folder : folder + "/";
  } else {
    this.folder = null;
  }

  this.s3Client = s3Client;
  this.thresholdSeconds = thresholdSeconds;
}
```

---

### Issue 8: Redundant Folder Equality Check
**Location**: Line 71
**Severity**: MEDIUM
**File Reference**: S3Cleaner.java:71

**Problem**:
```java
if (key.endsWith("/") || (folder != null && key.equals(folder))) {
  LOGGER.log(FINE, "Skipping key {0}: directory or folder prefix", key);
  continue;
}
```

The `key.equals(folder)` check is redundant because:
- Folder always ends with "/" (line 30)
- If key equals folder (e.g., both are "prefix/"), it would match `key.endsWith("/")`
- No realistic S3 object has "/" as final character except directory markers

**Impact**:
- Adds unnecessary condition check on every object
- Confuses readers (appears to serve a purpose but doesn't)
- Minor performance impact

**Fix Strategy**:
```java
if (key.endsWith("/")) {
  LOGGER.log(FINE, "Skipping key {0}: directory marker", key);
  continue;
}
```

---

### Issue 9: Misleading Variable Naming
**Location**: Line 84, Line 87
**Severity**: MEDIUM
**File Reference**: S3Cleaner.java:84, S3Cleaner.java:87

**Problem**:
```java
String createdDate = headResponse.metadata().get("last-modified");
// ...
Instant createdInstant = Instant.parse(createdDate);
```

Variable name `createdDate` implies object creation time, but it's actually `last-modified` (when object was last updated). Misleading for readers trying to understand deletion logic.

**Impact**:
- Code is harder to understand
- Potential bugs from developers misunderstanding the timestamp
- Audit logs may be misinterpreted

**Fix Strategy**:
```java
String lastModifiedStr = headResponse.metadata().get("last-modified");
// ...
Instant lastModified = Instant.parse(lastModifiedStr);
```

---

### Issue 10: Missing Dry-Run Mode
**Location**: Entire `deleteObject()` method (lines 157-168)
**Severity**: MEDIUM
**File Reference**: S3Cleaner.java:157-168

**Problem**:
The method always deletes objects. There's no way to:
- Test the deletion logic without actually deleting objects
- Preview which objects would be deleted
- Run safely in production for the first time

**Impact**:
- Risk of accidental bulk deletion
- No safe way to test threshold logic in production
- Testing requires separate isolated bucket

**Fix Strategy**:
1. Add dry-run flag to constructor:
```java
private final boolean dryRun;

public S3Cleaner(final S3Client s3Client, final long thresholdSeconds,
                 final String folder, final boolean dryRun) {
  // ... validation ...
  this.dryRun = dryRun;
}
```

2. Modify deleteObject():
```java
private void deleteObject(final S3Client s3Client, final String bucket, final String key) {
  try {
    if (dryRun) {
      LOGGER.log(INFO, "[DRY RUN] Would delete object: {0}", key);
    } else {
      DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build();
      s3Client.deleteObject(deleteObjectRequest);
      LOGGER.log(FINE, "Deleted object: {0}", key);
    }
  } catch (Exception e) {
    LOGGER.log(WARNING, "Failed to delete object {0}: {1}", new Object[]{key, e.getMessage()});
  }
}
```

3. Add to metrics tracking (Issue 6):
```java
if (dryRun) {
  LOGGER.log(INFO, "[DRY RUN] Would have deleted {0} objects", deletedCount);
}
```

---

## 🔵 MINOR ISSUES

### Issue 11: Inconsistent Log Levels
**Location**: Line 62
**Severity**: MINOR
**File Reference**: S3Cleaner.java:62

**Problem**:
```java
LOGGER.log(INFO, "Page {0}: Listed {1} objects", new Object[]{pageCount, objectCount});
```

For large buckets, this INFO log repeats thousands of times. Each page with 1000 objects produces one INFO log. This pollutes logs.

**Impact**:
- Log files become large
- Important INFO messages get buried
- Log aggregation systems may filter high-volume logs

**Fix Strategy**:
```java
LOGGER.log(FINE, "Page {0}: Listed {1} objects", new Object[]{pageCount, objectCount});

// Keep INFO for important milestones:
if (objectCount == 0) {
  LOGGER.log(INFO, "Page {0}: No objects found", pageCount);
}
```

---

### Issue 12: Magic Threshold Formula Not Documented
**Location**: Line 45
**Severity**: MINOR
**File Reference**: S3Cleaner.java:45

**Problem**:
```java
final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);
```

The logic `now - thresholdSeconds` means "delete objects older than N seconds." This is not documented anywhere in the class. A developer might assume the inverse.

**Impact**:
- Confusing to new readers
- Risk of off-by-one errors or logic misunderstandings
- Audit issues if threshold semantics are unclear

**Fix Strategy**:
Add class-level documentation:
```java
/**
 * Deletes S3 objects whose last modification time is OLDER than the threshold.
 *
 * Threshold calculation: threshold = now() - thresholdSeconds
 *
 * Example: If thresholdSeconds=86400 (1 day), objects last modified > 1 day ago are deleted.
 */
public class S3Cleaner {
```

And at line 45:
```java
// threshold = now() - thresholdSeconds
// Objects with lastModified < threshold will be deleted
final Instant threshold = Instant.now().minus(thresholdSeconds, ChronoUnit.SECONDS);
```

---

### Issue 13: Race Condition on Concurrent Bucket Writes
**Location**: Lines 77-82 (HeadObject call)
**Severity**: MINOR
**File Reference**: S3Cleaner.java:77-82

**Problem**:
Between listing objects (line 60) and fetching metadata (line 82), an object may be:
- Deleted by another process
- Modified in a way that causes metadata fetch to fail

The warning on line 121 doesn't distinguish:
- Object was deleted (transient, can retry)
- Permission denied (permanent, can't retry)
- Malformed metadata (data corruption, needs investigation)

**Impact**:
- Operators can't determine if the error is expected or problematic
- Retry logic can't be implemented (all failures treated the same)
- Audit trail lacks context

**Fix Strategy**:
```java
} catch (NoSuchKeyException e) {
  LOGGER.log(FINE, "Object {0} was deleted by another process, skipping", key);
  totalObjectsSkipped++;
} catch (AccessDeniedException e) {
  LOGGER.log(WARNING, "Access denied for object {0}: {1}",
          new Object[]{key, e.getMessage()});
  totalErrorsEncountered++;
} catch (Exception e) {
  LOGGER.log(WARNING, "Failed to fetch metadata for {0}: {1}",
          new Object[]{key, e.getMessage()});
  totalErrorsEncountered++;
}
```

---

### Issue 14: No Rate Limiting or Backoff
**Location**: Main loop (lines 57-153)
**Severity**: MINOR
**File Reference**: S3Cleaner.java:57-153

**Problem**:
The loop makes API calls as fast as possible with no rate limiting:
- ListObjectsV2 (1 per page)
- HeadObject (1 per object)
- DeleteObject (1 per deleted object)

For buckets with millions of objects, this can trigger S3 rate limiting (429 errors).

**Impact**:
- Operations fail on large buckets
- Increased AWS costs (more retries)
- Potential service disruption

**Fix Strategy**:
1. Add backoff on rate limit errors:
```java
} catch (SlowDownException e) {
  LOGGER.log(WARNING, "S3 rate limit hit on page {0}, backing off 5 seconds", pageCount);
  Thread.sleep(5000);
  // Retry this page
  pageCount--;
} catch (Exception e) {
  // ...
}
```

2. Consider adding configurable batch delay:
```java
private final long batchDelayMs;

public S3Cleaner(..., long batchDelayMs) {
  this.batchDelayMs = batchDelayMs;
}

// In loop:
if (pageCount > 1 && batchDelayMs > 0) {
  Thread.sleep(batchDelayMs);
}
```

---

### Issue 15: Missing JavaDoc
**Location**: Class declaration (line 18), methods (lines 34, 157)
**Severity**: MINOR
**File Reference**: S3Cleaner.java:18, 26, 34, 157

**Problem**:
No JavaDoc comments for:
- Class-level documentation
- Constructor parameters and behavior
- Method contracts
- Exceptions thrown
- Thread safety guarantees

**Impact**:
- IDE autocomplete doesn't show usage info
- Developers must read implementation to understand contract
- Risk of misuse

**Fix Strategy**:
```java
/**
 * Deletes S3 objects older than a specified threshold.
 *
 * Objects are evaluated based on their lastModified timestamp.
 * Deletion threshold = current time - thresholdSeconds.
 *
 * Objects with lastModified < threshold are deleted.
 *
 * @param s3Client AWS S3 client (should be closed by caller)
 * @param thresholdSeconds Age threshold in seconds (objects older than this are deleted)
 * @param folder Optional folder prefix to scope deletions (auto-adds trailing slash)
 */
public S3Cleaner(final S3Client s3Client, final long thresholdSeconds, final String folder) {
  // ...
}

/**
 * Executes the deletion operation.
 *
 * Processes all objects in the bucket (or folder), comparing lastModified
 * against the calculated threshold, and deleting those that are older.
 *
 * Handles pagination automatically. Logs detailed progress and errors.
 *
 * @throws Exception if bucket access fails or environment variables are missing
 */
public void cleanOldObjects() {
  // ...
}
```

---

## Summary Table

| # | Issue | Location | Severity | Lines | Status |
|---|-------|----------|----------|-------|--------|
| 1 | Uninitialized variable risk | Line 55, 153 | 🔴 CRITICAL | 55, 153 | Ready to fix |
| 2 | Stale continuation token | Lines 145-151 | 🔴 CRITICAL | 145-151 | Ready to fix |
| 3 | Incorrect metadata API usage | Line 84 | 🟠 MAJOR | 77-132 | Ready to fix |
| 4 | Code duplication / error handling | Lines 81-132 | 🟠 MAJOR | 81-132 | Ready to fix |
| 5 | Silent partial failures | Lines 139-142 | 🟠 MAJOR | 139-155 | Ready to fix |
| 6 | Missing metrics | Entire method | 🟠 MAJOR | 34-155 | Ready to fix |
| 7 | No folder validation | Lines 26-32 | 🟡 MEDIUM | 26-32 | Ready to fix |
| 8 | Redundant folder check | Line 71 | 🟡 MEDIUM | 71 | Ready to fix |
| 9 | Misleading variable naming | Lines 84, 87 | 🟡 MEDIUM | 84-87 | Ready to fix |
| 10 | No dry-run mode | Lines 157-168 | 🟡 MEDIUM | 26-168 | Ready to fix |
| 11 | Inconsistent log levels | Line 62 | 🔵 MINOR | 62 | Ready to fix |
| 12 | Undocumented threshold logic | Line 45 | 🔵 MINOR | 45 | Ready to fix |
| 13 | Race condition handling | Lines 77-82 | 🔵 MINOR | 120-132 | Ready to fix |
| 14 | No rate limiting | Lines 57-153 | 🔵 MINOR | 57-153 | Ready to fix |
| 15 | Missing JavaDoc | Lines 18, 26, 34, 157 | 🔵 MINOR | Throughout | Ready to fix |

---

## Implementation Priority

### Phase 1: Critical Fixes (Prevent Crashes)
- [ ] Issue 1: Initialize ListObjectsV2Response variable
- [ ] Issue 2: Fix continuation token null reference

### Phase 2: Major Fixes (Improve Reliability)
- [ ] Issue 3: Fix metadata API usage (remove HeadObject call)
- [ ] Issue 4: Consolidate error handling / extract processObject() helper
- [ ] Issue 5: Add comprehensive error logging
- [ ] Issue 6: Implement metrics tracking and result object

### Phase 3: Medium Fixes (Improve Safety)
- [ ] Issue 7: Add input validation to constructor
- [ ] Issue 8: Remove redundant folder check
- [ ] Issue 9: Fix variable naming
- [ ] Issue 10: Add dry-run mode

### Phase 4: Minor Fixes (Polish)
- [ ] Issue 11: Adjust log levels
- [ ] Issue 12: Add threshold logic documentation
- [ ] Issue 13: Improve exception handling clarity
- [ ] Issue 14: Consider rate limiting for large buckets
- [ ] Issue 15: Add JavaDoc comments

---

## Testing Strategy

After implementing fixes, add/enhance test coverage for:

```java
// AppTest.java or new S3CleanerTest.java

@ParameterizedTest
@MethodSource
void testInitializationWithVariousParameters(
  long thresholdSeconds, String folder, boolean dryRun) {
  // Tests Issue 7 validation
}

@Test
void testEmptyBucketHandling() {
  // Tests Issue 1 - uninitialized variable
}

@Test
void testPaginationWithErrors() {
  // Tests Issue 2 - stale continuation token
  // Tests Issue 5 - error logging
}

@Test
void testMetricsCollection() {
  // Tests Issue 6 - metrics tracking
}

@Test
void testDryRunMode() {
  // Tests Issue 10 - no actual deletion in dry-run
}
```

---

## Related Files to Review

- `App.java` - Initializes S3Cleaner, handles environment variables
- `S3Copier.java` - Similar pagination logic may have same issues
- `AppTest.java` - Update tests to cover new validation and metrics

---

## Notes for Implementation

1. **Backward Compatibility**: Adding metrics result object is a breaking change. Consider:
   - Creating a new `cleanOldObjectsWithMetrics()` method alongside existing one
   - Or updating App.java to handle the new return type

2. **Performance**: Removing HeadObject calls (Issue 3) will improve performance by ~50% since each object fetch becomes 1 API call instead of 2.

3. **Testing**: Mock S3Client responses for edge cases like empty buckets, single-page results, and error conditions.

4. **Deployment**: Changes should be tested with realistic bucket sizes (>10k objects) to verify pagination works correctly.