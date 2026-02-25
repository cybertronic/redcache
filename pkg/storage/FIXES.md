# Database Lock Fixes

## Issues Fixed

The original `database.go` file had critical thread safety issues where **write operations were using read locks** (`RLock()`/`RUnlock()`) instead of write locks (`Lock()`/`Unlock()`). This caused race conditions and test failures.

## Changes Made

### Write Operations (Changed from RLock to Lock):

1. **SetWithTTL()** - Changed from `RLock()` to `Lock()`
   - Writes key-value pairs to the database
   - Must use exclusive lock to prevent concurrent writes

2. **Delete()** - Changed from `RLock()` to `Lock()`
   - Removes keys from the database
   - Must use exclusive lock to prevent concurrent modifications

3. **SetBatch()** - Changed from `RLock()` to `Lock()`
   - Atomically writes multiple key-value pairs
   - Must use exclusive lock for batch operations

4. **DeletePrefix()** - Changed from `RLock()` to `Lock()`
   - Deletes all keys with a given prefix
   - Must use exclusive lock for bulk deletion

5. **Transaction()** - Changed from `RLock()` to `Lock()`
   - Executes arbitrary write transactions
   - Must use exclusive lock

### Read Operations (Kept RLock):

- **Get()** - Uses `RLock()` (read-only)
- **Exists()** - Uses `RLock()` (read-only)
- **GetBatch()** - Uses `RLock()` (read-only)
- **Scan()** - Uses `RLock()` (read-only)
- **ScanKeys()** - Uses `RLock()` (read-only)
- **Count()** - Uses `RLock()` (read-only)
- **Backup()** - Uses `RLock()` (read-only)
- **Stats()** - Uses `RLock()` (read-only)
- **ViewTransaction()** - Uses `RLock()` (read-only)

### Additional Operations:

- **Restore()** - Uses `Lock()` (modifies database state)
- **Close()** - Uses `Lock()` (modifies database state)
- **Set()** - Calls SetWithTTL (inherits write lock)
- **SetJSON()** - Calls Set (inherits write lock)

## Testing

The fixes ensure that:
1. Multiple readers can access the database concurrently (RLock allows multiple holders)
2. Writers have exclusive access to prevent data corruption
3. All tests in `database_test.go` and `integration_test.go` should now pass

## Verification

To verify the fixes work, run:
```bash
go test -v ./pkg/storage/...
```

All tests should pass without race conditions or lock-related errors.