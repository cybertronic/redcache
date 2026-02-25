package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabasePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create database
	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Write some data
	db.Set([]byte("key1"), []byte("value1"))
	db.Set([]byte("key2"), []byte("value2"))
	db.Set([]byte("key3"), []byte("value3"))

	// Close database
	db.Close()

	// Reopen database
	db2, err := NewDatabase(config)
	require.NoError(t, err)
	defer db2.Close()

	// Verify data persisted
	value, err := db2.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	value, err = db2.Get([]byte("key2"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), value)

	value, err = db2.Get([]byte("key3"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value3"), value)
}

func TestNamespacePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Create namespaces
	ns1 := db.NewNamespace("namespace1")
	ns2 := db.NewNamespace("namespace2")

	// Write data to namespaces
	ns1.Set("key1", []byte("value1"))
	ns2.Set("key1", []byte("value2"))

	// Close database
	db.Close()

	// Reopen database
	db2, err := NewDatabase(config)
	require.NoError(t, err)
	defer db2.Close()

	// Recreate namespaces
	ns1_2 := db2.NewNamespace("namespace1")
	ns2_2 := db2.NewNamespace("namespace2")

	// Verify data persisted in correct namespaces
	value, err := ns1_2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	value, err = ns2_2.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), value)
}

func TestBackupAndRestore(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	backupPath := filepath.Join(tmpDir, "backup.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Write data
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		db.Set(key, value)
	}

	// Create backup
	err = db.Backup(backupPath)
	assert.NoError(t, err)

	// Verify backup file exists
	_, err = os.Stat(backupPath)
	assert.NoError(t, err)

	// Close original database
	db.Close()

	// Create new database
	dbPath2 := filepath.Join(tmpDir, "test2.db")
	config2 := TestDatabaseConfig(dbPath2)
	db2, err := NewDatabase(config2)
	require.NoError(t, err)
	defer db2.Close()

	// Restore from backup
	err = db2.Restore(backupPath)
	assert.NoError(t, err)

	// Verify data restored
	for i := 0; i < 100; i++ {
		key := []byte("key" + string(rune(i)))
		expectedValue := []byte("value" + string(rune(i)))
		
		value, err := db2.Get(key)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte("key_" + string(rune(id)) + "_" + string(rune(j)))
				value := []byte("value_" + string(rune(id)) + "_" + string(rune(j)))
				db.Set(key, value)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all writes succeeded
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := []byte("key_" + string(rune(i)) + "_" + string(rune(j)))
			expectedValue := []byte("value_" + string(rune(i)) + "_" + string(rune(j)))
			
			value, err := db.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, expectedValue, value)
		}
	}
}

func TestTTLPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Set key with TTL
	db.SetWithTTL([]byte("ttl_key"), []byte("ttl_value"), 2*time.Second)

	// Verify key exists
	value, err := db.Get([]byte("ttl_key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("ttl_value"), value)

	// Close and reopen
	db.Close()

	db2, err := NewDatabase(config)
	require.NoError(t, err)
	defer db2.Close()

	// Key should still exist (within TTL)
	value, err = db2.Get([]byte("ttl_key"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("ttl_value"), value)

	// Wait for expiration
	time.Sleep(3 * time.Second)

	// Key should be expired
	value, err = db2.Get([]byte("ttl_key"))
	assert.NoError(t, err)
	assert.Nil(t, value)
}

func TestBatchOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Prepare batch
	items := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := "batch_key_" + string(rune(i))
		value := []byte("batch_value_" + string(rune(i)))
		items[key] = value
	}

	// Write batch
	err = db.SetBatch(items)
	assert.NoError(t, err)

	// Verify all items
	for key, expectedValue := range items {
		value, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

func TestPrefixOperations(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Write keys with different prefixes
	db.Set([]byte("user/1/name"), []byte("Alice"))
	db.Set([]byte("user/1/email"), []byte("alice@example.com"))
	db.Set([]byte("user/2/name"), []byte("Bob"))
	db.Set([]byte("user/2/email"), []byte("bob@example.com"))
	db.Set([]byte("product/1/name"), []byte("Widget"))

	// Count user keys
	count, err := db.Count([]byte("user/"))
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Scan user keys
	userKeys := make([]string, 0)
	err = db.ScanKeys([]byte("user/"), func(key []byte) error {
		userKeys = append(userKeys, string(key))
		return nil
	})
	assert.NoError(t, err)
	assert.Len(t, userKeys, 4)

	// Delete user prefix
	err = db.DeletePrefix([]byte("user/"))
	assert.NoError(t, err)

	// Verify user keys deleted
	count, err = db.Count([]byte("user/"))
	assert.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify product key still exists
	value, err := db.Get([]byte("product/1/name"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("Widget"), value)
}

func BenchmarkDatabaseWrite(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(b, err)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		db.Set(key, value)
	}
}

func BenchmarkDatabaseRead(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(b, err)
	defer db.Close()

	// Prepare data
	for i := 0; i < 10000; i++ {
		key := []byte("key" + string(rune(i)))
		value := []byte("value" + string(rune(i)))
		db.Set(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte("key" + string(rune(i%10000)))
		db.Get(key)
	}
}

func BenchmarkDatabaseBatch(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(b, err)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		items := make(map[string][]byte)
		for j := 0; j < 100; j++ {
			key := "batch_key_" + string(rune(i*100+j))
			value := []byte("value" + string(rune(j)))
			items[key] = value
		}
		db.SetBatch(items)
	}
}