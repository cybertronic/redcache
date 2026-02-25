package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabase(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create database
	config := TestDatabaseConfig(dbPath)
	config.GCInterval = 1 * time.Second
	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	t.Run("BasicOperations", func(t *testing.T) {
		// Set
		err := db.Set([]byte("key1"), []byte("value1"))
		assert.NoError(t, err)

		// Get
		value, err := db.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), value)

		// Exists
		exists, err := db.Exists([]byte("key1"))
		assert.NoError(t, err)
		assert.True(t, exists)

		// Delete
		err = db.Delete([]byte("key1"))
		assert.NoError(t, err)

		// Get after delete
		value, err = db.Get([]byte("key1"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("BatchOperations", func(t *testing.T) {
		items := map[string][]byte{
			"batch1": []byte("value1"),
			"batch2": []byte("value2"),
			"batch3": []byte("value3"),
		}

		// Set batch
		err := db.SetBatch(items)
		assert.NoError(t, err)

		// Get batch
		keys := [][]byte{
			[]byte("batch1"),
			[]byte("batch2"),
			[]byte("batch3"),
		}
		results, err := db.GetBatch(keys)
		assert.NoError(t, err)
		assert.Len(t, results, 3)
		assert.Equal(t, []byte("value1"), results["batch1"])
	})

	t.Run("Scan", func(t *testing.T) {
		// Set keys with prefix
		db.Set([]byte("scan/key1"), []byte("value1"))
		db.Set([]byte("scan/key2"), []byte("value2"))
		db.Set([]byte("scan/key3"), []byte("value3"))
		db.Set([]byte("other/key"), []byte("other"))

		// Scan with prefix
		count := 0
		err := db.Scan([]byte("scan/"), func(key, value []byte) error {
			count++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("Count", func(t *testing.T) {
		count, err := db.Count([]byte("scan/"))
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("DeletePrefix", func(t *testing.T) {
		err := db.DeletePrefix([]byte("scan/"))
		assert.NoError(t, err)

		count, err := db.Count([]byte("scan/"))
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("TTL", func(t *testing.T) {
		// Set with TTL
		err := db.SetWithTTL([]byte("ttl_key"), []byte("ttl_value"), 1*time.Second)
		assert.NoError(t, err)

		// Get immediately
		value, err := db.Get([]byte("ttl_key"))
		assert.NoError(t, err)
		assert.Equal(t, []byte("ttl_value"), value)

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Get after expiration
		value, err = db.Get([]byte("ttl_key"))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("JSON", func(t *testing.T) {
		type TestStruct struct {
			Name  string
			Value int
		}

		data := TestStruct{Name: "test", Value: 42}

		// Set JSON
		err := db.SetJSON([]byte("json_key"), data)
		assert.NoError(t, err)

		// Get JSON
		var result TestStruct
		err = db.GetJSON([]byte("json_key"), &result)
		assert.NoError(t, err)
		assert.Equal(t, data, result)
	})
}

func TestNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)
	defer db.Close()

	// Create namespaces
	ns1 := db.NewNamespace("namespace1")
	ns2 := db.NewNamespace("namespace2")

	t.Run("IsolatedNamespaces", func(t *testing.T) {
		// Set in namespace1
		err := ns1.Set("key1", []byte("value1"))
		assert.NoError(t, err)

		// Set in namespace2
		err = ns2.Set("key1", []byte("value2"))
		assert.NoError(t, err)

		// Get from namespace1
		value, err := ns1.Get("key1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("value1"), value)

		// Get from namespace2
		value, err = ns2.Get("key1")
		assert.NoError(t, err)
		assert.Equal(t, []byte("value2"), value)
	})

	t.Run("NamespaceScan", func(t *testing.T) {
		// Set multiple keys in namespace
		ns1.Set("scan1", []byte("value1"))
		ns1.Set("scan2", []byte("value2"))
		ns1.Set("scan3", []byte("value3"))

		// Scan namespace
		count := 0
		err := ns1.Scan(func(key, value []byte) error {
			count++
			return nil
		})
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, count, 3)
	})

	t.Run("NamespaceCount", func(t *testing.T) {
		count, err := ns1.Count()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, count, 3)
	})

	t.Run("NamespaceClear", func(t *testing.T) {
		err := ns1.Clear()
		assert.NoError(t, err)

		count, err := ns1.Count()
		assert.NoError(t, err)
		assert.Equal(t, 0, count)

		// Verify namespace2 is unaffected
		count, err = ns2.Count()
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, count, 1)
	})
}

func TestBackupRestore(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	backupPath := filepath.Join(tmpDir, "backup.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(t, err)

	// Set some data
	db.Set([]byte("key1"), []byte("value1"))
	db.Set([]byte("key2"), []byte("value2"))
	db.Set([]byte("key3"), []byte("value3"))

	// Backup
	err = db.Backup(backupPath)
	assert.NoError(t, err)

	// Verify backup file exists
	_, err = os.Stat(backupPath)
	assert.NoError(t, err)

	// Close database
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

	// Verify data
	value, err := db2.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)
}

func BenchmarkDatabase(b *testing.B) {
	tmpDir := b.TempDir()
	dbPath := filepath.Join(tmpDir, "bench.db")

	config := TestDatabaseConfig(dbPath)
	db, err := NewDatabase(config)
	require.NoError(b, err)
	defer db.Close()

	b.Run("Set", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte("key" + string(rune(i)))
			value := []byte("value" + string(rune(i)))
			db.Set(key, value)
		}
	})

	b.Run("Get", func(b *testing.B) {
		// Prepare data
		for i := 0; i < 1000; i++ {
			key := []byte("key" + string(rune(i)))
			value := []byte("value" + string(rune(i)))
			db.Set(key, value)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte("key" + string(rune(i%1000)))
			db.Get(key)
		}
	})

	b.Run("SetBatch", func(b *testing.B) {
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
	})
}