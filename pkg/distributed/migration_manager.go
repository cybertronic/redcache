package distributed

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ShardMigrationTask defines the parameters for moving a virtual shard.
type ShardMigrationTask struct {
	ShardID    ShardID
	SourceNode string
	TargetNode string
	StartTime  time.Time
}

// MigrationManager handles the coordinated movement of Shard state between nodes.
type MigrationManager struct {
	mu            sync.RWMutex
	activeTasks   map[ShardID]*ShardMigrationTask
	router        *Router
	
	dataTransferFn func(ctx context.Context, sid ShardID, target string) error
}

// NewMigrationManager creates a new manager linked to the cluster router.
func NewMigrationManager(router *Router) *MigrationManager {
	return &MigrationManager{
		activeTasks: make(map[ShardID]*ShardMigrationTask),
		router:      router,
	}
}

// PrepareMigration marks a shard as "Moving" in the routing table.
func (mm *MigrationManager) PrepareMigration(sid ShardID, source, target string) (*ShardMigrationTask, error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if _, exists := mm.activeTasks[sid]; exists {
		return nil, fmt.Errorf("migration already in progress for shard %d", sid)
	}

	task := &ShardMigrationTask{
		ShardID:    sid,
		SourceNode: source,
		TargetNode: target,
		StartTime:  time.Now(),
	}
	mm.activeTasks[sid] = task

	return task, nil
}

// ExecuteMigration performs the data handoff.
func (mm *MigrationManager) ExecuteMigration(ctx context.Context, task *ShardMigrationTask) error {
	if mm.dataTransferFn != nil {
		if err := mm.dataTransferFn(ctx, task.ShardID, task.TargetNode); err != nil {
			mm.AbortMigration(task.ShardID)
			return fmt.Errorf("migration data transfer failed: %w", err)
		}
	}

	mm.mu.Lock()
	delete(mm.activeTasks, task.ShardID)
	mm.mu.Unlock()

	return nil
}

// AbortMigration cleans up state if a move fails.
func (mm *MigrationManager) AbortMigration(sid ShardID) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	delete(mm.activeTasks, sid)
}

// SetDataTransferProvider links the manager to the storage engine's streaming capabilities.
func (mm *MigrationManager) SetDataTransferProvider(fn func(ctx context.Context, sid ShardID, target string) error) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.dataTransferFn = fn
}
