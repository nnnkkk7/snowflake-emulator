package warehouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// State represents the state of a warehouse.
type State string

const (
	StateSuspended State = "SUSPENDED"
	StateResuming  State = "RESUMING"
	StateActive    State = "ACTIVE"
	StateSuspending State = "SUSPENDING"
)

// Warehouse represents a Snowflake virtual warehouse (metadata only for Phase 1).
type Warehouse struct {
	ID        string
	Name      string
	State     State
	Size      string // X-Small, Small, Medium, Large, X-Large, etc.
	Comment   string
	CreatedAt time.Time
	Owner     string
	AutoResume bool
	AutoSuspend int // seconds
}

// Manager manages virtual warehouses (metadata only for Phase 1).
// In Phase 2, this will manage actual compute resources.
type Manager struct {
	mu         sync.RWMutex
	warehouses map[string]*Warehouse // keyed by name (uppercase)
}

// NewManager creates a new warehouse manager.
func NewManager() *Manager {
	return &Manager{
		warehouses: make(map[string]*Warehouse),
	}
}

// CreateWarehouse creates a new virtual warehouse.
func (m *Manager) CreateWarehouse(ctx context.Context, name, size, comment string) (*Warehouse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("warehouse name cannot be empty")
	}

	// Normalize name to uppercase (Snowflake convention)
	normalizedName := normalizeWarehouseName(name)

	// Check if warehouse already exists
	if _, exists := m.warehouses[normalizedName]; exists {
		return nil, fmt.Errorf("warehouse %s already exists", normalizedName)
	}

	// Validate size
	if size == "" {
		size = "X-SMALL" // Default size
	}
	if !isValidSize(size) {
		return nil, fmt.Errorf("invalid warehouse size: %s", size)
	}

	warehouse := &Warehouse{
		ID:          uuid.New().String(),
		Name:        normalizedName,
		State:       StateSuspended, // Default state
		Size:        size,
		Comment:     comment,
		CreatedAt:   time.Now(),
		Owner:       "", // Will be set from session in Phase 2
		AutoResume:  true,
		AutoSuspend: 600, // Default 10 minutes
	}

	m.warehouses[normalizedName] = warehouse

	return warehouse, nil
}

// GetWarehouse retrieves a warehouse by name.
func (m *Manager) GetWarehouse(ctx context.Context, name string) (*Warehouse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	normalizedName := normalizeWarehouseName(name)

	warehouse, exists := m.warehouses[normalizedName]
	if !exists {
		return nil, fmt.Errorf("warehouse %s not found", normalizedName)
	}

	// Return a copy to prevent external modification
	warehouseCopy := *warehouse
	return &warehouseCopy, nil
}

// ResumeWarehouse transitions a warehouse from SUSPENDED to ACTIVE state.
// In Phase 1, this is metadata-only. In Phase 2, this will allocate compute resources.
func (m *Manager) ResumeWarehouse(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedName := normalizeWarehouseName(name)

	warehouse, exists := m.warehouses[normalizedName]
	if !exists {
		return fmt.Errorf("warehouse %s not found", normalizedName)
	}

	if warehouse.State == StateActive {
		return nil // Already active
	}

	if warehouse.State != StateSuspended {
		return fmt.Errorf("warehouse %s is in %s state, cannot resume", normalizedName, warehouse.State)
	}

	// Phase 1: Just update state (no actual compute)
	warehouse.State = StateActive

	return nil
}

// SuspendWarehouse transitions a warehouse from ACTIVE to SUSPENDED state.
// In Phase 1, this is metadata-only. In Phase 2, this will deallocate compute resources.
func (m *Manager) SuspendWarehouse(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedName := normalizeWarehouseName(name)

	warehouse, exists := m.warehouses[normalizedName]
	if !exists {
		return fmt.Errorf("warehouse %s not found", normalizedName)
	}

	if warehouse.State == StateSuspended {
		return nil // Already suspended
	}

	if warehouse.State != StateActive {
		return fmt.Errorf("warehouse %s is in %s state, cannot suspend", normalizedName, warehouse.State)
	}

	// Phase 1: Just update state (no actual compute)
	warehouse.State = StateSuspended

	return nil
}

// DropWarehouse removes a warehouse.
func (m *Manager) DropWarehouse(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedName := normalizeWarehouseName(name)

	if _, exists := m.warehouses[normalizedName]; !exists {
		return fmt.Errorf("warehouse %s not found", normalizedName)
	}

	delete(m.warehouses, normalizedName)

	return nil
}

// ListWarehouses returns all warehouses.
func (m *Manager) ListWarehouses(ctx context.Context) ([]*Warehouse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	warehouses := make([]*Warehouse, 0, len(m.warehouses))
	for _, wh := range m.warehouses {
		warehouseCopy := *wh
		warehouses = append(warehouses, &warehouseCopy)
	}

	return warehouses, nil
}

// Helper functions

func normalizeWarehouseName(name string) string {
	// Snowflake normalizes unquoted identifiers to uppercase
	return strings.ToUpper(name)
}

func isValidSize(size string) bool {
	validSizes := map[string]bool{
		"X-SMALL":  true,
		"SMALL":    true,
		"MEDIUM":   true,
		"LARGE":    true,
		"X-LARGE":  true,
		"2X-LARGE": true,
		"3X-LARGE": true,
		"4X-LARGE": true,
		"5X-LARGE": true,
		"6X-LARGE": true,
	}
	return validSizes[size]
}
