package warehouse

import (
	"context"
	"fmt"
	"testing"
)

func TestManager_CreateWarehouse(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	tests := []struct {
		name        string
		warehouseName string
		size        string
		comment     string
		wantErr     bool
	}{
		{
			name:        "Valid warehouse",
			warehouseName: "TEST_WH",
			size:        "X-SMALL",
			comment:     "Test warehouse",
			wantErr:     false,
		},
		{
			name:        "Default size",
			warehouseName: "DEFAULT_WH",
			size:        "",
			comment:     "",
			wantErr:     false,
		},
		{
			name:        "Large warehouse",
			warehouseName: "LARGE_WH",
			size:        "LARGE",
			comment:     "",
			wantErr:     false,
		},
		{
			name:        "Empty name",
			warehouseName: "",
			size:        "X-SMALL",
			comment:     "",
			wantErr:     true,
		},
		{
			name:        "Invalid size",
			warehouseName: "INVALID_SIZE_WH",
			size:        "GIGANTIC",
			comment:     "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wh, err := mgr.CreateWarehouse(ctx, tt.warehouseName, tt.size, tt.comment)

			if (err != nil) != tt.wantErr {
				t.Errorf("CreateWarehouse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if wh == nil {
					t.Fatal("CreateWarehouse() returned nil warehouse")
				}
				if wh.ID == "" {
					t.Error("Warehouse ID is empty")
				}
				if wh.State != StateSuspended {
					t.Errorf("Expected warehouse state to be SUSPENDED, got %s", wh.State)
				}
				if tt.size == "" && wh.Size != "X-SMALL" {
					t.Errorf("Expected default size X-SMALL, got %s", wh.Size)
				}
				if wh.AutoResume != true {
					t.Error("Expected AutoResume to be true")
				}
				if wh.AutoSuspend != 600 {
					t.Errorf("Expected AutoSuspend to be 600, got %d", wh.AutoSuspend)
				}
			}
		})
	}
}

func TestManager_CreateWarehouse_Duplicate(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	_, err := mgr.CreateWarehouse(ctx, "TEST_WH", "X-SMALL", "")
	if err != nil {
		t.Fatalf("First CreateWarehouse() error = %v", err)
	}

	_, err = mgr.CreateWarehouse(ctx, "TEST_WH", "X-SMALL", "")
	if err == nil {
		t.Error("Expected error for duplicate warehouse, got nil")
	}
}

func TestManager_GetWarehouse(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	created, err := mgr.CreateWarehouse(ctx, "TEST_WH", "MEDIUM", "Test")
	if err != nil {
		t.Fatalf("CreateWarehouse() error = %v", err)
	}

	retrieved, err := mgr.GetWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("GetWarehouse() error = %v", err)
	}

	if retrieved.ID != created.ID {
		t.Errorf("Expected ID %s, got %s", created.ID, retrieved.ID)
	}
	if retrieved.Name != created.Name {
		t.Errorf("Expected name %s, got %s", created.Name, retrieved.Name)
	}
	if retrieved.Size != "MEDIUM" {
		t.Errorf("Expected size MEDIUM, got %s", retrieved.Size)
	}
}

func TestManager_GetWarehouse_NotFound(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	_, err := mgr.GetWarehouse(ctx, "NONEXISTENT")
	if err == nil {
		t.Error("Expected error for non-existent warehouse, got nil")
	}
}

func TestManager_ResumeWarehouse(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	_, err := mgr.CreateWarehouse(ctx, "TEST_WH", "X-SMALL", "")
	if err != nil {
		t.Fatalf("CreateWarehouse() error = %v", err)
	}

	// Resume warehouse
	err = mgr.ResumeWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("ResumeWarehouse() error = %v", err)
	}

	// Check state
	wh, err := mgr.GetWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("GetWarehouse() error = %v", err)
	}

	if wh.State != StateActive {
		t.Errorf("Expected state ACTIVE, got %s", wh.State)
	}

	// Resuming again should be idempotent
	err = mgr.ResumeWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Errorf("Second ResumeWarehouse() error = %v", err)
	}
}

func TestManager_SuspendWarehouse(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	_, err := mgr.CreateWarehouse(ctx, "TEST_WH", "X-SMALL", "")
	if err != nil {
		t.Fatalf("CreateWarehouse() error = %v", err)
	}

	// Resume first
	err = mgr.ResumeWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("ResumeWarehouse() error = %v", err)
	}

	// Suspend warehouse
	err = mgr.SuspendWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("SuspendWarehouse() error = %v", err)
	}

	// Check state
	wh, err := mgr.GetWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("GetWarehouse() error = %v", err)
	}

	if wh.State != StateSuspended {
		t.Errorf("Expected state SUSPENDED, got %s", wh.State)
	}

	// Suspending again should be idempotent
	err = mgr.SuspendWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Errorf("Second SuspendWarehouse() error = %v", err)
	}
}

func TestManager_DropWarehouse(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	_, err := mgr.CreateWarehouse(ctx, "TEST_WH", "X-SMALL", "")
	if err != nil {
		t.Fatalf("CreateWarehouse() error = %v", err)
	}

	// Drop warehouse
	err = mgr.DropWarehouse(ctx, "TEST_WH")
	if err != nil {
		t.Fatalf("DropWarehouse() error = %v", err)
	}

	// Verify it's gone
	_, err = mgr.GetWarehouse(ctx, "TEST_WH")
	if err == nil {
		t.Error("Expected error after dropping warehouse, got nil")
	}
}

func TestManager_ListWarehouses(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	// Create multiple warehouses
	_, err := mgr.CreateWarehouse(ctx, "WH_A", "X-SMALL", "")
	if err != nil {
		t.Fatalf("CreateWarehouse(WH_A) error = %v", err)
	}

	_, err = mgr.CreateWarehouse(ctx, "WH_B", "MEDIUM", "")
	if err != nil {
		t.Fatalf("CreateWarehouse(WH_B) error = %v", err)
	}

	_, err = mgr.CreateWarehouse(ctx, "WH_C", "LARGE", "")
	if err != nil {
		t.Fatalf("CreateWarehouse(WH_C) error = %v", err)
	}

	// List all warehouses
	warehouses, err := mgr.ListWarehouses(ctx)
	if err != nil {
		t.Fatalf("ListWarehouses() error = %v", err)
	}

	if len(warehouses) != 3 {
		t.Errorf("Expected 3 warehouses, got %d", len(warehouses))
	}

	// Verify all warehouses are in the list
	names := make(map[string]bool)
	for _, wh := range warehouses {
		names[wh.Name] = true
	}

	expectedNames := []string{"WH_A", "WH_B", "WH_C"}
	for _, name := range expectedNames {
		if !names[name] {
			t.Errorf("Expected to find warehouse %s in list", name)
		}
	}
}

func TestManager_ConcurrentOperations(t *testing.T) {
	mgr := NewManager()
	ctx := context.Background()

	done := make(chan bool, 10)
	errors := make(chan error, 10)

	// Concurrently create warehouses
	for i := 0; i < 10; i++ {
		go func(id int) {
			name := fmt.Sprintf("WH_%d", id)
			_, err := mgr.CreateWarehouse(ctx, name, "X-SMALL", "")
			if err != nil {
				errors <- err
				return
			}
			done <- true
		}(i)
	}

	// Wait for all operations
	for i := 0; i < 10; i++ {
		select {
		case <-done:
			// Success
		case err := <-errors:
			t.Errorf("Concurrent CreateWarehouse() error: %v", err)
		}
	}

	// Verify all warehouses were created
	warehouses, err := mgr.ListWarehouses(ctx)
	if err != nil {
		t.Fatalf("ListWarehouses() error = %v", err)
	}

	if len(warehouses) != 10 {
		t.Errorf("Expected 10 warehouses, got %d", len(warehouses))
	}
}
