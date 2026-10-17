// Package stage provides internal stage file management for Snowflake emulator.
package stage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nnnkkk7/snowflake-emulator/pkg/metadata"
)

// StageFile represents a file in a stage.
type StageFile struct {
	Name         string
	Size         int64
	ModifiedTime time.Time
}

// Manager manages stage file operations.
type Manager struct {
	repo     *metadata.Repository
	stageDir string // Base directory for internal stages
}

// NewManager creates a new stage manager.
func NewManager(repo *metadata.Repository, stageDir string) *Manager {
	if stageDir == "" {
		stageDir = "./stages"
	}
	return &Manager{
		repo:     repo,
		stageDir: stageDir,
	}
}

// CreateStage creates a new stage in the specified schema.
func (m *Manager) CreateStage(ctx context.Context, schemaID, name, stageType, url, comment string) (*metadata.Stage, error) {
	stage, err := m.repo.CreateStage(ctx, schemaID, name, stageType, url, comment)
	if err != nil {
		return nil, err
	}

	// Create directory for internal stages
	if stageType == "" || strings.ToUpper(stageType) == "INTERNAL" {
		stageDir := m.getStageDir(schemaID, stage.Name)
		if err := os.MkdirAll(stageDir, 0o755); err != nil {
			// Rollback stage creation
			_ = m.repo.DropStage(ctx, stage.ID)
			return nil, fmt.Errorf("failed to create stage directory: %w", err)
		}
	}

	return stage, nil
}

// GetStage retrieves a stage by schema ID and name.
func (m *Manager) GetStage(ctx context.Context, schemaID, name string) (*metadata.Stage, error) {
	return m.repo.GetStageByName(ctx, schemaID, name)
}

// ListStages returns all stages in a schema.
func (m *Manager) ListStages(ctx context.Context, schemaID string) ([]*metadata.Stage, error) {
	return m.repo.ListStages(ctx, schemaID)
}

// DropStage removes a stage and its files.
func (m *Manager) DropStage(ctx context.Context, schemaID, name string) error {
	stage, err := m.repo.GetStageByName(ctx, schemaID, name)
	if err != nil {
		return err
	}

	// Delete stage directory for internal stages
	if stage.StageType == "" || strings.ToUpper(stage.StageType) == "INTERNAL" {
		stageDir := m.getStageDir(schemaID, stage.Name)
		if err := os.RemoveAll(stageDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove stage directory: %w", err)
		}
	}

	return m.repo.DropStage(ctx, stage.ID)
}

// PutFile uploads a file to a stage.
func (m *Manager) PutFile(ctx context.Context, schemaID, stageName, fileName string, data io.Reader) error {
	stage, err := m.repo.GetStageByName(ctx, schemaID, stageName)
	if err != nil {
		return err
	}

	if strings.ToUpper(stage.StageType) != "INTERNAL" && stage.StageType != "" {
		return fmt.Errorf("PUT operation only supported for internal stages")
	}

	stageDir := m.getStageDir(schemaID, stage.Name)
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return fmt.Errorf("failed to create stage directory: %w", err)
	}

	// Sanitize file name to prevent directory traversal
	cleanName := filepath.Clean(fileName)
	if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
		return fmt.Errorf("invalid file name: %s", fileName)
	}

	filePath := filepath.Join(stageDir, cleanName)

	// Ensure parent directory exists for nested paths
	if dir := filepath.Dir(filePath); dir != stageDir {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// GetFile retrieves a file from a stage.
func (m *Manager) GetFile(ctx context.Context, schemaID, stageName, fileName string) (io.ReadCloser, error) {
	stage, err := m.repo.GetStageByName(ctx, schemaID, stageName)
	if err != nil {
		return nil, err
	}

	if strings.ToUpper(stage.StageType) != "INTERNAL" && stage.StageType != "" {
		return nil, fmt.Errorf("GET operation only supported for internal stages")
	}

	stageDir := m.getStageDir(schemaID, stage.Name)

	// Sanitize file name to prevent directory traversal
	cleanName := filepath.Clean(fileName)
	if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
		return nil, fmt.Errorf("invalid file name: %s", fileName)
	}

	filePath := filepath.Join(stageDir, cleanName)

	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file %s not found in stage %s", fileName, stageName)
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return file, nil
}

// ListFiles lists files in a stage, optionally filtered by pattern.
func (m *Manager) ListFiles(ctx context.Context, schemaID, stageName, pattern string) ([]StageFile, error) {
	stage, err := m.repo.GetStageByName(ctx, schemaID, stageName)
	if err != nil {
		return nil, err
	}

	if strings.ToUpper(stage.StageType) != "INTERNAL" && stage.StageType != "" {
		return nil, fmt.Errorf("LIST operation only supported for internal stages")
	}

	stageDir := m.getStageDir(schemaID, stage.Name)

	var files []StageFile
	err = filepath.Walk(stageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // Stage directory doesn't exist yet
			}
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(stageDir, path)
		if err != nil {
			return err
		}

		// Apply pattern filter if specified
		if pattern != "" {
			matched, err := filepath.Match(pattern, filepath.Base(relPath))
			if err != nil {
				return err
			}
			if !matched {
				return nil
			}
		}

		files = append(files, StageFile{
			Name:         relPath,
			Size:         info.Size(),
			ModifiedTime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	return files, nil
}

// RemoveFile removes a file from a stage.
func (m *Manager) RemoveFile(ctx context.Context, schemaID, stageName, fileName string) error {
	stage, err := m.repo.GetStageByName(ctx, schemaID, stageName)
	if err != nil {
		return err
	}

	if strings.ToUpper(stage.StageType) != "INTERNAL" && stage.StageType != "" {
		return fmt.Errorf("REMOVE operation only supported for internal stages")
	}

	stageDir := m.getStageDir(schemaID, stage.Name)

	// Sanitize file name to prevent directory traversal
	cleanName := filepath.Clean(fileName)
	if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
		return fmt.Errorf("invalid file name: %s", fileName)
	}

	filePath := filepath.Join(stageDir, cleanName)

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file %s not found in stage %s", fileName, stageName)
		}
		return fmt.Errorf("failed to remove file: %w", err)
	}

	return nil
}

// getStageDir returns the directory path for a stage.
func (m *Manager) getStageDir(schemaID, stageName string) string {
	return filepath.Join(m.stageDir, schemaID, stageName)
}

// GetStageDirectory returns the full path to a stage directory.
func (m *Manager) GetStageDirectory(ctx context.Context, schemaID, stageName string) (string, error) {
	stage, err := m.repo.GetStageByName(ctx, schemaID, stageName)
	if err != nil {
		return "", err
	}

	if strings.ToUpper(stage.StageType) != "INTERNAL" && stage.StageType != "" {
		return "", fmt.Errorf("directory access only supported for internal stages")
	}

	return m.getStageDir(schemaID, stage.Name), nil
}
