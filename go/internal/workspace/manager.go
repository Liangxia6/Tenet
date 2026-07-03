package workspace

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/tenet/orchestrator/internal/config"
	"github.com/tenet/orchestrator/internal/guard"
)

type Manager struct {
	basePath        string
	snapshotDriver  string
	excludePatterns []string
}

type Snapshot struct {
	Type string `json:"type"`
	Ref  string `json:"ref"`
}

func NewManager(cfg *config.RuntimeConfig) *Manager {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Manager{
		basePath:        cfg.Workspace.BasePath,
		snapshotDriver:  cfg.Workspace.SnapshotDriver,
		excludePatterns: cfg.Workspace.ExcludePatterns,
	}
}

func (m *Manager) Init(sessionID string) (string, error) {
	if strings.TrimSpace(sessionID) == "" {
		return "", errors.New("session_id is required")
	}
	base, err := filepath.Abs(m.basePath)
	if err != nil {
		return "", err
	}
	root := filepath.Join(base, safeName(filepath.Clean(sessionID)))
	if err := ensureInside(base, root); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(root, "findings"), 0755); err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(root, ".backup"), 0755); err != nil {
		return "", err
	}
	return filepath.Abs(root)
}

func (m *Manager) ValidatePath(workspaceRoot, relativePath string, mustExist bool) (string, error) {
	if strings.TrimSpace(relativePath) == "" {
		return "", errors.New("path is required")
	}
	root, err := filepath.Abs(workspaceRoot)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	candidate := filepath.Join(root, filepath.Clean(relativePath))
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		candidate = resolved
	} else if mustExist {
		return "", err
	}
	if err := ensureInside(root, candidate); err != nil {
		return "", err
	}
	return candidate, nil
}

func (m *Manager) AnalyzeTextRatio(root string) (float64, error) {
	var total, text int
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == ".backup" || name == "node_modules" || name == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		total++
		if isTextFile(path) {
			text++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 1, nil
	}
	return float64(text) / float64(total), nil
}

func (m *Manager) Snapshot(ctx context.Context, root, sessionID string, seq int64, lock guard.LockManager, lease guard.FencingLease) (Snapshot, error) {
	if err := validateLease(ctx, lock, lease); err != nil {
		return Snapshot{}, err
	}
	driver := strings.ToLower(strings.TrimSpace(m.snapshotDriver))
	if driver == "" || driver == "auto" {
		ratio, err := m.AnalyzeTextRatio(root)
		if err != nil {
			return Snapshot{}, err
		}
		if ratio >= 0.9 {
			driver = "git"
		} else {
			driver = "archive"
		}
	}
	if driver == "git" {
		ref, err := m.GitCommit(root, fmt.Sprintf("tenet snapshot %s #%d", sessionID, seq))
		return Snapshot{Type: "git", Ref: ref}, err
	}
	ref, err := m.CreateArchive(root, sessionID, seq)
	return Snapshot{Type: "archive", Ref: ref}, err
}

func (m *Manager) CreateArchive(root, sessionID string, seq int64) (string, error) {
	backupDir := filepath.Join(root, ".backup")
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return "", err
	}
	archivePath := filepath.Join(backupDir, fmt.Sprintf("%s-%d.tar.gz", safeName(sessionID), seq))
	file, err := os.Create(archivePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	gzw := gzip.NewWriter(file)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == archivePath {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		if shouldExclude(rel, d, m.excludePatterns) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		source, err := os.Open(path)
		if err != nil {
			return err
		}
		defer source.Close()
		_, err = io.Copy(tw, source)
		return err
	})
	if err != nil {
		return "", err
	}
	return archivePath, nil
}

func (m *Manager) ExtractArchive(archivePath, destPath string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(destPath, filepath.Clean(header.Name))
		if err := ensureInside(destPath, target); err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(out, tr)
			closeErr := out.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
}

func (m *Manager) Backup(ctx context.Context, root, sessionID string, lock guard.LockManager, lease guard.FencingLease) (Snapshot, error) {
	return m.Snapshot(ctx, root, sessionID, 0, lock, lease)
}

func (m *Manager) Restore(ctx context.Context, snapshot Snapshot, destPath string, lock guard.LockManager, lease guard.FencingLease) error {
	if err := validateLease(ctx, lock, lease); err != nil {
		return err
	}
	if snapshot.Type == "git" {
		return m.GitCheckout(destPath, snapshot.Ref)
	}
	return m.ExtractArchive(snapshot.Ref, destPath)
}

func (m *Manager) Cleanup(ctx context.Context, root string, lock guard.LockManager, lease guard.FencingLease) error {
	if err := validateLease(ctx, lock, lease); err != nil {
		return err
	}
	return os.RemoveAll(root)
}

func (m *Manager) GitCommit(root, message string) (string, error) {
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		if err := runGit(root, "init"); err != nil {
			return "", err
		}
		if err := runGit(root, "config", "user.email", "tenet@example.local"); err != nil {
			return "", err
		}
		if err := runGit(root, "config", "user.name", "Tenet"); err != nil {
			return "", err
		}
	}
	if err := runGit(root, "add", "-A"); err != nil {
		return "", err
	}
	if err := runGit(root, "commit", "--allow-empty", "-m", message); err != nil {
		return "", err
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (m *Manager) GitCheckout(root, ref string) error {
	return runGit(root, "checkout", ref)
}

func (m *Manager) GitResetHard(root, ref string) error {
	return runGit(root, "reset", "--hard", ref)
}

func (m *Manager) GitDiff(root, baseRef string) (string, error) {
	out, err := exec.Command("git", "-C", root, "diff", baseRef).CombinedOutput()
	return string(out), err
}

func validateLease(ctx context.Context, lock guard.LockManager, lease guard.FencingLease) error {
	if lock == nil || !lease.FencingRequired {
		return nil
	}
	return lock.Validate(ctx, lease)
}

func ensureInside(root, candidate string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	candidateAbs, err := filepath.Abs(candidate)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, candidateAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("path escapes workspace: %s", candidate)
	}
	return nil
}

func runGit(root string, args ...string) error {
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func isTextFile(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".go", ".py", ".rs", ".js", ".ts", ".tsx", ".jsx", ".md", ".yaml", ".yml", ".json", ".toml", ".txt", ".sh", ".sql", ".css", ".html", ".xml":
		return true
	default:
		return false
	}
}

func shouldExclude(rel string, d os.DirEntry, patterns []string) bool {
	rel = filepath.ToSlash(rel)
	name := d.Name()
	if name == ".git" || name == ".backup" {
		return true
	}
	for _, pattern := range patterns {
		pattern = filepath.ToSlash(strings.TrimSpace(pattern))
		if pattern == "" {
			continue
		}
		matched, _ := filepath.Match(pattern, rel)
		if matched || strings.Contains(rel, strings.TrimSuffix(pattern, "/")) {
			return true
		}
	}
	return false
}

func safeName(value string) string {
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(value)
}
