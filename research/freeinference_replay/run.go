package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"
)

type repository struct {
	Path     string `json:"path"`
	Revision string `json:"revision"`
	Dirty    *bool  `json:"dirty,omitempty"`
}

func repositoryState(path string) repository {
	r := repository{Path: path, Revision: "unknown"}
	b, err := exec.Command("git", "-C", path, "rev-parse", "HEAD").Output()
	if err != nil {
		return r
	}
	r.Revision = strings.TrimSpace(string(b))
	if b, err = exec.Command("git", "-C", path, "status", "--porcelain").Output(); err == nil {
		dirty := len(b) != 0
		r.Dirty = &dirty
	}
	return r
}

type runRecord struct {
	FormatVersion    int                 `json:"format_version"`
	Status           string              `json:"status"`
	StartedAt        time.Time           `json:"started_at"`
	FinishedAt       *time.Time          `json:"finished_at,omitempty"`
	Error            string              `json:"error,omitempty"`
	ExitCode         *int                `json:"blis_exit_code,omitempty"`
	Command          []string            `json:"command"`
	WorkingDirectory string              `json:"working_directory"`
	Executable       artifact            `json:"executable"`
	Build            *debug.BuildInfo    `json:"build"`
	CorpusPath       string              `json:"corpus_path"`
	Corpus           artifact            `json:"corpus"`
	Catalog          repository          `json:"catalog"`
	Registry         repository          `json:"registry"`
	Artifacts        map[string]artifact `json:"artifacts"`
}

func writeRecord(path string, record runRecord) error {
	b, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func run(ctx context.Context, o options) (runErr error) {
	p, err := prepare(o)
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err := fingerprint(exe)
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	build, _ := debug.ReadBuildInfo()
	record := runRecord{
		FormatVersion: 1, Status: "running", StartedAt: time.Now().UTC(),
		Command: append([]string{exe}, p.args...), WorkingDirectory: cwd,
		Executable: executable, Build: build, CorpusPath: p.corpusPath, Corpus: p.corpus,
		Catalog: repositoryState(p.catalog), Registry: repositoryState(p.registry),
		Artifacts: make(map[string]artifact),
	}
	if err := os.MkdirAll(filepath.Dir(p.output), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(p.output, 0755); err != nil {
		return err
	}
	// Keep failed runs and logs for diagnosis. Only a successful child process and
	// a readable metrics file make the launcher successful.
	defer func() {
		finished := time.Now().UTC()
		record.FinishedAt = &finished
		record.Status = "succeeded"
		if runErr != nil {
			record.Status, record.Error = "failed", runErr.Error()
		}
		for _, name := range append(fileNames(p.files), "metrics.json", "stdout.log", "stderr.log") {
			if a, err := fingerprint(filepath.Join(p.output, name)); err == nil {
				record.Artifacts[name] = a
			}
		}
		runErr = errors.Join(runErr, writeRecord(filepath.Join(p.output, "run.json"), record))
	}()
	for name, contents := range p.files {
		path := filepath.Join(p.output, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(path, contents, 0644); err != nil {
			return err
		}
	}
	if err := writeRecord(filepath.Join(p.output, "run.json"), record); err != nil {
		return err
	}
	stdout, err := os.Create(filepath.Join(p.output, "stdout.log"))
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(p.output, "stderr.log"))
	if err != nil {
		return err
	}
	defer stderr.Close()
	child := exec.CommandContext(ctx, exe, p.args...)
	child.Stdout, child.Stderr = stdout, io.MultiWriter(stderr, os.Stderr)
	fmt.Fprintf(os.Stderr, "Running BLIS; artifacts: %s\n", p.output)
	err = child.Run()
	if child.ProcessState != nil {
		code := child.ProcessState.ExitCode()
		record.ExitCode = &code
	}
	if err != nil {
		return fmt.Errorf("BLIS failed (see %s): %w", filepath.Join(p.output, "stderr.log"), err)
	}
	metrics, err := os.ReadFile(filepath.Join(p.output, "metrics.json"))
	if err != nil {
		return err
	}
	var result struct {
		Agentic struct {
			SHA256   string `json:"corpus_sha256"`
			Verified bool   `json:"corpus_sha256_verified"`
		} `json:"agentic"`
	}
	if err := json.Unmarshal(metrics, &result); err != nil {
		return fmt.Errorf("BLIS metrics: %w", err)
	}
	if !result.Agentic.Verified || result.Agentic.SHA256 != p.corpus.SHA256 {
		return fmt.Errorf("BLIS metrics do not confirm the input corpus digest")
	}
	fmt.Fprintf(os.Stderr, "Completed BLIS replay; metrics: %s\n", filepath.Join(p.output, "metrics.json"))
	return nil
}

func fileNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	return names
}
