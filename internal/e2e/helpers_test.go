package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const e2eSecret = "whsec_e2e_suite"

var (
	buildOnce sync.Once
	buildPath string
	buildErr  error
)

// buildBinary compiles the driftless binary with crashpoints enabled, once
// per test run.
func buildBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping subprocess test in short mode")
	}
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "driftless-e2e-*")
		if err != nil {
			buildErr = err
			return
		}
		buildPath = filepath.Join(dir, "driftless")
		cmd := exec.Command("go", "build", "-tags", "crashpoint", "-o", buildPath, "../../cmd/driftless")
		out, err := cmd.CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return buildPath
}

// freeAddr reserves an ephemeral localhost address and releases it for the
// subprocess to bind.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// serveProc is one running serve subprocess.
type serveProc struct {
	cmd        *exec.Cmd
	IngestURL  string
	MetricsURL string
	output     *strings.Builder
}

// startServe launches the binary against the given database, optionally
// with a crashpoint armed, and waits until it serves /healthz.
func startServe(t *testing.T, binary, connString, apiBaseURL, crashpointName string, extraEnv ...string) *serveProc {
	t.Helper()
	ingestAddr := freeAddr(t)
	metricsAddr := freeAddr(t)

	p := &serveProc{output: &strings.Builder{}}
	p.cmd = exec.Command(binary, "serve")
	p.cmd.Env = append(os.Environ(),
		"DRIFTLESS_DATABASE_URL="+connString,
		"DRIFTLESS_STRIPE_API_KEY=rk_test_chaos",
		"DRIFTLESS_STRIPE_WEBHOOK_SECRET="+e2eSecret,
		"DRIFTLESS_SERVER_LISTEN="+ingestAddr,
		"DRIFTLESS_SERVER_METRICS_LISTEN="+metricsAddr,
		"DRIFTLESS_STRIPE_API_BASE_URL="+apiBaseURL,
		"DRIFTLESS_LOG_LEVEL=warn",
		"DRIFTLESS_LOG_FORMAT=text",
	)
	if crashpointName != "" {
		p.cmd.Env = append(p.cmd.Env, "DRIFTLESS_CRASHPOINT="+crashpointName)
	}
	p.cmd.Env = append(p.cmd.Env, extraEnv...)
	p.cmd.Stdout = p.output
	p.cmd.Stderr = p.output
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			_, _ = p.cmd.Process.Wait()
		}
		if t.Failed() {
			t.Logf("serve output:\n%s", p.output.String())
		}
	})

	p.IngestURL = "http://" + ingestAddr
	p.MetricsURL = "http://" + metricsAddr
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(p.IngestURL + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("serve did not become healthy; output:\n%s", p.output.String())
	return nil
}

// Kill9 sends SIGKILL, the way an OOM kill or power loss would.
func (p *serveProc) Kill9(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_, _ = p.cmd.Process.Wait()
}

// WaitExit blocks until the process exits on its own, as it does when a
// crashpoint fires.
func (p *serveProc) WaitExit(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = p.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("serve did not exit; output:\n%s", p.output.String())
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForDrain waits until no live jobs remain, meaning the workers have
// consumed everything that was enqueued.
func waitForDrain(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	waitFor(t, 30*time.Second, "job queue to drain", func() bool {
		var live int
		err := pool.QueryRow(context.Background(),
			`SELECT count(*) FROM driftless.jobs WHERE status IN ('pending','running')`).Scan(&live)
		return err == nil && live == 0
	})
}
