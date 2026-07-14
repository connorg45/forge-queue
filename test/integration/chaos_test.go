package integration

import (
	"os"
	"os/exec"
	"testing"
)

func TestChaosScript(t *testing.T) {
	if os.Getenv("FORGE_CHAOS") != "1" {
		t.Skip("set FORGE_CHAOS=1 to run the 60s kill -9 chaos test")
	}
	cmd := exec.Command("scripts/chaos.sh")
	cmd.Dir = "../.."
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "TOTAL=10000", "KILL_SECONDS=60")
	if err := cmd.Run(); err != nil {
		t.Fatalf("chaos script failed: %v", err)
	}
}
