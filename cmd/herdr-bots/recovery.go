package main

import (
	"context"
	"flag"
	"fmt"
	"time"
)

// recoverCmd implements `herdr-bots recover RUN [--config PATH] [--state PATH]`.
// It is an explicit, owner-directed retirement of exactly one stuck
// provisioning run: the engine verifies workspace absence for the run's saved
// planned branch and only then retires the run as terminal interrupted. It is
// deliberately not daemon reconciliation. Argument arity is validated before
// any writable store is opened, and positionalFirst keeps the documented
// RUN-before-flags ordering working.
func recoverCmd(args []string) error {
	fs := flag.NewFlagSet("recover", flag.ContinueOnError)
	configPath, statePath := common(fs)
	if err := fs.Parse(positionalFirst(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("recover requires one run id")
	}
	eng, state, err := openEngine(*configPath, *statePath)
	if err != nil {
		return err
	}
	defer state.Close()
	run, err := eng.RecoverProvisioning(context.Background(), fs.Arg(0), time.Now())
	if err != nil {
		return err
	}
	fmt.Printf("recovered %s: %s\n", run.ID, run.State)
	return nil
}
