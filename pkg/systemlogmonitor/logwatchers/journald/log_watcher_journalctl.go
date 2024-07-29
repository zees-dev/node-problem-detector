//go:build journalctl
// +build journalctl

/*
Copyright 2016 The Kubernetes Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package journald

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/node-problem-detector/pkg/systemlogmonitor/logwatchers/types"
	logtypes "k8s.io/node-problem-detector/pkg/systemlogmonitor/types"
	"k8s.io/node-problem-detector/pkg/util"
	"k8s.io/node-problem-detector/pkg/util/tomb"
)

const (
	// configSourceKey is the key of source configuration in the plugin configuration.
	configSourceKey   = "source"
	entryMessageField = "MESSAGE"
	entryTimeField    = "__REALTIME_TIMESTAMP"
)

type journaldWatcher struct {
	cmd       *exec.Cmd
	cfg       types.WatcherConfig
	startTime time.Time
	logCh     chan *logtypes.Log
	tomb      *tomb.Tomb
}

// NewJournaldWatcher is the create function of journald watcher.
func NewJournaldWatcher(cfg types.WatcherConfig) types.LogWatcher {
	klog.Info("Using journalctl binary to watch journal logs")
	uptime, err := util.GetUptimeDuration()
	if err != nil {
		klog.Fatalf("failed to get uptime: %v", err)
	}
	startTime, err := util.GetStartTime(time.Now(), uptime, cfg.Lookback, cfg.Delay)
	if err != nil {
		klog.Fatalf("failed to get start time: %v", err)
	}

	return &journaldWatcher{
		cfg:       cfg,
		startTime: startTime,
		tomb:      tomb.NewTomb(),
		// A capacity 1000 buffer should be enough
		logCh: make(chan *logtypes.Log, 1000),
	}
}

// Make sure NewJournaldWatcher is types.WatcherCreateFunc .
var _ types.WatcherCreateFunc = NewJournaldWatcher

// Watch starts the journal watcher.
func (j *journaldWatcher) Watch() (<-chan *logtypes.Log, error) {
	args := []string{
		"--identifier",
		j.cfg.PluginConfig[configSourceKey],
		"--since",
		j.startTime.Format(time.DateTime),
		"--output",
		"json",
	}
	if j.cfg.LogPath != "" {
		if _, err := os.Stat(j.cfg.LogPath); err != nil {
			return nil, fmt.Errorf("failed to stat the log path %q: %v", j.cfg.LogPath, err)
		}
		args = append(args, "--directory", j.cfg.LogPath)
	}
	j.cmd = exec.Command("journalctl", args...)

	klog.V(4).Infof("Running journalctl command: %v", j.cmd)

	stdout, err := j.cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %v", err)
	}

	stderr, err := j.cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stderr pipe: %v", err)
	}

	if err := j.cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start journalctl command: %v", err)
	}

	go j.watchLoop(stdout, stderr)
	return j.logCh, nil
}

// Stop stops the journald watcher.
func (j *journaldWatcher) Stop() {
	j.tomb.Stop()
	if j.cmd != nil && j.cmd.Process != nil {
		j.cmd.Process.Kill()
	}
}

func (j *journaldWatcher) watchLoop(stdout io.ReadCloser, stderr io.ReadCloser) {
	defer j.tomb.Done()

	// handle stderr to propagate errors
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			klog.Errorf("journalctl error: %s", scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			klog.Errorf("error reading stderr: %v", err)
		}
	}()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		select {
		case <-j.tomb.Stopping():
			klog.Infof("Stop watching journalctl")
			return
		default:
			line := scanner.Text()
			log, err := parseJournalctlOutput(line)
			if err != nil {
				klog.Errorf("failed to parse journalctl output: %v", err)
				continue
			}
			j.logCh <- log
		}
	}
	if err := scanner.Err(); err != nil {
		klog.Errorf("error reading journalctl output: %v", err)
	}

	// wait for the command to complete and check its exit status
	if err := j.cmd.Wait(); err != nil {
		klog.Errorf("journalctl command failed: %v", err)
		os.Exit(2)
	}
}

func parseJournalctlOutput(line string) (*logtypes.Log, error) {
	// parse the JSON output from journalctl
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal journalctl output: %v", err)
	}

	message, ok := entry[entryMessageField].(string)
	if !ok {
		return nil, fmt.Errorf("missing %s field in journalctl output", entryMessageField)
	}

	realtimeTimestamp, ok := entry[entryTimeField].(string)
	if !ok {
		return nil, fmt.Errorf("missing %s field in journalctl output", entryTimeField)
	}

	// convert the __REALTIME_TIMESTAMP from microseconds to time.Time
	timestampInt, err := strconv.ParseInt(realtimeTimestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %v", entryTimeField, err)
	}
	timestamp := time.Unix(0, timestampInt*int64(time.Microsecond))

	return &logtypes.Log{
		Timestamp: timestamp,
		Message:   strings.TrimSpace(message),
	}, nil
}
