//go:build (cgo && journald) || journalctl
// +build cgo,journald journalctl

/*
Copyright 2018 The Kubernetes Authors All rights reserved.

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

package logcounter

import (
	"fmt"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"k8s.io/node-problem-detector/cmd/logcounter/options"
	"k8s.io/node-problem-detector/pkg/logcounter/types"
	"k8s.io/node-problem-detector/pkg/systemlogmonitor"
	"k8s.io/node-problem-detector/pkg/systemlogmonitor/logwatchers/journald"
	watchertypes "k8s.io/node-problem-detector/pkg/systemlogmonitor/logwatchers/types"
	systemtypes "k8s.io/node-problem-detector/pkg/systemlogmonitor/types"
)

const (
	bufferSize        = 1000
	timeout           = 1 * time.Second
	journaldSourceKey = "source"
)

type logCounter struct {
	logCh         <-chan *systemtypes.Log
	buffer        systemlogmonitor.LogBuffer
	pattern       string
	revertPattern string
	clock         clock.Clock
}

func NewJournaldLogCounter(options *options.LogCounterOptions) (types.LogCounter, error) {
	watcher := journald.NewJournaldWatcher(watchertypes.WatcherConfig{
		Plugin:       "journald",
		PluginConfig: map[string]string{journaldSourceKey: options.JournaldSource},
		LogPath:      options.LogPath,
		Lookback:     options.Lookback,
		Delay:        options.Delay,
	})
	logCh, err := watcher.Watch()
	if err != nil {
		return nil, fmt.Errorf("error watching journald: %v", err)
	}
	return &logCounter{
		logCh:         logCh,
		buffer:        systemlogmonitor.NewLogBuffer(bufferSize),
		pattern:       options.Pattern,
		revertPattern: options.RevertPattern,
		clock:         clock.RealClock{},
	}, nil
}

func (e *logCounter) Count() (count int, err error) {
	start := e.clock.Now()
	for {
		select {
		case log, ok := <-e.logCh:
			if !ok {
				err = fmt.Errorf("log channel closed unexpectedly")
				return
			}

			klog.V(5).Infof("Received log: %v", log)

			// We only want to count events up until the time at which we started.
			// Otherwise we would run forever
			if start.Before(log.Timestamp) {
				return
			}
			e.buffer.Push(log)
			if len(e.buffer.Match(e.pattern)) != 0 {
				count++
			}
			if e.revertPattern != "" && len(e.buffer.Match(e.revertPattern)) != 0 {
				count--
			}
		case <-e.clock.After(timeout):
			// Don't block forever if we do not get any new messages
			return
		}
	}
}
