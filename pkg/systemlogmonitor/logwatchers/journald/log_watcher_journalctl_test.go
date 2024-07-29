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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/node-problem-detector/pkg/systemlogmonitor/logwatchers/types"
	logtypes "k8s.io/node-problem-detector/pkg/systemlogmonitor/types"
)

func TestParseJournalctlOutput(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	testCases := []struct {
		entry    string
		log      *logtypes.Log
		errorMsg *string
	}{
		{
			// has log message
			entry: "{\"__REALTIME_TIMESTAMP\":\"1234567890123456\",\"MESSAGE\":\"This is a test log message\"}",
			log: &logtypes.Log{
				Timestamp: time.Unix(0, 1234567890123456*1000),
				Message:   "This is a test log message",
			},
			errorMsg: nil,
		},
		{
			// log message has additional fields
			entry: "{\"__REALTIME_TIMESTAMP\":\"1234567890123456\",\"MESSAGE\":\"another log message\",\"__MONOTONIC_TIMESTAMP\":\"123456789\"}",
			log: &logtypes.Log{
				Timestamp: time.Unix(0, 1234567890123456*1000),
				Message:   "another log message",
			},
			errorMsg: nil,
		},
		{
			// log message has invalid __REALTIME_TIMESTAMP field
			entry:    "{\"__REALTIME_TIMESTAMP\":\"123abc\",\"MESSAGE\":\"another log message\",\"__MONOTONIC_TIMESTAMP\":\"123456789\"}",
			errorMsg: strPtr("failed to parse __REALTIME_TIMESTAMP: strconv.ParseInt: parsing \"123abc\": invalid syntax"),
		},
		{
			// missing __REALTIME_TIMESTAMP field
			entry:    "{\"MESSAGE\":\"This is a test log message\"}",
			errorMsg: strPtr("missing __REALTIME_TIMESTAMP field in journalctl output"),
		},
		{
			// missing MESSAGE field
			entry:    "{\"__REALTIME_TIMESTAMP\":\"1719976451991183\"}",
			errorMsg: strPtr("missing MESSAGE field in journalctl output"),
		},
		{
			// no log message
			entry:    "",
			errorMsg: strPtr("failed to unmarshal journalctl output: unexpected end of JSON input"),
		},
	}

	for c, test := range testCases {
		t.Logf("TestCase #%d: %#v", c+1, test)
		out, err := parseJournalctlOutput(test.entry)
		if test.errorMsg != nil {
			assert.Error(t, err, "Expected parseJournalctlOutput to return an error.")
			assert.Equal(t, *test.errorMsg, err.Error())
		} else {
			assert.NoError(t, err, "Expected parseJournalctlOutput error to be nil.")
			assert.Equal(t, test.log, out)
		}
	}
}

func TestGoroutineLeak(t *testing.T) {
	original := runtime.NumGoroutine()
	w := NewJournaldWatcher(types.WatcherConfig{
		Plugin:       "journald",
		PluginConfig: map[string]string{"source": "not-exist-service"},
		LogPath:      "/not/exist/path",
		Lookback:     "10m",
	})
	_, err := w.Watch()
	assert.Error(t, err)
	assert.Equal(t, original, runtime.NumGoroutine())
}
