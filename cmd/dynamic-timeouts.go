/*
 * MinIO Cloud Storage, (C) 2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dynamicTimeoutIncreaseThresholdPct = 0.33 // Upper threshold for failures in order to increase timeout
	dynamicTimeoutDecreaseThresholdPct = 0.10 // Lower threshold for failures in order to decrease timeout
	dynamicTimeoutLogSize              = 16
	maxDuration                        = math.MaxInt64
	maxDynamicTimeout                  = 24 * time.Hour // Never set timeout bigger than this.
)

// timeouts that are dynamically adapted based on actual usage results
type DynamicTimeout struct {
	timeout int64
	minimum int64
	entries int64
	log     [dynamicTimeoutLogSize]time.Duration
	mutex   sync.Mutex
}

// NewDynamicTimeout returns a new dynamic timeout initialized with timeout value
func NewDynamicTimeout(timeout, minimum time.Duration) *DynamicTimeout {
	if timeout <= 0 || minimum <= 0 {
		panic("NewDynamicTimeout: negative or zero timeout")
	}
	if minimum > timeout {
		minimum = timeout
	}
	return &DynamicTimeout{timeout: int64(timeout), minimum: int64(minimum)}
}

// Timeout returns the current timeout value
func (dt *DynamicTimeout) Timeout() time.Duration {
	return time.Duration(atomic.LoadInt64(&dt.timeout))
}

// LogSuccess logs the duration of a successful action that
// did not hit the timeout
func (dt *DynamicTimeout) LogSuccess(duration time.Duration) {
	dt.logEntry(duration)
}

// LogFailure logs an action that hit the timeout
func (dt *DynamicTimeout) LogFailure() {
	dt.logEntry(maxDuration)
}

// logEntry stores a log entry
func (dt *DynamicTimeout) logEntry(duration time.Duration) {
	if duration < 0 {
		return
	}
	entries := int(atomic.AddInt64(&dt.entries, 1))
	index := entries - 1
	if index < dynamicTimeoutLogSize {
		dt.mutex.Lock()
		dt.log[index] = duration

		// We leak entries while we copy
		if entries == dynamicTimeoutLogSize {

			// Make copy on stack in order to call adjust()
			logCopy := [dynamicTimeoutLogSize]time.Duration{}
			copy(logCopy[:], dt.log[:])

			// reset log entries
			atomic.StoreInt64(&dt.entries, 0)
			dt.mutex.Unlock()

			dt.adjust(logCopy)
			return
		}
		dt.mutex.Unlock()
	}
}

// adjust changes the value of the dynamic timeout based on the
// previous results
func (dt *DynamicTimeout) adjust(entries [dynamicTimeoutLogSize]time.Duration) {
	failures, max := 0, time.Duration(0)
	for _, dur := range entries[:] {
		if dur == maxDuration {
			failures++
		} else if dur > max {
			max = dur
		}
	}

	failPct := float64(failures) / float64(len(entries))

	if failPct > dynamicTimeoutIncreaseThresholdPct {
		// We are hitting the timeout too often, so increase the timeout by 25%
		timeout := atomic.LoadInt64(&dt.timeout) * 125 / 100

		// Set upper cap.
		if timeout > int64(maxDynamicTimeout) {
			timeout = int64(maxDynamicTimeout)
		}
		// Safety, shouldn't happen
		if timeout < dt.minimum {
			timeout = dt.minimum
		}
		atomic.StoreInt64(&dt.timeout, timeout)
	} else if failPct < dynamicTimeoutDecreaseThresholdPct {
		// We are hitting the timeout relatively few times,
		// so decrease the timeout towards 25 % of maximum time spent.
		max = max * 125 / 100

		timeout := atomic.LoadInt64(&dt.timeout)
		if max < time.Duration(timeout) {
			// Move 50% toward the max.
			timeout = (int64(max) + timeout) / 2
		}
		if timeout < dt.minimum {
			timeout = dt.minimum
		}
		atomic.StoreInt64(&dt.timeout, timeout)
	}
}
