// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
)

// pipelinedMemBuffer wraps tikv.MemDB as kv.MemBuffer.
type pipelinedMemBuffer struct {
	*tikv.PipelinedMemDB
}

func newPipelinedMemBuffer(p *tikv.PipelinedMemDB) kv.MemBuffer {
	if p == nil {
		return nil
	}
	return &pipelinedMemBuffer{PipelinedMemDB: p}
}

func (m *pipelinedMemBuffer) Size() int {
	return m.PipelinedMemDB.Size()
}

func (m *pipelinedMemBuffer) Delete(k kv.Key) error {
	return m.PipelinedMemDB.GetMemDB().Delete(k)
}

func (m *pipelinedMemBuffer) RemoveFromBuffer(k kv.Key) {
	m.PipelinedMemDB.GetMemDB().RemoveFromBuffer(k)
}

func (m *pipelinedMemBuffer) DeleteWithFlags(k kv.Key, ops ...kv.FlagsOp) error {
	err := m.PipelinedMemDB.GetMemDB().DeleteWithFlags(k, getTiKVFlagsOps(ops)...)
	return derr.ToTiDBErr(err)
}

func (m *pipelinedMemBuffer) UpdateFlags(k kv.Key, ops ...kv.FlagsOp) {
	m.PipelinedMemDB.GetMemDB().UpdateFlags(k, getTiKVFlagsOps(ops)...)
}

func (m *pipelinedMemBuffer) Get(_ context.Context, key kv.Key) ([]byte, error) {
	data, err := m.PipelinedMemDB.Get(key)
	return data, derr.ToTiDBErr(err)
}

func (m *pipelinedMemBuffer) GetFlags(key kv.Key) (kv.KeyFlags, error) {
	data, err := m.PipelinedMemDB.GetFlags(key)
	return getTiDBKeyFlags(data), derr.ToTiDBErr(err)
}

// Staging returns 0 handle.
func (m *pipelinedMemBuffer) Staging() kv.StagingHandle {
	return 0
}

// Cleanup will not take effect.
func (m *pipelinedMemBuffer) Cleanup(h kv.StagingHandle) {}

// Release will not take effect.
func (m *pipelinedMemBuffer) Release(h kv.StagingHandle) {}

// InspectStage will not take effect.
func (m *pipelinedMemBuffer) InspectStage(handle kv.StagingHandle, f func(kv.Key, kv.KeyFlags, []byte)) {
}

func (m *pipelinedMemBuffer) Set(key kv.Key, value []byte) error {
	err := m.PipelinedMemDB.GetMemDB().SetWithFlags(key, value, tikvstore.SetPresumeKeyNotExists)
	return derr.ToTiDBErr(err)
}

func (m *pipelinedMemBuffer) SetWithFlags(key kv.Key, value []byte, ops ...kv.FlagsOp) error {
	err := m.PipelinedMemDB.GetMemDB().SetWithFlags(key, value, append(getTiKVFlagsOps(ops), tikvstore.SetPresumeKeyNotExists)...)
	return derr.ToTiDBErr(err)
}

// Iter creates an Iterator positioned on the first entry that k <= entry's key.
// If such entry is not found, it returns an invalid Iterator with no error.
// It yields only keys that < upperBound. If upperBound is nil, it means the upperBound is unbounded.
// The Iterator must be Closed after use.
func (m *pipelinedMemBuffer) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	return nil, errors.New("can not iterate")
}

// IterReverse creates a reversed Iterator positioned on the first entry which key is less than k.
// The returned iterator will iterate from greater key to smaller key.
// If k is nil, the returned iterator will be positioned at the last key.
func (m *pipelinedMemBuffer) IterReverse(k, lowerBound kv.Key) (kv.Iterator, error) {
	return nil, errors.New("can not iterate")
}

// SnapshotIter returns a Iterator for a snapshot of MemBuffer.
func (m *pipelinedMemBuffer) SnapshotIter(k, upperbound kv.Key) kv.Iterator {
	it := m.PipelinedMemDB.GetMemDB().SnapshotIter(k, upperbound)
	return &tikvIterator{Iterator: it}
}

func (m *pipelinedMemBuffer) SnapshotIterReverse(k, lowerBound kv.Key) kv.Iterator {
	it := m.GetMemDB().SnapshotIterReverse(k, lowerBound)
	return &tikvIterator{Iterator: it}
}

// SnapshotGetter returns a Getter for a snapshot of MemBuffer.
func (m *pipelinedMemBuffer) SnapshotGetter() kv.Getter {
	return newKVGetter(m.GetMemDB().SnapshotGetter())
}

// MayFlush implements kv.MemBuffer.MayFlush interface.
func (m *pipelinedMemBuffer) MayFlush() error {
	return m.PipelinedMemDB.MayFlush()
}
