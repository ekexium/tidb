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

package pipelinedtxntest

import (
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func compareTables(t *testing.T, tk *testkit.TestKit, t1, t2 string) {
	t1Rows := tk.MustQuery("select * from " + t1).Sort().Rows()
	t2Rows := tk.MustQuery("select * from " + t2)
	require.Equal(t, len(t1Rows), len(t2Rows.Rows()))
	t2Rows.Sort().Check(t1Rows)
}

func TestPipelinedTxnInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, _t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("create table _t like t")
	for i := 0; i < 10000; i++ {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("set session tidb_enable_pipelined_txn = 1")
	tk.MustExec("insert into _t select * from t")
	compareTables(t, tk, "t", "_t")

	tk.MustExec("insert into t select a + 10000, b from t")
	tk.MustQuery("select count(1) from t").Check(testkit.Rows("20000"))
}
