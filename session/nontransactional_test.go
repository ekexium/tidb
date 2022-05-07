// Copyright 2022 PingCAP, Inc.
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

package session_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestNonTransactionalDeleteSharding(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")

	tables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a int, b int, key(a, b))",
		"create table t(a int, b int, key(a))",
		"create table t(a int, b int, unique key(a, b))",
		"create table t(a int, b int, unique key(a))",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a) nonclustered)",
		"create table t(a varchar(30), b int, key(a, b))",
		"create table t(a varchar(30), b int, key(a))",
		"create table t(a varchar(30), b int, unique key(a, b))",
		"create table t(a varchar(30), b int, unique key(a))",
	}
	tableSizes := []int{0, 1, 10, 35, 40, 100}
	batchSizes := []int{1, 10, 25, 35, 50, 80, 120}
	for _, table := range tables {
		tk.Exec("drop table if exists t")
		tk.Exec(table)
		for _, tableSize := range tableSizes {
			for _, batchSize := range batchSizes {
				for i := 0; i < tableSize; i++ {
					tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
				}
				tk.MustQuery(fmt.Sprintf("split on a limit %d delete from t", batchSize)).Check(testkit.Rows(fmt.Sprintf("%d all succeeded", (tableSize+batchSize-1)/batchSize)))
				tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
			}
		}
	}
}

func TestNonTransactionalDeleteDryRun(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	rows := tk.MustQuery("split on a limit 3 dry run delete from t").Rows()
	for _, row := range rows {
		require.True(t, strings.HasPrefix(row[0].(string), "DELETE FROM `test`.`t` WHERE `a` BETWEEN"))
	}
	tk.MustQuery("split on a limit 3 dry run query delete from t").Check(testkit.Rows(
		"SELECT `a` FROM `test`.`t` WHERE TRUE ORDER BY IF(ISNULL(`a`),0,1),`a`"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
}

func TestNonTransactionalDeleteErrorMessage(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, primary key(a, b) clustered)")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	failpoint.Enable("github.com/pingcap/tidb/session/splitDeleteError", `return`)
	defer failpoint.Disable("github.com/pingcap/tidb/session/splitDeleteError")
	err := tk.ExecToErr("split on a limit 3 delete from t")
	require.EqualError(t, err, "Early return: error occurred in the first job. All jobs are canceled: injected split delete error")
}

func TestNonTransactionalDeleteSplitOnTiDBRowID(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	tk.Exec("split on _tidb_rowid limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteNull(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.Exec("insert into t values (null, null)")
	}

	tk.Exec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))

	// all values are null
	for i := 0; i < 100; i++ {
		tk.Exec("insert into t values (null, null)")
	}
	tk.Exec("split on a limit 3 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteSmallBatch(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=1024")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		tk.Exec("insert into t values (null, null)")
	}
	require.Equal(t, 1, len(tk.MustQuery("split on a limit 1000 dry run delete from t").Rows()))
	tk.Exec("split on a limit 1000 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteShardOnGeneratedColumn(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, c double as (sqrt(a * a + b * b)), key(c))")
	for i := 0; i < 1000; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d, default)", i, i*2))
	}
	tk.Exec("split on c limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteAutoDetectShardColumn(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")

	goodTables := []string{
		"create table t(a int, b int)",
		"create table t(a int, b int, primary key(a) clustered)",
		"create table t(a int, b int, primary key(a) nonclustered)",
		"create table t(a int, b int, primary key(a, b) nonclustered)",
		"create table t(a varchar(30), b int, primary key(a) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) nonclustered)",
	}
	badTables := []string{
		"create table t(a int, b int, primary key(a, b) clustered)",
		"create table t(a varchar(30), b int, primary key(a, b) clustered)",
	}

	testFunc := func(table string, expectSuccess bool) {
		tk.Exec("drop table if exists t")
		tk.Exec(table)
		for i := 0; i < 100; i++ {
			tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
		}
		_, err := tk.Exec("split limit 3 delete from t")
		require.Equal(t, expectSuccess, err == nil)
	}

	for _, table := range goodTables {
		testFunc(table, true)
	}
	for _, table := range badTables {
		testFunc(table, false)
	}
}

func TestNonTransactionalDeleteInvisibleIndex(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int)")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	err := tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.Exec("CREATE UNIQUE INDEX c1 ON t (a) INVISIBLE")
	err = tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.Exec("CREATE UNIQUE INDEX c2 ON t (a)")
	tk.Exec("split on a limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteIgnoreSelectLimit(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("set @@sql_select_limit=3")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.Exec("split on a limit 10 delete from t")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteReadStaleness(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("set @@tidb_read_staleness=-100")
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.Exec("split on a limit 10 delete from t")
	tk.Exec("set @@tidb_read_staleness=0")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("0"))
}

func TestNonTransactionalDeleteCheckConstraint(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")

	// For mocked tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	now := time.Now()
	safePointValue := now.Format(tikvutil.GCTimeFormat)
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf("INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s') ON DUPLICATE KEY UPDATE variable_value = '%[2]s', comment = '%[3]s'", safePointName, safePointValue, safePointComment)
	tk.Exec(updateSafePoint)

	tk.Exec("set @@tidb_max_chunk_size=35")
	tk.Exec("set @a=now(6)")

	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}
	tk.Exec("set @@tidb_snapshot=@a")
	err := tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.Exec("set @@tidb_snapshot=''")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	tk.Exec("set @@tidb_read_consistency=weak")
	err = tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.Exec("set @@tidb_read_consistency=strict")

	tk.Exec("set autocommit=0")
	err = tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.Exec("set autocommit=1")

	tk.Exec("begin")
	err = tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.Exec("commit")

	config.GetGlobalConfig().EnableBatchDML = true
	tk.Session().GetSessionVars().BatchInsert = true
	tk.Session().GetSessionVars().DMLBatchSize = 1
	err = tk.ExecToErr("split on a limit 10 delete from t")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	config.GetGlobalConfig().EnableBatchDML = false
	tk.Session().GetSessionVars().BatchInsert = false
	tk.Session().GetSessionVars().DMLBatchSize = 0

	err = tk.ExecToErr("split on a limit 10 delete from t limit 10")
	require.EqualError(t, err, "Non-transactional delete doesn't support limit")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	err = tk.ExecToErr("split on a limit 10 delete from t order by a")
	require.EqualError(t, err, "Non-transactional delete doesn't support order by")
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))

	err = tk.ExecToErr("prepare nt FROM 'split limit 1 delete from t'")
	require.EqualError(t, err, "[executor:1295]This command is not supported in the prepared statement protocol yet")
}

func TestNonTransactionalDeleteOptimizerHints(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 10; i++ {
		tk.Exec(fmt.Sprintf("insert into t values ('%d', %d)", i, i*2))
	}
	result := tk.MustQuery("split on a limit 10 dry run delete /*+ USE_INDEX(t) */ from t").Rows()[0][0].(string)
	require.Equal(t, result, "DELETE /*+ USE_INDEX(`t` )*/ FROM `test`.`t` WHERE `a` BETWEEN 0 AND 9")
}

func TestNonTransactionalDeleteMultiTables(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.Exec("use test")
	tk.Exec("create table t(a int, b int, key(a))")
	for i := 0; i < 100; i++ {
		tk.Exec(fmt.Sprintf("insert into t values (%d, %d)", i, i*2))
	}

	tk.Exec("create table t1(a int, b int, key(a))")
	tk.Exec("insert into t1 values (1, 1)")
	err := tk.ExecToErr("split limit 1 delete t, t1 from t, t1 where t.a = t1.a")
	require.Error(t, err)
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("100"))
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("1"))
}

func TestNonTransactionalDeleteAlias(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	goodSplitStmts := []string{
		"split on test.t1.a limit 5 delete t1.* from test.t as t1",
		"split on a limit 5 delete t1.* from test.t as t1",
		"split on _tidb_rowid limit 5 delete from test.t as t1",
		"split on t1._tidb_rowid limit 5 delete from test.t as t1",
		"split on test.t1._tidb_rowid limit 5 delete from test.t as t1",
		"split limit 5 delete from test.t as t1", // auto assigns table name to be the alias
	}

	badSplitStmts := []string{
		"split on test.t.a limit 5 delete t1.* from test.t as t1",
		"split on t.a limit 5 delete t1.* from test.t as t1",
		"split on t._tidb_rowid limit 5 delete from test.t as t1",
		"split on test.t._tidb_rowid limit 5 delete from test.t as t1",
	}

	tk.Exec("create table test.t(a int, b int, key(a))")
	tk.Exec("create table test.t2(a int, b int, key(a))")

	for _, sql := range goodSplitStmts {
		for i := 0; i < 5; i++ {
			tk.Exec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
		}
		tk.Exec(sql)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("0"))
	}

	for i := 0; i < 5; i++ {
		tk.Exec(fmt.Sprintf("insert into test.t values (%d, %d)", i, i*2))
	}
	for _, sql := range badSplitStmts {
		err := tk.ExecToErr(sql)
		require.Error(t, err)
		tk.MustQuery("select count(*) from test.t").Check(testkit.Rows("5"))
	}
}

func TestGBKUnsupported(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("create database test2")
	tk2.MustExec("use test2")
	tk1.MustQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='new_collation_enabled';").Check(testkit.Rows("True"))
	initSqls := []string{
		"create table tbl_3 ( col_11 text ( 74 ) collate utf8_bin ,col_12 enum ( 'Alice','Bob','Charlie','David' )   not null default 'Alice' ,col_13 text ( 65 ) collate gbk_bin ,col_14 varchar ( 300 ) collate utf8_unicode_ci  not null ,col_15 bit ( 20 )   not null , unique key idx_5 ( col_13 ( 3 ) ) ,key idx_6 ( col_11 ( 5 ) ), key(col_11(70)) ) charset binary collate binary ;",
		"insert into tbl_3  values ( 'zxLGrU0f-FQ','Bob','Iw噒M蕊+匡蝐%!EB朞齫','',484653 ) ,( 'Ac-Ireq=iHjcW','Bob','釂un轕0桓槄3踇N骦','dh$IkI',717480 ) ,( 'EUUZ%wrqVGK','Charlie',null,'M&Cj25j',1020133 ) ,( 'lala','David','xc籰熟襏I幼昶l~=T','m%XseKI582kqZiHD',870843 ) ,( 'K%@U1','Charlie','L鳩ekKrgl簝*幔豶S','3%vKr*rA!9Oni0Wpk',99665 );",
	}
	sqls := []string{
		"split on col_11 limit 2 delete from tbl_3 where not( tbl_3.col_11 in ( select col_11 from tbl_3 where not( tbl_3.col_11 in ( select col_13 from tbl_3 where not( tbl_3.col_11 in ( select col_11 from tbl_3 where tbl_3.col_11 = '' ) ) ) ) ) );",
		// "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'Ac-Ireq=iHjcW' AND 'Ac-Ireq=iHjcW' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))",
		// "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'EUUZ%wrqVGK' AND 'EUUZ%wrqVGK' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))",
		// "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'K%@U1' AND 'K%@U1' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))",
		// "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'lala' AND 'lala' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))",
		// "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'zxLGrU0f-FQ' AND 'zxLGrU0f-FQ' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))",
	}

	// query := "SELECT * FROM tbl_3 ORDER BY col_11, col_12, col_13, col_14, col_15;"
	query := "SELECT count(*) FROM tbl_3"

	for _, sql := range initSqls {
		tk1.MustExec(sql)
		tk2.MustExec(sql)
	}
	for _, sql := range sqls {
		tk1.Exec(sql)
		if strings.HasPrefix(sql, "split") {
			sql = sql[strings.Index(sql, "delete"):]
		}
		tk2.Exec(sql)
		println(sql)
		tk1.MustQuery(query).Check(tk2.MustQuery(query).Rows())
	}
	rows := tk1.MustQuery("SELECT * FROM tbl_3").Rows()
	for _, row := range rows {
		for _, col := range row {
			print(col.(string), " ")
		}
		println()
	}
	tk1.MustQuery("select count(*) from tbl_3").Check(tk2.MustQuery("select count(*) from tbl_3").Rows())
	println(tk1.MustQuery("select count(*) from tbl_3").Rows()[0][0].(string))
}

func TestBug(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// insert 5 rows.
	initSqls := []string{
		"create table tbl_3 ( col_11 text ( 74 ) collate utf8_bin ,col_12 enum ( 'Alice','Bob','Charlie','David' )   not null default 'Alice' ,col_13 text ( 65 ) collate gbk_bin ,col_14 varchar ( 300 ) collate utf8_unicode_ci  not null ,col_15 bit ( 20 )   not null , unique key idx_5 ( col_13 ( 3 ) ) ,key idx_6 ( col_11 ( 5 ) ), key(col_11(70)) ) charset binary collate binary ;",
		"insert into tbl_3  values ( 'zxLGrU0f-FQ','Bob','Iw噒M蕊+匡蝐%!EB朞齫','',484653 ) ,( 'Ac-Ireq=iHjcW','Bob','釂un轕0桓槄3踇N骦','dh$IkI',717480 ) ,( 'EUUZ%wrqVGK','Charlie',null,'M&Cj25j',1020133 ) ,( 'lala','David','xc籰熟襏I幼昶l~=T','m%XseKI582kqZiHD',870843 ) ,( 'K%@U1','Charlie','L鳩ekKrgl簝*幔豶S','3%vKr*rA!9Oni0Wpk',99665 );",
	}

	delete0 := "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'EUUZ%wrqVGK' AND 'EUUZ%wrqVGK' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))"
	delete1 := "DELETE FROM `tbl_3` WHERE (`col_11` BETWEEN 'K%@U1' AND 'K%@U1' AND NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_13` FROM `tbl_3` WHERE NOT (`tbl_3`.`col_11` IN (SELECT `col_11` FROM `tbl_3` WHERE (`tbl_3`.`col_11` = ''))))))))"

	tk.MustExec(initSqls[0])
	tk.MustExec(initSqls[1])
	// The statement can delete a row.
	tk.MustExec(delete1)
	tk.MustQuery("select count(*) from tbl_3").Check(testkit.Rows("4"))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("create database test2")
	tk2.MustExec("use test2")
	tk2.MustExec(initSqls[0])
	tk2.MustExec(initSqls[1])

	tk2.MustExec(delete0)
	tk2.MustQuery("select count(*) from tbl_3").Check(testkit.Rows("4"))

	tk2.MustExec(delete1)
	tk2.MustQuery("select count(*) from tbl_3").Check(testkit.Rows("3"))
}

func TestAnother(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("create database test2")
	tk2.MustExec("use test2")
	tk1.MustQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='new_collation_enabled';").Check(testkit.Rows("True"))
	initSqls := []string{
		"create table tbl_5 ( col_21 bit ( 60 )   not null default 683925682683277923 ,col_22 text ( 499 )   not null ,col_23 blob ( 10 )   not null ,col_24 bit ( 23 )   not null ,col_25 enum ( 'Alice','Bob','Charlie','David' ) , unique key idx_9 ( col_24 ,col_21 ,col_22 ( 5 ) ) ,unique key idx_10 ( col_23 ( 3 ) ) ) charset binary collate binary ;",
	}
	sqls := []string{
		"insert into tbl_5 values ( 195655191915635751,'O$aAa%%C+)#k0_zcYJ','',3025746,'Bob' );",
		"create table tbl_8 ( col_36 decimal ( 41 , 13 ) ,col_37 year   not null ,col_38 varbinary ( 86 )    default 'W3m@' ,col_39 set ( 'Alice','Bob','Charlie','David' )   not null default 'Alice' ,col_40 bit ( 11 )   not null default 1546 , primary key  ( col_40 ,col_37 ) /*T![clustered_index] clustered */ ,unique key idx_16 ( col_36 ,col_40 ,col_39 ) ) charset binary collate binary ;",
		"split on col_23 limit 1000 delete from tbl_5 where not( tbl_5.col_21 in ( select col_40 from tbl_8 where not( tbl_5.col_21 between 1033085249881905107 and 493477033195417129 ) ) );",
	}

	// query := "SELECT * FROM tbl_3 ORDER BY col_11, col_12, col_13, col_14, col_15;"
	query := "SELECT count(*) FROM tbl_5"

	for _, sql := range initSqls {
		tk1.MustExec(sql)
		tk2.MustExec(sql)
	}
	for _, sql := range sqls {
		tk1.Exec(sql)
		if strings.HasPrefix(sql, "split") {
			sql = sql[strings.Index(sql, "delete"):]
		}
		tk2.Exec(sql)
		println(sql)
		tk1.MustQuery(query).Check(tk2.MustQuery(query).Rows())
	}
	rows := tk1.MustQuery("SELECT * FROM tbl_5").Rows()
	for _, row := range rows {
		for _, col := range row {
			print(col.(string), " ")
		}
		println()
	}
	// tk1.MustQuery("select count(*) from tbl_3").Check(tk2.MustQuery("select count(*) from tbl_3").Rows())
	// println(tk1.MustQuery("select count(*) from tbl_3").Rows()[0][0].(string))
}
