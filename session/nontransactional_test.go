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

func TestRandCase(t *testing.T) {
	store, clean := createStorage(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("create database test2")
	tk2.MustExec("use test2")

	sqls := []string{
		"create table tbl_4 ( col_16 mediumint  unsigned not null ,col_17 smallint  unsigned  default 64211 ,col_18 char ( 123 )    default 'GD$cjhoPNTgK~JKPS' ,col_19 double ,col_20 enum ( 'Alice','Bob','Charlie','David' )   not null , primary key  ( col_16 ,col_18 ) /*T![clustered_index] clustered */ ,unique key idx_8 ( col_16 ) ) charset binary collate binary partition by range ( col_16 ) ( partition p0 values less than (6160417), partition p1 values less than (8917959) );",
		"insert into tbl_4 values ( 10963918,39313,'MgQ&j4m',4121.448936065941,'Charlie' );",
		"insert into tbl_4 values ( 2542106,8316,'rkyvxvu$*8u_Qp-',378.4350860185114,'Charlie' );",
		"insert into tbl_4 values ( 9748324,2715,'xDPT)(Zh17JEfc^JKCu',3618.0951920659168,'Charlie' );",
		"insert into tbl_4 values ( 2284921,54164,'',230.07615760937918,'Bob' );",
		"insert into tbl_4 values ( 6580621,36141,'lD6bN_',8706.308232918482,'Alice' );",
		"insert into tbl_4 values ( 9729855,14427,'Gn6y#',7425.408195525086,'David' );",
		"insert into tbl_4 values ( 16118482,null,')-EVaVQeFSq-JkQa',9292.01575417356,'Bob' );",
		"insert into tbl_4 values ( 1358316,22266,'z~CgHr%69E7TF-r',4089.2425614144486,'Charlie' );",
		"insert into tbl_4 values ( 3493677,15129,'Z',6149.869655640478,'Alice' );",
		"insert into tbl_4 values ( 9290486,65420,'',1692.1349059705556,'Charlie' );",
		"insert  into tbl_4 set col_16 = 315523, col_17 = 51672, col_18 = 'Ku2fW9L', col_19 = 1595.6559222053074, col_20 = 'Charlie' ;",
		"insert  into tbl_4 set col_16 = 8701064, col_17 = 34355, col_18 = 'SGa5niLF~moN', col_19 = 1175.033809511273, col_20 = 'David' ;",
		"insert ignore into tbl_4 (col_16,col_17,col_18,col_19,col_20) values ( 4654879,6280,'q&d&4bjx-p',5628.227081657687,'Alice' ) ,( 4846875,42777,'1DZ3)_s~N-41oJFa',929.0293163693651,'Alice' ) ,( 2772218,415,'3Q6Fc*3hw',6529.194086834193,'David' ) ,( 10370015,59805,'mslzq+(p!j99jj@W',9015.298692773958,'Alice' ) ;",
		"replace into tbl_4 set col_16 = 10020984, col_17 = 56132, col_18 = 'e#h=0)=NUvjp', col_19 = 6355.371609321243, col_20 = 'Bob';",
		"split on col_17 limit 1 delete from tbl_4 where col_19 is null;",
		"replace into tbl_4 set col_16 = 8765620, col_17 = 36409, col_18 = '~', col_19 = 8597.982354249432, col_20 = 'Charlie';",
		"insert  into tbl_4  values ( 7390599,52596,'c~~*VlB!',1071.2193940041052,'Alice' ) on duplicate key update col_16 = 15443583;",
		"insert ignore into tbl_4 set col_16 = 7931974, col_17 = 15697, col_18 = 'Qbc(~4+', col_19 = 792.4411324397139, col_20 = 'Bob' on duplicate key update col_18 = '@#3I1X=!F+t_X6AJ~hR', col_17 = 35074;",
		"insert ignore into tbl_4 set col_16 = 908725, col_17 = null, col_18 = 'OC#0aAEaU0V', col_19 = 7212.916743741324, col_20 = 'David' on duplicate key update col_18 = 'OTkY)d9l_utrmY#D', col_16 = 13024365;",
		"split on col_19 limit 4 delete from tbl_4 where tbl_4.col_18 in ( '1k' ,'gYmo-_Sy98w7G' ,'GzPcMDZ^' ,'qwa' ,'M)FdI$w*d6' ,'fd#VNwV' ,'*' );",
		"insert  into tbl_4 (col_16,col_17,col_18,col_19,col_20) values ( 7634525,14080,'H8V',8664.2705284215,'Charlie' ) ,( 5017296,14107,'Q',2937.3975265466943,'Alice' ) on duplicate key update col_18 = '+H+UyW_68X#L6ERx', col_20 = 'Bob';",
		"split on col_17 limit 1 delete from tbl_4 where tbl_4.col_16 in ( 1330665 ,10467506 ,3493677 );",
		"insert  into tbl_4  values ( 7270895,2229,'AO5',8982.559830507913,'David' ) ,( 7735986,18028,'Q!!VPh&d',7746.198740161912,'Alice' ) ,( 2405809,36476,'Pm9ye~z-~ZenvnOoLO',6419.267797355345,'Bob' ) ,( 638372,25532,'N=6hJ',4634.936217424711,'Bob' ) ;",
		"insert ignore into tbl_4 (col_16,col_17,col_18,col_19,col_20) values ( 14295345,48013,'*6eaK4iY+',4429.776730282555,'Charlie' ) ,( 14298833,53744,'loI4bna7O^SjMKp&',4985.616251613528,'Bob' ) ,( 11354002,47330,'=p14!8t0#LlZX19p',8998.474552705939,'Charlie' ) ,( 12380050,1092,'zYZv)9o',6685.545645403333,'Bob' ) ,( 7694778,18844,'K3G)&0HHuD*8',null,'Alice' ) ;",
		"split on col_18 limit 4 delete from tbl_4 where col_18 is null;",
		"replace into tbl_4 set col_16 = 12513996, col_17 = 48564, col_18 = '', col_19 = 5651.935737268817, col_20 = 'Charlie';",
		"insert ignore into tbl_4 set col_16 = 7867243, col_17 = 41871, col_18 = 'G58eDF', col_19 = 9289.61376043116, col_20 = 'Bob' ;",
		"split on col_17 limit 4 delete from tbl_4 where tbl_4.col_18 in ( 'f%K^UCnItX~rNH&f8Y' ,'cu' );",
		// "insert  into tbl_4  values ( 3849184,29141,'vzvK%g',2983.788811040152,'Charlie' ) ,( 9266404,27566,'!d',null,'David' ) ,( 10450869,59825,'u)4axeMT24',4239.101374450549,'Bob' ) ,( 6485440,63545,'7a*7KdQ_+r_ngfo76Y',null,'Alice' ) ,( 605027,10000,'%*V',3087.8779060070087,'David' ) ,( 1570759,44827,'',4986.4238824037775,'Alice' ) ,( 4906838,32263,'^i*HMJ_xW',4885.209942652734,'David' ) on duplicate key update col_18 = '3KVOnF', col_19 = 982.6104992047269, col_16 = 179598, col_17 = null;",
		// "insert ignore into tbl_4 set col_16 = 9867825, col_17 = 42659, col_18 = 'jKCNXzZIQK0@SVR=8NB', col_19 = 8112.080898800254, col_20 = 'Bob' on duplicate key update col_16 = 10438899;",
		// "insert  into tbl_4  values ( 3368437,45589,'v)l2mvHhSTxoxp',1929.443445231295,'Alice' ) ,( 6119577,20122,'!',2915.3393297060334,'Alice' ) ,( 14614361,65046,'efA+OCSA$KL467',5271.579233848835,'Charlie' ) ,( 10530017,62336,'_n',4507.082864632823,'Alice' ) ,( 10464981,60651,'+B*&D*qWyeO',8529.217166391498,'Bob' ) on duplicate key update col_19 = 766.778343099527, col_20 = 'Alice', col_16 = 5739682, col_18 = 'Ty%u97LFmzl$gOqc', col_17 = 16434;",
		// "replace into tbl_4 set col_16 = 9323032, col_17 = 14664, col_18 = 'g', col_19 = 2754.0345921443327, col_20 = 'Bob';",
		// "replace into tbl_4  values ( 3970334,52714,'',209.63075245822054,'Bob' ) ,( 3317226,3502,'+9SFhYxcnnMpF^',1119.2533267635063,'Charlie' ) ,( 12420341,46526,'&=sO+CJc45hqG*n*_v',4265.841895760079,'Charlie' ) ,( 13356460,28654,'c',2124.185561421236,'Charlie' ) ,( 1804537,18317,'mh$b1Akb_Pyr@2teI',6492.054297443444,'Bob' ) ,( 9023887,22888,'9',3770.5542822946827,'Bob' ) ,( 4049515,19818,'W-DS~2S+-i$+2)Eo!V',5863.740372758795,'Charlie' );",
		// "SELECT * FROM tbl_4 ORDER BY col_16, col_17, col_18, col_19, col_20;",
	}
	query := "SELECT * FROM tbl_4 ORDER BY col_16, col_17, col_18, col_19, col_20;"
	for _, sql := range sqls {
		tk1.Exec(sql)
		if strings.HasPrefix(sql, "split") {
			sql = sql[strings.Index(sql, "delete"):]
			println(sql)
		}
		tk2.Exec(sql)
		// tk1.MustQuery(query).Check(tk2.MustQuery(query).Rows())
	}
	rows := tk1.MustQuery(query).Rows()
	for _, row := range rows {
		for _, col := range row {
			print(col.(string), " ")
		}
		println()
	}
	println(tk1.MustQuery("select count(*) from tbl_4").Rows()[0][0].(string), tk2.MustQuery("select count(*) from tbl_4").Rows()[0][0].(string))
}
