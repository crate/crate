/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.sql.parser;

import static io.crate.sql.parser.TreeAssertions.assertFormattedSql;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.crate.sql.Literals;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.AlterPublication;
import io.crate.sql.tree.AlterRoleReset;
import io.crate.sql.tree.AlterRoleSet;
import io.crate.sql.tree.AlterServer;
import io.crate.sql.tree.AlterSubscription;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArrayLikePredicate;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.BeginStatement;
import io.crate.sql.tree.Close;
import io.crate.sql.tree.CommitStatement;
import io.crate.sql.tree.ComparisonExpression;
import io.crate.sql.tree.CopyFrom;
import io.crate.sql.tree.CreateForeignTable;
import io.crate.sql.tree.CreateFunction;
import io.crate.sql.tree.CreatePublication;
import io.crate.sql.tree.CreateRole;
import io.crate.sql.tree.CreateServer;
import io.crate.sql.tree.CreateSubscription;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateTableAs;
import io.crate.sql.tree.CreateUserMapping;
import io.crate.sql.tree.DeallocateStatement;
import io.crate.sql.tree.Declare;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.DenyPrivilege;
import io.crate.sql.tree.DropAnalyzer;
import io.crate.sql.tree.DropBlobTable;
import io.crate.sql.tree.DropForeignTable;
import io.crate.sql.tree.DropFunction;
import io.crate.sql.tree.DropPublication;
import io.crate.sql.tree.DropRepository;
import io.crate.sql.tree.DropRole;
import io.crate.sql.tree.DropServer;
import io.crate.sql.tree.DropSnapshot;
import io.crate.sql.tree.DropSubscription;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.DropUserMapping;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Fetch;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.GCDanglingArtifacts;
import io.crate.sql.tree.GrantPrivilege;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.KillStatement;
import io.crate.sql.tree.MatchPredicate;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.RevokePrivilege;
import io.crate.sql.tree.SetSessionAuthorizationStatement;
import io.crate.sql.tree.SetStatement;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.SwapTable;
import io.crate.sql.tree.Update;
import io.crate.sql.tree.Window;
import io.crate.sql.tree.With;

public class TestStatementBuilder {

    @Test
    public void testMultipleStatements() {
        List<Statement> statements = SqlParser.createStatementsForSimpleQuery("BEGIN; END;", str -> str);
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0)).isExactlyInstanceOf(BeginStatement.class);
        assertThat(statements.get(1)).isExactlyInstanceOf(CommitStatement.class);

        statements = SqlParser.createStatementsForSimpleQuery("BEGIN; END", str -> str);
        assertThat(statements).hasSize(2);
        assertThat(statements.get(0)).isExactlyInstanceOf(BeginStatement.class);
        assertThat(statements.get(1)).isExactlyInstanceOf(CommitStatement.class);

        statements = SqlParser.createStatementsForSimpleQuery("BEGIN", str -> str);
        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isExactlyInstanceOf(BeginStatement.class);

        statements = SqlParser.createStatementsForSimpleQuery("SET extra_float_digits = 3", str -> str);
        assertThat(statements).hasSize(1);
        assertThat(statements.get(0)).isExactlyInstanceOf(SetStatement.class);
    }

    @Test
    public void test_declare() {
        printStatement("DECLARE c1 CURSOR FOR select 1");
        printStatement("DECLARE c1 BINARY CURSOR FOR select 1");
        printStatement("DECLARE c1 INSENSITIVE CURSOR FOR select 1");
        printStatement("DECLARE c1 ASENSITIVE CURSOR FOR select 1");
        printStatement("DECLARE c1 SCROLL CURSOR FOR select 1");
        printStatement("DECLARE c1 NO SCROLL CURSOR FOR select 1");
        printStatement("DECLARE c1 NO SCROLL BINARY ASENSITIVE CURSOR FOR select 1");
    }

    @Test
    public void test_fetch() {
        printStatement("FETCH FROM c1");
        printStatement("FETCH IN c1");
        printStatement("FETCH FIRST FROM c1");
        printStatement("FETCH LAST FROM c1");
        printStatement("FETCH ALL FROM c1");
        printStatement("FETCH FORWARD FROM c1");
        printStatement("FETCH FORWARD 4 FROM c1");
        printStatement("FETCH ABSOLUTE 3 FROM c1");
        printStatement("FETCH BACKWARD 4 FROM c1");
        printStatement("FETCH RELATIVE -12 FROM c1");

        Fetch fetch = (Fetch) SqlParser.createStatement("FETCH FORWARD 3 FROM c1");
        assertThat(fetch.count()).isEqualTo(3L);
    }

    @Test
    public void test_close() {
        printStatement("CLOSE ALL");
        printStatement("CLOSE c1");
    }

    @Test
    public void testBegin() {
        printStatement("BEGIN");
        printStatement("BEGIN WORK");
        printStatement("BEGIN WORK DEFERRABLE");
        printStatement("BEGIN TRANSACTION");
        printStatement("BEGIN TRANSACTION DEFERRABLE");
        printStatement("BEGIN ISOLATION LEVEL SERIALIZABLE, " +
                       "      READ WRITE," +
                       "      NOT DEFERRABLE");
        // PG supports leaving out the `,` separator for BWC reasons. Let's support it as well.
        printStatement("BEGIN ISOLATION LEVEL SERIALIZABLE " +
            "      READ WRITE" +
            "      NOT DEFERRABLE");
    }

    @Test
    public void testStartTransaction() {
        printStatement("START TRANSACTION");
        printStatement("START TRANSACTION DEFERRABLE");
        printStatement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE, " +
                       "      READ WRITE," +
                       "      NOT DEFERRABLE");
        // PG supports leaving out the `,` separator for BWC reasons. Let's support it as well.
        printStatement("START TRANSACTION ISOLATION LEVEL SERIALIZABLE " +
            "      READ WRITE" +
            "      NOT DEFERRABLE");
    }

    @Test
    public void testSetTransaction() {
        printStatement("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        printStatement("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        printStatement("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
        printStatement("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        printStatement("SET TRANSACTION READ WRITE");
        printStatement("SET TRANSACTION READ ONLY");
        printStatement("SET TRANSACTION DEFERRABLE");
        printStatement("SET TRANSACTION NOT DEFERRABLE");
        printStatement("SET TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    }

    @Test
    public void test_analyze_statement_can_be_parsed() {
        printStatement("ANALYZE");
    }

    @Test
    public void test_discard_statement_parsing() {
        printStatement("DISCARD ALL");
        printStatement("DISCARD PLANS");
        printStatement("DISCARD SEQUENCES");
        printStatement("DISCARD TEMPORARY");
        printStatement("DISCARD TEMP");
    }

    @Test
    public void testEmptyOverClauseAfterFunction() {
        printStatement("SELECT avg(x) OVER () FROM t");
    }

    @Test
    public void testOverClauseWithOrderBy() {
        printStatement("SELECT avg(x) OVER (ORDER BY x) FROM t");
        printStatement("SELECT avg(x) OVER (ORDER BY x DESC) FROM t");
        printStatement("SELECT avg(x) OVER (ORDER BY x DESC NULLS FIRST) FROM t");
    }

    @Test
    public void testOverClauseWithPartition() {
        printStatement("SELECT avg(x) OVER (PARTITION BY p) FROM t");
        printStatement("SELECT avg(x) OVER (PARTITION BY p1, p2, p3) FROM t");
    }

    @Test
    public void testOverClauseWithPartitionAndOrderBy() {
        printStatement("SELECT avg(x) OVER (PARTITION BY p ORDER BY x ASC) FROM t");
        printStatement("SELECT avg(x) OVER (PARTITION BY p1, p2, p3 ORDER BY x, y) FROM t");
    }

    @Test
    public void testOverClauseWithFrameClause() {
        printStatement("SELECT avg(x) OVER (ROWS BETWEEN 5 PRECEDING AND 10 FOLLOWING) FROM t");
        printStatement("SELECT avg(x) OVER (RANGE BETWEEN 5 PRECEDING AND 10 FOLLOWING) FROM t");
        printStatement("SELECT avg(x) OVER (ROWS UNBOUND PRECEDING) FROM t");
        printStatement("SELECT avg(x) OVER (ROWS BETWEEN UNBOUND PRECEDING AND CURRENT ROW) FROM t");
        printStatement("SELECT avg(x) OVER (ROWS BETWEEN 10 PRECEDING AND UNBOUND FOLLOWING) FROM t");
    }

    @Test
    public void test_over_references_empty_window_def() {
        printStatement("SELECT avg(x) OVER (w) FROM t WINDOW w AS ()");
        printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS ()");
        printStatement("SELECT avg(x) OVER w, sum(x) OVER w FROM t WINDOW w AS ()");
    }

    @Test
    public void test_over_with_order_by_or_frame_references_empty_window_def() {
        printStatement("SELECT avg(x) OVER (w ORDER BY x) FROM t WINDOW w AS ()");
        printStatement("SELECT avg(x) OVER (w ROWS UNBOUND PRECEDING) FROM t WINDOW w AS ()");
    }

    @Test
    public void test_over_references_window_with_order_by_or_partition_by() {
        printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS (PARTITION BY x)");
        printStatement("SELECT avg(x) OVER (w) FROM t WINDOW w AS (ORDER BY x)");
        printStatement("SELECT avg(x) OVER w FROM t WINDOW w AS (PARTITION BY x ORDER BY x)");
    }

    @Test
    public void test_multiple_window_definitions_referenced_in_over() {
        printStatement("SELECT avg(x) OVER w1, avg(x) OVER w2 FROM t WINDOW w1 AS (), w2 AS ()");
    }

    @Test
    public void test_ignore_nulls_and_respect_nulls_options() {
        printStatement("SELECT avg(x) IGNORE NULLS OVER w1, avg(x) RESPECT NULLS OVER w2 FROM t WINDOW w1 AS (), w2 AS ()");
    }

    @Test
    public void test_over_does_not_reference_window_definitions_from_window_clause() {
        printStatement("SELECT x FROM t WINDOW w AS ()");
        printStatement("SELECT avg(x) OVER () FROM t WINDOW w AS () ");
    }

    @Test
    public void test_duplicates_in_window_definitions() {
        assertThatThrownBy(
            () -> printStatement("SELECT x FROM t WINDOW w AS (), w as ()"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Window w is already defined");
    }

    @Test
    public void test_circular_references_in_window_definitions() {
        assertThatThrownBy(
            () -> printStatement("SELECT x FROM t WINDOW w AS (ww), ww as (w), www as ()"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Window ww does not exist");
    }

    @Test
    public void testCommit() {
        printStatement("COMMIT");
        printStatement("COMMIT WORK");
        printStatement("COMMIT TRANSACTION");
    }

    @Test
    public void testEnd() {
        printStatement("END");
        printStatement("END WORK");
        printStatement("END TRANSACTION");
    }

    @Test
    public void testNullNotAllowedAsArgToExtractField() {
        assertThatThrownBy(
            () -> printStatement("select extract(null from x)"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:16: no viable alternative at input 'select extract(null'");
    }

    @Test
    public void testShowCreateTableStmtBuilder() {
        printStatement("show create table test");
        printStatement("show create table foo.test");
        printStatement("show create table \"select\"");
    }

    @Test
    public void testDropTableStmtBuilder() {
        printStatement("drop table test");
        printStatement("drop table if exists test");
        printStatement("drop table bar.foo");
    }

    @Test
    public void testSwapTable() {
        printStatement("ALTER CLUSTER SWAP TABLE t1 TO t2");
        printStatement("ALTER CLUSTER SWAP TABLE t1 TO t2 WITH (prune_second = true)");
    }

    @Test
    public void testAlterClusterGCDanglingArtifacts() {
        printStatement("ALTER CLUSTER GC DANGLING ARTIFACTS");
    }

    @Test
    public void testAlterClusterDecommissionNode() {
        printStatement("ALTER CLUSTER DECOMMISSION 'node1'");
    }

    @Test
    public void test_decommission_node_id_is_missing() {
        assertThatThrownBy(
            () -> printStatement("ALTER CLUSTER DECOMMISSION'"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:27: mismatched input ''' expecting {");
    }

    @Test
    public void testStmtWithSemicolonBuilder() {
        printStatement("select 1;");
    }

    @Test
    public void testShowTablesStmtBuilder() {
        printStatement("show tables");
        printStatement("show tables like '.*'");
        printStatement("show tables from table_schema");
        printStatement("show tables from \"tableSchema\"");
        printStatement("show tables in table_schema");
        printStatement("show tables from foo like '.*'");
        printStatement("show tables in foo like '.*'");
        printStatement("show tables from table_schema like '.*'");
        printStatement("show tables in table_schema like '*'");
        printStatement("show tables in table_schema where name = 'foo'");
        printStatement("show tables in table_schema where name > 'foo'");
        printStatement("show tables in table_schema where name != 'foo'");
    }

    @Test
    public void testShowColumnsStmtBuilder() {
        printStatement("show columns from table_name");
        printStatement("show columns in table_name");
        printStatement("show columns from table_name from table_schema");
        printStatement("show columns in table_name from table_schema");
        printStatement("show columns in foo like '*'");
        printStatement("show columns from foo like '*'");
        printStatement("show columns from table_name from table_schema like '*'");
        printStatement("show columns in table_name from table_schema like '*'");
        printStatement("show columns from table_name where column_name = 'foo'");
        printStatement("show columns from table_name from table_schema where column_name = 'foo'");
    }

    @Test
    public void testDeleteFromStmtBuilder() {
        printStatement("delete from foo as alias");
        printStatement("delete from foo");
        printStatement("delete from schemah.foo where foo.a=foo.b and a is not null");
        printStatement("delete from schemah.foo as alias where foo.a=foo.b and a is not null");
    }

    @Test
    public void testShowSchemasStmtBuilder() {
        printStatement("show schemas");
        printStatement("show schemas like 'doc%'");
        printStatement("show schemas where schema_name='doc'");
        printStatement("show schemas where schema_name LIKE 'd%'");
    }

    @Test
    public void testShowParameterStmtBuilder() {
        printStatement("show search_path");
        printStatement("show all");
    }

    @Test
    public void testUpdateStmtBuilder() {
        printStatement("update foo set \"column['looks_like_nested']\"=1");
        printStatement("update foo set foo.a='b'");
        printStatement("update bar.foo set bar.foo.t=3");
        printStatement("update foo set col['x'] = 3");
        printStatement("update foo set col['x'] = 3 where foo['x'] = 2");
        printStatement("update schemah.foo set foo.a='b', foo.b=foo.a");
        printStatement("update schemah.foo set foo.a=abs(-6.3334), x=true where x=false");
    }

    @Test
    public void test_update_returning_clause() {
        printStatement("update foo set foo='a' returning id");
        printStatement("update foo set foo='a' where x=false returning id");
        printStatement("update foo set foo='a' returning id AS foo");
        printStatement("update foo set foo='a' returning id + 1 AS foo, id -1 as bar");
        printStatement("update foo set foo='a' returning \"column['nested']\" AS bar");
        printStatement("update foo set foo='a' returning foo.bar - 1 AS foo");
        printStatement("update foo set foo='a' returning *");
        printStatement("update foo set foo='a' returning foo.*");
    }

    @Test
    public void testExplainStmtBuilder() {
        printStatement("explain select * from foo");
        printStatement("explain analyze select * from foo");
        printStatement("explain (costs) select * from foo");
        printStatement("explain (costs true) select * from foo");
        printStatement("explain (costs false) select * from foo");
        printStatement("explain (analyze) select * from foo");
        printStatement("explain (analyze true) select * from foo");
        printStatement("explain (analyze false) select * from foo");
        printStatement("explain (costs, analyze) select * from foo");
        printStatement("explain (costs true, analyze true) select * from foo");
        printStatement("explain (costs false, analyze false) select * from foo");
        printStatement("explain verbose select * from foo");
        printStatement("explain (costs, verbose) select * from foo");
        printStatement("explain (verbose) select * from foo");
        printStatement("explain (costs false, verbose true) select * from foo");
    }

    @Test
    public void testSetStmtBuiler() {
        printStatement("set session some_setting = 1, ON");
        printStatement("set session some_setting = false");
        printStatement("set session some_setting = DEFAULT");
        printStatement("set session some_setting = 1, 2, 3");
        printStatement("set session some_setting = ON");
        printStatement("set session some_setting = 'value'");

        printStatement("set session some_setting TO DEFAULT");
        printStatement("set session some_setting TO 'value'");
        printStatement("set session some_setting TO 1, 2, 3");
        printStatement("set session some_setting TO ON");
        printStatement("set session some_setting TO true");
        printStatement("set session some_setting TO 1, ON");

        printStatement("set local some_setting = DEFAULT");
        printStatement("set local some_setting = 'value'");
        printStatement("set local some_setting = 1, 2, 3");
        printStatement("set local some_setting = 1, ON");
        printStatement("set local some_setting = ON");
        printStatement("set local some_setting = false");

        printStatement("set local some_setting TO DEFAULT");
        printStatement("set local some_setting TO 'value'");
        printStatement("set local some_setting TO 1, 2, 3");
        printStatement("set local some_setting TO ON");
        printStatement("set local some_setting TO true");
        printStatement("set local some_setting TO ALWAYS");

        printStatement("set some_setting TO 1, 2, 3");
        printStatement("set some_setting TO ON");

        printStatement("set session characteristics as transaction isolation level read uncommitted");
    }

    @Test
    public void test_set_session_authorization_statement_with_user() {
        printStatement("set session authorization 'test'");
        printStatement("set session authorization test");
        printStatement("set session session authorization test");
        printStatement("set local session authorization 'test'");
    }

    @Test
    public void test_set_session_authorization_statement_with_default_user() {
        printStatement("set session authorization default");
        printStatement("set session session authorization default");
        printStatement("set local session authorization default");
    }

    @Test
    public void test_reset_session_authorization_statement() {
        printStatement("reset session authorization");
    }

    @Test
    public void testKillStmtBuilder() {
        printStatement("kill all");
        printStatement("kill '6a3d6fb6-1401-4333-933d-b38c9322fca7'");
        printStatement("kill ?");
        printStatement("kill $1");
    }

    @Test
    public void testKillJob() {
        KillStatement<?> stmt = (KillStatement<?>) SqlParser.createStatement("KILL $1");
        assertThat(stmt.jobId()).isNotNull();
    }

    @Test
    public void testKillAll() {
        Statement stmt = SqlParser.createStatement("KILL ALL");
        assertThat(stmt).isEqualTo(new KillStatement<>(null));
    }

    @Test
    public void testDeallocateStmtBuilder() {
        printStatement("deallocate all");
        printStatement("deallocate prepare all");
        printStatement("deallocate 'myStmt'");
        printStatement("deallocate myStmt");
        printStatement("deallocate test.prep.stmt");
    }

    @Test
    public void testDeallocateWithoutParamThrowsParsingException() {
        assertThatThrownBy(
            () -> printStatement("deallocate"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:11: mismatched input '<EOF>'");
    }

    @Test
    public void testDeallocate() {
        DeallocateStatement stmt = (DeallocateStatement) SqlParser.createStatement("DEALLOCATE test_prep_stmt");
        assertThat(stmt.preparedStmt()).hasToString("'test_prep_stmt'");
        stmt = (DeallocateStatement) SqlParser.createStatement("DEALLOCATE 'test_prep_stmt'");
        assertThat(stmt.preparedStmt()).hasToString("'test_prep_stmt'");
    }

    @Test
    public void testDeallocateAll() {
        Statement stmt = SqlParser.createStatement("DEALLOCATE ALL");
        assertThat(stmt.equals(new DeallocateStatement())).isTrue();
    }

    @Test
    public void testRefreshStmtBuilder() {
        printStatement("refresh table t");
        printStatement("refresh table t partition (pcol='val'), tableh partition (pcol='val')");
        printStatement("refresh table schemah.tableh");
        printStatement("refresh table tableh partition (pcol='val')");
        printStatement("refresh table tableh partition (pcol=?)");
        printStatement("refresh table tableh partition (pcol['nested'] = ?)");
    }

    @Test
    public void testOptimize() {
        printStatement("optimize table t");
        printStatement("optimize table t1, t2");
        printStatement("optimize table schema.t");
        printStatement("optimize table schema.t1, schema.t2");
        printStatement("optimize table t partition (pcol='val')");
        printStatement("optimize table t partition (pcol=?)");
        printStatement("optimize table t partition (pcol['nested'] = ?)");
        printStatement("optimize table t partition (pcol='val') with (param1=val1, param2=val2)");
        printStatement("optimize table t1 partition (pcol1='val1'), t2 partition (pcol2='val2')");
        printStatement("optimize table t1 partition (pcol1='val1'), t2 partition (pcol2='val2') " +
            "with (param1=val1, param2=val2, param3='val3')");
    }

    @Test
    public void testSetSessionInvalidSetting() {
        assertThatThrownBy(
            () -> printStatement("set session 'some_setting' TO 1, ON"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:13: no viable alternative at input 'set session 'some_setting''");
    }

    @Test
    public void testSetGlobal() {
        printStatement("set global sys.cluster['some_settings']['3'] = '1'");
        printStatement("set global sys.cluster['some_settings'] = '1', other_setting = 2");
        printStatement("set global transient sys.cluster['some_settings'] = '1'");
        printStatement("set global persistent sys.cluster['some_settings'] = '1'");
    }

    @Test
    public void test_set_timezone_value() {
        SetStatement<?> stmt = (SetStatement<?>) SqlParser.createStatement("SET TIME ZONE 'Europe/Vienna'");
        assertThat(stmt.scope()).isEqualTo(SetStatement.Scope.TIME_ZONE);
        assertThat(stmt.assignments()).hasSize(1);
        assertThat(stmt.assignments().get(0).expressions().get(0)).hasToString("'Europe/Vienna'");
    }

    @Test
    public void test_set_timezone_local() {
        SetStatement<?> stmt = (SetStatement<?>) SqlParser.createStatement("SET TIME ZONE LOCAL");
        assertThat(stmt.scope()).isEqualTo(SetStatement.Scope.TIME_ZONE);
        assertThat(stmt.assignments()).hasSize(1);
        assertThat(stmt.assignments().get(0).expressions().get(0)).hasToString("'LOCAL'");
    }

    @Test
    public void test_set_timezone_default() {
        SetStatement<?> stmt = (SetStatement<?>) SqlParser.createStatement("SET TIME ZONE DEFAULT");
        assertThat(stmt.scope()).isEqualTo(SetStatement.Scope.TIME_ZONE);
        assertThat(stmt.assignments()).hasSize(1);
        assertThat(stmt.assignments().get(0).expressions().get(0)).hasToString("'DEFAULT'");
    }

    @Test
    public void testResetGlobalStmtBuilder() {
        printStatement("reset global some_setting['nested'], other_setting");
    }

    @Test
    public void testAlterTableStmtBuilder() {
        printStatement("alter table t add foo integer");
        printStatement("alter table t add foo['1']['2'] integer");
        printStatement("alter table t add foo integer null");

        printStatement("alter table t set (number_of_replicas=4)");
        printStatement("alter table schema.t set (number_of_replicas=4)");
        printStatement("alter table t reset (number_of_replicas)");
        printStatement("alter table t reset (property1, property2, property3)");

        printStatement("alter table t add foo integer");
        printStatement("alter table t add column foo integer");
        printStatement("alter table t add column foo integer null");
        printStatement("alter table t add foo integer primary key");
        printStatement("alter table t add foo string index using fulltext");
        printStatement("alter table t add column foo['x'] integer");
        printStatement("alter table t add column foo integer");

        printStatement("alter table t add column foo['x'] integer");
        printStatement("alter table t add column foo['x']['y'] object as (z integer)");

        printStatement("alter table t drop foo");
        printStatement("alter table t drop column foo");
        printStatement("alter table t drop if exists foo");
        printStatement("alter table t drop column if exists foo");
        printStatement("alter table t drop column if exists foo, drop if exists bar");
        printStatement("alter table t drop column if exists foo");
        printStatement("alter table t drop foo, drop column if exists bar");

        printStatement("alter table t drop column foo['x']");
        printStatement("alter table t drop if exists foo['x']['y']");

        printStatement("alter table t rename foo to bar");
        printStatement("alter table t rename foo['x'] to foo['y']");
        printStatement("alter table t rename column foo to bar");

        printStatement("alter table t partition (partitioned_col=1) set (number_of_replicas=4)");
        printStatement("alter table only t set (number_of_replicas=4)");
    }

    @Test
    public void test_alter_table_multiple_columns_stmt_builder() {
        // Some without 'COLUMN'
        printStatement(
            "alter table t add int_col integer not null, " +
            "add obj['1']['2'] integer constraint leaf_check check (obj['1']['2'] > 10), " +
            "add str_col string index using fulltext"
        );
        // Some with 'COLUMN'
        printStatement("alter table t add column foo integer primary key, add column bar long primary key");

        // Mix of multiple ADD COLUMN with/without 'COLUMN'
        printStatement("alter table t add column col1 INTEGER, add col2 TEXT");
    }

    @Test
    public void testCreateTableStmtBuilder() {
        printStatement("create table if not exists t (id integer primary key, name string)");
        printStatement("create table t (id double precision)");
        printStatement("create table t (id double precision null)");
        printStatement("create table t (id character varying)");
        printStatement("create table t (id integer primary key, value array(double precision))");
        printStatement("create table t (id integer, value double precision not null)");
        printStatement("create table t (id integer primary key, name string)");
        printStatement("create table t (id integer primary key, name string null)");
        printStatement("create table t (id integer primary key, name string) clustered into 3 shards");
        printStatement("create table t (id integer primary key, name string null) clustered into 3 shards");
        printStatement("create table t (id integer primary key, name string) clustered into ? shards");
        printStatement("create table t (id integer primary key, name string) clustered into CAST('123' AS int) shards");
        printStatement("create table t (id integer primary key, name string) clustered by (id)");
        printStatement("create table t (id integer primary key, name string) clustered by (id) into 4 shards");
        printStatement("create table t (id integer primary key, name string) clustered by (id) into ? shards");
        printStatement("create table t (id integer primary key, name string) clustered by (id) into ?::int shards");
        printStatement("create table t (id integer primary key, name string) with (number_of_replicas=4)");
        printStatement("create table t (id integer primary key, name string) with (number_of_replicas=?)");
        printStatement("create table t (id integer primary key, name string) clustered by (id) with (number_of_replicas=4)");
        printStatement("create table t (id integer primary key, name string) clustered by (id) into 999 shards with (number_of_replicas=4)");
        printStatement("create table t (id integer primary key, name string) with (number_of_replicas=-4)");
        printStatement("create table t (id integer constraint my_constraint_1 check (id > 0) constraint my_constraint_2 primary key not null)");
        printStatement("create table t (id integer, constraint my_constraint_1 primary key (id))");
        printStatement("create table t (o object(dynamic) as (i integer, d double))");
        printStatement("create table t (id integer, name string, primary key (id))");
        printStatement("create table t (id integer, name string null, primary key (id))");
        printStatement("create table t (" +
            "  \"_i\" integer, " +
            "  \"in\" int," +
            "  \"Name\" string, " +
            "  bo boolean," +
            "  \"by\" byte," +
            "  sh short," +
            "  lo long," +
            "  fl float," +
            "  do double," +
            "  \"ip_\" ip," +
            "  ti timestamp with time zone," +
            "  ob object" +
            ")");
        printStatement("create table \"TABLE\" (o object(dynamic))");
        printStatement("create table \"TABLE\" (o object(strict))");
        printStatement("create table \"TABLE\" (o object(ignored))");
        printStatement("create table \"TABLE\" (o object(strict) as (inner_col object as (sub_inner_col timestamp with time zone, another_inner_col string)))");

        printStatement("create table test (col1 int, col2 timestamp with time zone not null)");
        printStatement("create table test (col1 int primary key not null, col2 timestamp with time zone)");

        printStatement("create table t (" +
            "name string index off, " +
            "another string index using plain, " +
            "\"full\" string index using fulltext," +
            "analyzed string index using fulltext with (analyzer='german', param=?, list=[1,2,3])" +
            ")");
        printStatement("create table test (col1 string, col2 string," +
            "index \"_col1_ft\" using fulltext(col1))");
        printStatement("create table test (col1 string, col2 string," +
            "index col1_col2_ft using fulltext(col1, col2) with (analyzer='custom'))");

        printStatement("create table test (col1 int, col2 timestamp with time zone) partitioned by (col1)");
        printStatement("create table test (col1 int, col2 timestamp with time zone) partitioned by (col1, col2)");
        printStatement("create table test (col1 int, col2 timestamp with time zone) partitioned by (col1) clustered by (col2)");
        printStatement("create table test (col1 int, col2 timestamp with time zone) clustered by (col2) partitioned by (col1)");
        printStatement("create table test (col1 int, col2 object as (col3 timestamp with time zone)) partitioned by (col2['col3'])");
        printStatement("create table test (col1 object as (col3 timestamp without time zone))");
        printStatement("create table test (col1 int, col2 timestamp without time zone not null)");

        printStatement("create table test (col1 string storage with (columnstore = false))");
    }

    @Test
    public void testCreateTableAs() {
        printStatement("create table test as select * from created");
        printStatement("create table if not exists test  as Select * FROM created");
    }

    @Test
    public void test_named_primary_key_constraint_without_name_is_not_allowed() {
        assertThatThrownBy(
            () -> printStatement("create table t (a int CONSTRAINT primary key)"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:34: no viable alternative at input 'CONSTRAINT primary key'");
        assertThatThrownBy(
            () -> printStatement("create table t (a int, CONSTRAINT primary key (a))"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:35: no viable alternative at input 'CONSTRAINT primary key'");
    }

    @Test
    public void testCreateTableColumnTypeOrGeneratedExpressionAreDefined() {
        assertThatThrownBy(
            () -> printStatement("create table test (col1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column [col1]: data type needs to be provided or " +
                        "column should be defined as a generated expression");
    }

    @Test
    public void testCreateTableDefaultExpression() {
        printStatement("create table test (col1 int default 1)");
        printStatement("create table test (col1 int default random())");
        printStatement("create table test (col1 int not null default 1)");
    }

    @Test
    public void testCreateTableBothDefaultAndGeneratedExpressionsNotAllowed() {
        assertThatThrownBy(
            () -> printStatement("create table test (col1 int default random() as 1+1)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column [col1]: the default and generated expressions are mutually exclusive");
    }

    @Test
    public void test_create_table_column_with_duplicated_constraints_is_not_allowed() {
        var constraintDefinitions = List.of(
            "INDEX OFF",
            "DEFAULT 1",
            "GENERATED ALWAYS AS 1+1",
            "PRIMARY KEY",
            "STORAGE WITH(foobar=1)"
        );
        for (var constraintDefinition : constraintDefinitions) {
            assertThatThrownBy(
                () -> printStatement("CREATE TABLE test (col1 INT " + constraintDefinition + " " + constraintDefinition + ")"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Column [col1]: multiple")
                .hasMessageEndingWith("constraints found");
        }
    }

    @Test
    public void test_create_table_column_with_allowed_duplicated_constraints() {
        printStatement("CREATE TABLE test (col1 INT NULL NULL)");
        printStatement("CREATE TABLE test (col1 INT NOT NULL NOT NULL)");
    }

    @Test
    public void test_alter_table_add_column_with_duplicated_constraints_is_not_allowed() {
        var constraintDefinitions = List.of(
            "INDEX OFF",
            "DEFAULT 1",
            "GENERATED ALWAYS AS 1+1",
            "PRIMARY KEY",
            "STORAGE WITH(foobar=1)"
        );
        for (var constraintDefinition : constraintDefinitions) {
            assertThatThrownBy(
                () -> printStatement("ALTER TABLE test ADD COLUMN col1 INT " + constraintDefinition + " " + constraintDefinition))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Column [\"col1\"]: multiple")
                .hasMessageEndingWith("constraints found");
        }
    }

    @Test
    public void test_alter_table_add_column_with_ignored_duplicated_constraints() {
        printStatement("ALTER TABLE test ADD COLUMN col1 INT NULL NULL");
        printStatement("ALTER TABLE test ADD COLUMN col1 INT NOT NULL NOT NULL");
    }

    @Test
    public void testCreateTableOptionsMultipleTimesNotAllowed() {
        assertThatThrownBy(
            () -> printStatement(
                "create table test (col1 int, col2 timestamp with time zone) partitioned by (col1) partitioned by (col2)"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:83: mismatched input 'partitioned' expecting {<EOF>, ';'}");
    }

    @Test
    public void testBlobTable() {
        printStatement("drop blob table screenshots");

        printStatement("create blob table screenshots");
        printStatement("create blob table screenshots clustered into 5 shards");
        printStatement("create blob table screenshots with (number_of_replicas=3)");
        printStatement("create blob table screenshots with (number_of_replicas='0-all')");
        printStatement("create blob table screenshots clustered into 5 shards with (number_of_replicas=3)");

        printStatement("alter blob table screenshots set (number_of_replicas=3)");
        printStatement("alter blob table screenshots set (number_of_replicas='0-all')");
        printStatement("alter blob table screenshots reset (number_of_replicas)");

        assertThatThrownBy(() -> printStatement("alter blob table notblob.screenshots reset (number_of_replicas)"))
            .isExactlyInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> printStatement("alter blob table this.isnota.tablename reset (number_of_replicas)"))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCreateAnalyzerStmtBuilder() {
        printStatement("create analyzer myAnalyzer ( tokenizer german )");
        printStatement("create analyzer my_analyzer (" +
            " token_filters (" +
            "   filter_1," +
            "   filter_2," +
            "   filter_3 WITH (" +
            "     \"key\"=?" +
            "   )" +
            " )," +
            " tokenizer my_tokenizer WITH (" +
            "   property='value'," +
            "   property_list=['l', 'i', 's', 't']" +
            " )," +
            " char_filters (" +
            "   filter_1," +
            "   filter_2 WITH (" +
            "     key='property'" +
            "   )," +
            "   filter_3" +
            " )" +
            ")");
        printStatement("create analyzer \"My_Builtin\" extends builtin WITH (" +
            "  over='write'" +
            ")");
    }

    @Test
    public void testDropAnalyzer() {
        printStatement("drop analyzer my_analyzer");
    }

    @Test
    public void testCreateUserStmtBuilder() {
        printStatement("create user \"Günter\"");
        printStatement("create user root");
        printStatement("create user foo with (password = 'foo')");
        printStatement("create user foo with password 'foo'");
        printStatement("create user foo with password ?");
    }

    @Test
    public void test_create_role_statement() {
        // No option
        printStatement("create role admin");
        // Single option
        printStatement("create role admin with inherit password 'foo'");
        printStatement("create role admin inherit");
        printStatement("create role admin InheRit");

        printStatement("create role admin inherit password 'foo' login");
        printStatement("create role admin inherit password ? login");
    }

    @Test
    public void testDropUserStmtBuilder() {
        printStatement("drop user \"Günter\"");
        printStatement("drop user root");
        printStatement("drop user if exists root");
    }

    @Test
    public void testDropRoleStmtBuilder() {
        printStatement("drop role \"Günter\"");
        printStatement("drop role root");
        printStatement("drop role if exists root");
    }

    @Test
    public void testGrantPrivilegeStmtBuilder() {
        printStatement("grant DML To \"Günter\"");
        printStatement("grant DQL, DDL to root");
        printStatement("grant DQL, DDL to root, wolfie, anna");
        printStatement("grant ALL PRIVILEGES to wolfie");
        printStatement("grant ALL PRIVILEGES to wolfie, anna");
        printStatement("grant ALL to wolfie, anna");
        printStatement("grant ALL to anna");
        printStatement("grant role1, role2 to anna, trillian");
    }

    @Test
    public void testGrantOnSchemaPrivilegeStmtBuilder() {
        printStatement("grant DML ON SCHEMA my_schema To \"Günter\"");
        printStatement("grant DQL, DDL ON SCHEMA my_schema to root");
        printStatement("grant DQL, DDL ON SCHEMA my_schema to root, wolfie, anna");
        printStatement("grant ALL PRIVILEGES ON SCHEMA my_schema to wolfie");
        printStatement("grant ALL PRIVILEGES ON SCHEMA my_schema to wolfie, anna");
        printStatement("grant ALL ON SCHEMA my_schema to wolfie, anna");
        printStatement("grant ALL ON SCHEMA my_schema to anna");
        printStatement("grant ALL ON SCHEMA my_schema, banana, tree to anna, nyan, cat");
    }

    @Test
    public void testGrantOnTablePrivilegeStmtBuilder() {
        printStatement("grant DML ON TABLE my_schema.t To \"Günter\"");
        printStatement("grant DQL, DDL ON TABLE my_schema.t to root");
        printStatement("grant DQL, DDL ON TABLE my_schema.t to root, wolfie, anna");
        printStatement("grant ALL PRIVILEGES ON TABLE my_schema.t to wolfie");
        printStatement("grant ALL PRIVILEGES ON TABLE my_schema.t to wolfie, anna");
        printStatement("grant ALL ON TABLE my_schema.t to wolfie, anna");
        printStatement("grant ALL ON TABLE my_schema.t to anna");
        printStatement("grant ALL ON TABLE my_schema.t, banana.b, tree to anna, nyan, cat");
    }

    @Test
    public void testDenyPrivilegeStmtBuilder() {
        printStatement("deny DML To \"Günter\"");
        printStatement("deny DQL, DDL to root");
        printStatement("deny DQL, DDL to root, wolfie, anna");
        printStatement("deny ALL PRIVILEGES to wolfie");
        printStatement("deny ALL PRIVILEGES to wolfie, anna");
        printStatement("deny ALL to wolfie, anna");
        printStatement("deny ALL to anna");
        printStatement("deny dml to anna");
        printStatement("deny ddl, dql to anna");
    }

    @Test
    public void testDenyOnSchemaPrivilegeStmtBuilder() {
        printStatement("deny DML ON SCHEMA my_schema To \"Günter\"");
        printStatement("deny DQL, DDL ON SCHEMA my_schema to root");
        printStatement("deny DQL, DDL ON SCHEMA my_schema to root, wolfie, anna");
        printStatement("deny ALL PRIVILEGES ON SCHEMA my_schema to wolfie");
        printStatement("deny ALL PRIVILEGES ON SCHEMA my_schema to wolfie, anna");
        printStatement("deny ALL ON SCHEMA my_schema to wolfie, anna");
        printStatement("deny ALL ON SCHEMA my_schema to anna");
        printStatement("deny ALL ON SCHEMA my_schema, banana, tree to anna, nyan, cat");
    }

    @Test
    public void testDenyOnTablePrivilegeStmtBuilder() {
        printStatement("deny DML ON TABLE my_schema.t To \"Günter\"");
        printStatement("deny DQL, DDL ON TABLE my_schema.t to root");
        printStatement("deny DQL, DDL ON TABLE my_schema.t to root, wolfie, anna");
        printStatement("deny ALL PRIVILEGES ON TABLE my_schema.t to wolfie");
        printStatement("deny ALL PRIVILEGES ON TABLE my_schema.t to wolfie, anna");
        printStatement("deny ALL ON TABLE my_schema.t to wolfie, anna");
        printStatement("deny ALL ON TABLE my_schema.t to anna");
        printStatement("deny ALL ON TABLE my_schema.t, banana.b, tree to anna, nyan, cat");
    }

    @Test
    public void testRevokePrivilegeStmtBuilder() {
        printStatement("revoke DML from \"Günter\"");
        printStatement("revoke DQL, DDL from root");
        printStatement("revoke DQL, DDL from root, wolfie, anna");
        printStatement("revoke ALL PRIVILEGES from wolfie");
        printStatement("revoke ALL from wolfie");
        printStatement("revoke ALL PRIVILEGES from wolfie, anna, herald");
        printStatement("revoke ALL from wolfie, anna, herald");
        printStatement("revoke role1, role2 from anna, trillian");
    }

    @Test
    public void testRevokeOnSchemaPrivilegeStmtBuilder() {
        printStatement("revoke DML ON SCHEMA my_schema from \"Günter\"");
        printStatement("revoke DQL, DDL ON SCHEMA my_schema from root");
        printStatement("revoke DQL, DDL ON SCHEMA my_schema from root, wolfie, anna");
        printStatement("revoke ALL PRIVILEGES ON SCHEMA my_schema from wolfie");
        printStatement("revoke ALL ON SCHEMA my_schema from wolfie");
        printStatement("revoke ALL PRIVILEGES ON SCHEMA my_schema from wolfie, anna, herald");
        printStatement("revoke ALL ON SCHEMA my_schema from wolfie, anna, herald");
        printStatement("revoke ALL ON SCHEMA my_schema, banana, tree from anna, nyan, cat");
    }

    @Test
    public void testRevokeOnTablePrivilegeStmtBuilder() {
        printStatement("revoke DML ON TABLE my_schema.t from \"Günter\"");
        printStatement("revoke DQL, DDL ON TABLE my_schema.t from root");
        printStatement("revoke DQL, DDL ON TABLE my_schema.t from root, wolfie, anna");
        printStatement("revoke ALL PRIVILEGES ON TABLE my_schema.t from wolfie");
        printStatement("revoke ALL ON TABLE my_schema.t from wolfie");
        printStatement("revoke ALL PRIVILEGES ON TABLE my_schema.t from wolfie, anna, herald");
        printStatement("revoke ALL ON TABLE my_schema.t from wolfie, anna, herald");
        printStatement("revoke ALL ON TABLE my_schema.t, banana.b, tree from anna, nyan, cat");
    }

    @Test
    public void testCreateFunctionStmtBuilder() {
        printStatement("create function foo.bar() returns boolean language ? as ?");
        printStatement("create function foo.bar() returns boolean language $1 as $2");

        // create or replace function
        printStatement("create function foo.bar(int, long)" +
            " returns int" +
            " language javascript" +
            " as 'function(a, b) {return a + b}'");
        printStatement("create function bar(array(int))" +
            " returns array(int) " +
            " language javascript" +
            " as 'function(a) {return [a]}'");
        printStatement("create function bar()" +
            " returns string" +
            " language javascript" +
            " as 'function() {return \"\"}'");
        printStatement("create or replace function bar()" +
            " returns string" +
            " language javascript as 'function() {return \"1\"}'");

        // argument with names
        printStatement("create function foo.bar(\"f\" int, s object)" +
            " returns object" +
            " language javascript as 'function(f, s) {return {\"a\": 1}}'");
        printStatement("create function foo.bar(location geo_point, geo_shape)" +
            " returns boolean" +
            " language javascript as 'function(location, b) {return true;}'");
    }

    @Test
    public void testCreateFunctionStmtBuilderWithIncorrectFunctionName() {
        assertThatThrownBy(
            () -> printStatement("create function foo.bar.a() " +
                             "returns object " +
                             "language sql as 'select 1'"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("The function name is not correct! name [foo.bar.a] " +
                "does not conform the [[schema_name .] function_name] format.");
    }

    @Test
    public void testDropFunctionStmtBuilder() {
        printStatement("drop function bar(int)");
        printStatement("drop function foo.bar(obj object)");
        printStatement("drop function if exists foo.bar(obj object)");
    }

    @Test
    public void testSelectStmtBuilder() {
        printStatement("select ab" +
            " from (select (ii + y) as iiy, concat(a, b) as ab" +
                " from (select t1.a, t2.b, t2.y, (t1.i + t2.i) as ii " +
                    " from t1, t2 where t1.a='a' or t2.b='aa') as t)" +
            " as tt order by iiy");
        printStatement("select extract(day from x) from y");
        printStatement("select * from foo order by 1, 2 limit all offset ?");
        printStatement("select * from foo order by 1, 2 fetch first 10 rows only offset ?");
        printStatement("select * from foo order by 1, 2 limit null offset ?");
        printStatement("select * from foo order by 1, 2 fetch next null row only offset ?");
        printStatement("select * from foo order by 1, 2 limit 1 offset ?");
        printStatement("select * from foo order by 1, 2 limit 1 offset ? row");
        printStatement("select * from foo order by 1, 2 limit 1 offset null row");
        printStatement("select * from foo order by 1, 2 limit all offset ? rows");
        printStatement("select * from foo order by 1, 2 limit all offset null rows");
        printStatement("select * from foo order by 1, 2 fetch first ? row only offset null rows");
        printStatement("select * from foo order by 1, 2 offset 10 rows limit all");
        printStatement("select * from foo order by 1, 2 offset 10 limit 5");
        printStatement("select * from foo order by 1, 2 offset '120'::int limit ?::short");
        printStatement("select * from foo order by 1, 2 offset CAST(? AS long) limit '15'::int");
        printStatement("select * from foo a (x, y, z)");
        printStatement("select *, 123, * from foo");
        printStatement("select show from foo");
        printStatement("select extract(day from x), extract('day' from x) from y");

        printStatement("select 1 + 13 || '15' from foo");
        printStatement("select \"test\" from foo");
        printStatement("select col['x'] + col['y'] from foo");
        printStatement("select col['x'] - col['y'] from foo");
        printStatement("select col['y'] / col[2 / 1] from foo");
        printStatement("select col[1] from foo");

        printStatement("select - + 10");
        printStatement("select - ( - - 10)");
        printStatement("select - ( + - 10) * - ( - 10 - + 10)");
        printStatement("select - - col['x']");

        // expressions as subscript index are only supported by the parser
        printStatement("select col[1 + 2] - col['y'] from foo");

        printStatement("select x is distinct from y from foo where a is not distinct from b");

        printStatement("select * from information_schema.tables");

        printStatement("select * from a.b.c@d");

        printStatement("select \"TOTALPRICE\" \"my price\" from \"orders\"");

        printStatement("select * from foo limit 100 offset 20");
        printStatement("select * from foo limit null offset 20");
        printStatement("select * from foo limit all offset 20 row");
        printStatement("select * from foo limit 100 offset 20 rows");
        printStatement("select * from foo offset 20");
        printStatement("select * from foo offset null");
        printStatement("select * from foo offset 20 row");
        printStatement("select * from foo offset null row");
        printStatement("select * from foo offset 20 rows");
        printStatement("select * from foo offset null rows");

        printStatement("select * from t where 'value' LIKE ANY (col)");
        printStatement("select * from t where 'value' NOT LIKE ANY (col)");
        printStatement("select * from t where 'source' ~ 'pattern'");
        printStatement("select * from t where 'source' !~ 'pattern'");
        printStatement("select * from t where source_column ~ pattern_column");
        printStatement("select * from t where ? !~ ?");
    }

    @Test
    public void test_bitwise_expression() {
        printStatement("select 1 | 2 & 3 # 4");
    }

    @Test
    public void testIntervalLiteral() {
        printStatement("select interval '1' HOUR");
    }

    @Test
    public void test_number_underscope_support() throws Exception {
        printStatement("select 1_000_000");
        printStatement("select 1_000.00");
        printStatement("select 1_00_0_0.00e3");
    }

    @Test
    public void testEscapedStringLiteralBuilder() {
        printStatement("select E'aValue'");
        printStatement("select E'\\141Value'");
        printStatement("select e'aa\\\'bb'");
    }

    @Test
    public void testBitString() {
        printStatement("select B'0010'");
    }

    @Test
    public void testThatEscapedStringLiteralContainingDoubleBackSlashAndSingleQuoteThrowsException() {
        assertThatThrownBy(
            () -> printStatement("select e'aa\\\\\'bb' as col1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid Escaped String Literal aa\\\\'bb");
    }

    @Test
    public void testTrimFunctionStmtBuilder() {
        printStatement("SELECT trim(LEADING ' ' FROM  x) FROM t");
        printStatement("SELECT trim(' ' FROM  x) FROM t");
        printStatement("SELECT trim(FROM  x) FROM t");
        printStatement("SELECT trim(x) FROM t");
    }

    @Test
    public void test_substring_can_be_used_as_concrete_and_generic_function() {
        printStatement("SELECT substring('foo' FROM 1)");
        printStatement("SELECT substring('foo' FROM 1 FOR 3)");
        printStatement("SELECT substring('foo', 1)");
        printStatement("SELECT substring('foo', 1, 3)");
    }

    @Test
    public void testSystemInformationFunctionsStmtBuilder() {
        printStatement("select current_schema");
        printStatement("select current_schema()");
        printStatement("select pg_catalog.current_schema()");
        printStatement("select * from information_schema.tables where table_schema = current_schema");
        printStatement("select * from information_schema.tables where table_schema = current_schema()");
        printStatement("select * from information_schema.tables where table_schema = pg_catalog.current_schema()");

        printStatement("select current_database()");
        printStatement("select pg_catalog.current_database()");

        printStatement("select current_catalog");
        printStatement("select current_user");
        printStatement("select current_role");
        printStatement("select user");
        printStatement("select session_user");
    }

    @Test
    public void testStatementBuilderTpch() throws Exception {
        printTpchQuery(1, 3);
        printTpchQuery(2, 33, "part type like", "region name");
        printTpchQuery(3, "market segment", "2013-03-05");
        printTpchQuery(4, "2013-03-05");
        printTpchQuery(5, "region name", "2013-03-05");
        printTpchQuery(6, "2013-03-05", 33, 44);
        printTpchQuery(7, "nation name 1", "nation name 2");
        printTpchQuery(8, "nation name", "region name", "part type");
        printTpchQuery(9, "part name like");
        printTpchQuery(10, "2013-03-05");
        printTpchQuery(11, "nation name", 33);
        printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
        printTpchQuery(13, "comment like 1", "comment like 2");
        printTpchQuery(14, "2013-03-05");
        // query 15: views not supported
        printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
        printTpchQuery(17, "part brand", "part container");
        printTpchQuery(18, 33);
        printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
        printTpchQuery(20, "part name like", "2013-03-05", "nation name");
        printTpchQuery(21, "nation name");
        printTpchQuery(22,
            "phone 1",
            "phone 2",
            "phone 3",
            "phone 4",
            "phone 5",
            "phone 6",
            "phone 7");
    }

    @Test
    public void testShowTransactionLevel() {
        printStatement("show transaction isolation level");
    }

    @Test
    public void testArrayConstructorStmtBuilder() {
        printStatement("select []");
        printStatement("select [ARRAY[1]]");
        printStatement("select ARRAY[]");
        printStatement("select ARRAY[1, 2]");
        printStatement("select ARRAY[ARRAY[1,2], ARRAY[2]]");
        printStatement("select ARRAY[ARRAY[1,2], [2]]");
        printStatement("select ARRAY[ARRAY[1,2], ?]");

        printStatement("select ARRAY[1 + 2]");
        printStatement("select ARRAY[ARRAY[col, 2 + 3], [col]]");
        printStatement("select [ARRAY[1 + 2, ?], [1 + 2]]");
        printStatement("select ARRAY[col_a IS NULL, col_b IS NOT NULL]");
    }

    @Test
    public void test_cast_with_pg_array_type_definition() {
        printStatement("SELECT '{a,b,c}'::text[]");
    }

    @Test
    public void testArrayConstructorSubSelectBuilder() {
        printStatement("select array(select foo from f1) from f2");
        printStatement("select array(select * from f1) as array1 from f2");
        printStatement("select count(*) from f1 where f1.array1 = array(select foo from f2)");
    }


    @Test
    public void testArrayConstructorSubSelectBuilderNoParenthesisThrowsParsingException() {
        assertThatThrownBy(
            () -> printStatement("select array from f2"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:14: no viable alternative at input 'select array from'");
    }

    @Test
    public void testArrayConstructorSubSelectBuilderNoSubQueryThrowsParsingException() {
        assertThatThrownBy(
            () -> printStatement("select array() as array1 from f2"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:14: no viable alternative at input 'select array()'");
    }

    @Test
    public void testTableFunctions() {
        printStatement("select * from unnest([1, 2], ['Arthur', 'Marvin'])");
        printStatement("select * from unnest(?, ?)");
        printStatement("select * from open('/tmp/x')");
        printStatement("select * from x.y()");
    }

    @Test
    public void testStatementSubscript() {
        printStatement("select a['x'] from foo where a['x']['y']['z'] = 1");
        printStatement("select a['x'] from foo where a[1 + 2]['y'] = 1");
    }

    @Test
    public void testCopy() {
        printStatement("copy foo partition (a='x') from ?");
        printStatement("copy foo partition (a={key='value'}) from ?");
        printStatement("copy foo from '/folder/file.extension'");
        printStatement("copy foo from ?");
        printStatement("copy foo (a,b) from ?");
        printStatement("copy foo from ? with (some_property=1)");
        printStatement("copy foo from ? with (some_property=false)");
        printStatement("copy schemah.foo from '/folder/file.extension'");
        printStatement("copy schemah.foo from '/folder/file.extension' return summary");
        printStatement("copy schemah.foo from '/folder/file.extension' with (some_property=1) return summary");

        printStatement("copy foo (nae) to '/folder/file.extension'");
        printStatement("copy foo to '/folder/file.extension'");
        printStatement("copy foo to DIRECTORY '/folder'");
        printStatement("copy foo to DIRECTORY ?");
        printStatement("copy foo to DIRECTORY '/folder' with (some_param=4)");
        printStatement("copy foo partition (a='x') to DIRECTORY '/folder' with (some_param=4)");
        printStatement("copy foo partition (a=?) to DIRECTORY '/folder' with (some_param=4)");

        printStatement("copy foo where a = 'x' to DIRECTORY '/folder'");
    }

    @Test
    public void testInsertStmtBuilder() {
        // insert from values
        printStatement("insert into foo (id, name) values ('string', 1.2)");
        printStatement("insert into foo values ('string', NULL)");
        printStatement("insert into foo (id, name) values ('string', 1.2), (abs(-4), 4+?)");
        printStatement("insert into schemah.foo (id, name) values ('string', 1.2)");

        printStatement("insert into t (a, b) values (1, 2) on conflict do nothing");
        printStatement("insert into t (a, b) values (1, 2) on conflict (a,b) do nothing");
        printStatement("insert into t (a, b) values (1, 2) on conflict (o['id'], b) do nothing");
        printStatement("insert into t (a, b) values (1, 2) on conflict (a, o['x']['y']) do nothing");
        printStatement("insert into t (a, b) values (1, 2) on conflict (a) do update set b = b + 1");
        printStatement("insert into t (a, b, c) values (1, 2, 3) on conflict (a, b) do update set a = a + 1, b = 3");
        printStatement("insert into t (a, b, c) values (1, 2), (3, 4) on conflict (c) do update set a = excluded.a + 1, b = 4");
        printStatement("insert into t (a, b, c) values (1, 2), (3, 4) on conflict (c) do update set a = excluded.a + 1, b = excluded.b - 2");

        @SuppressWarnings("unchecked")
        Insert<Expression> insert = (Insert<Expression>) SqlParser.createStatement(
                "insert into test_generated_column (id, ts) values (?, ?) on conflict (id) do update set ts = ?");
        Assignment<Expression> onDuplicateAssignment = insert.duplicateKeyContext().getAssignments().get(0);
        assertThat(onDuplicateAssignment.expression()).isExactlyInstanceOf(ParameterExpression.class);
        assertThat(onDuplicateAssignment.expressions().get(0)).hasToString("$3");

        // insert from query
        printStatement("insert into foo (id, name) select id, name from bar order by id");
        printStatement("insert into foo (id, name) select * from bar limit 3 offset 10");
        printStatement("insert into foo (id, name) select * from bar fetch first 3 rows only offset 10");
        printStatement("insert into foo (id, name) select * from bar limit null offset 10");
        printStatement("insert into foo (id, name) select * from bar limit all offset 10 row");
        printStatement("insert into foo (id, name) select * from bar limit 3 offset 10 rows");
        printStatement("insert into foo (wealth, name) select sum(money), name from bar group by name");
        printStatement("insert into foo select sum(money), name from bar group by name");

        printStatement("insert into foo (id, name) (select id, name from bar order by id)");
        printStatement("insert into foo (id, name) (select * from bar limit all offset 10)");
        printStatement("insert into foo (id, name) (select * from bar limit 3 offset 10 row)");
        printStatement("insert into foo (id, name) (select * from bar limit 3 offset 10 rows)");
        printStatement("insert into foo (id, name) (select * from bar limit 3 offset null rows)");
        printStatement("insert into foo (wealth, name) (select sum(money), name from bar group by name)");
        printStatement("insert into foo (select sum(money), name from bar group by name)");
    }

    @Test
    public void test_insert_returning() {
        printStatement("insert into foo (id, name) values ('string', 1.2) returning id");
        printStatement("insert into foo (id, name) values ('string', 1.2) returning id as foo");
        printStatement("insert into foo (id, name) values ('string', 1.2) returning *");
        printStatement("insert into foo (id, name) values ('string', 1.2) returning foo.*");
    }

    @Test
    public void test_using_fqn_in_column_list_of_insert_into_results_in_user_friendly_error() {
        assertThatThrownBy(() -> printStatement("insert into tbl (t.x) values (1)"))
            .hasMessage("Column references used in INSERT INTO <tbl> (...) must use the column name. They cannot qualify catalog, schema or table. Got `t.x`");
    }


    @Test
    public void testParameterExpressionLimitOffset() {
        // ORMs like SQLAlchemy generate these kind of queries.
        printStatement("select * from foo limit ? offset ?");
        printStatement("select * from foo limit ? offset ? row");
        printStatement("select * from foo limit ? offset ? rows");
    }

    @Test
    public void testMatchPredicateStmtBuilder() {
        printStatement("select * from foo where match (a['1']['2'], 'abc')");
        printStatement("select * from foo where match (a, 'abc')");
        printStatement("select * from foo where match ((a, b 2.0), 'abc')");
        printStatement("select * from foo where match ((a ?, b 2.0), ?)");
        printStatement("select * from foo where match ((a ?, b 2.0), {type= 'Point', coordinates= [0.0,0.0] })");
        printStatement("select * from foo where match ((a 1, b 2.0), 'abc') using best_fields");
        printStatement("select * from foo where match ((a 1, b 2.0), 'abc') using best_fields with (prop=val, foo=1)");
        printStatement("select * from foo where match (a, (select shape from countries limit 1))");
    }

    @Test
    public void testRepositoryStmtBuilder() {
        printStatement("create repository my_repo type s3");
        printStatement("CREATE REPOSITORY \"myRepo\" TYPE \"fs\"");
        printStatement("CREATE REPOSITORY \"myRepo\" TYPE \"fs\" with (location='/mount/backups/my_backup', compress=True)");
        Statement statement = SqlParser.createStatement("CREATE REPOSITORY my_repo type s3 with (location='/mount/backups/my_backup')");
        assertThat(statement).hasToString("CreateRepository{" +
                                            "repository=my_repo, " +
                                            "type=s3, " +
                                            "properties={location='/mount/backups/my_backup'}}");

        printStatement("DROP REPOSITORY my_repo");
        statement = SqlParser.createStatement("DROP REPOSITORY \"myRepo\"");
        assertThat(statement).hasToString("DropRepository{" +
                                            "repository=myRepo}");
    }

    @Test
    public void testCreateSnapshotStmtBuilder() {
        printStatement("CREATE SNAPSHOT my_repo.my_snapshot ALL");
        printStatement("CREATE SNAPSHOT my_repo.my_snapshot TABLE authors, books");
        printStatement("CREATE SNAPSHOT my_repo.my_snapshot TABLE authors, books with (wait_for_completion=True)");
        printStatement("CREATE SNAPSHOT my_repo.my_snapshot ALL with (wait_for_completion=True)");
        Statement statement = SqlParser.createStatement(
            "CREATE SNAPSHOT my_repo.my_snapshot TABLE authors PARTITION (year=2015, year=2014), books");
        assertThat(statement).hasToString("CreateSnapshot{" +
                                            "name=my_repo.my_snapshot, " +
                                            "properties={}, " +
                                            "tables=[Table{only=false, authors, partitionProperties=[" +
                                            "Assignment{column=\"year\", expressions=[2015]}, " +
                                            "Assignment{column=\"year\", expressions=[2014]}]}, " +
                                            "Table{only=false, books, partitionProperties=[]}]}");
    }

    @Test
    public void testDropSnapshotStmtBuilder() {
        Statement statement = SqlParser.createStatement("DROP SNAPSHOT my_repo.my_snapshot");
        assertThat(statement).hasToString("DropSnapshot{name=my_repo.my_snapshot}");
    }

    @Test
    public void testRestoreSnapshotStmtBuilder() {
        printStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
        printStatement("RESTORE SNAPSHOT my_repo.my_snapshot TABLE authors, books");
        printStatement("RESTORE SNAPSHOT my_repo.my_snapshot TABLE authors, books with (wait_for_completion=True)");
        printStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL with (wait_for_completion=True)");
        printStatement("RESTORE SNAPSHOT my_repo.my_snapshot TABLE authors PARTITION (year=2015, year=2014), books");
        Statement statement = SqlParser.createStatement(
            "RESTORE SNAPSHOT my_repo.my_snapshot " +
            "TABLE authors PARTITION (year=2015, year=2014), books " +
            "WITH (wait_for_completion=True)");
        assertThat(statement).hasToString("RestoreSnapshot{" +
                                            "name=my_repo.my_snapshot, " +
                                            "properties={wait_for_completion=true}, " +
                                            "tables=[" +
                                            "Table{only=false, authors, partitionProperties=[" + "" +
                                            "Assignment{column=\"year\", expressions=[2015]}, " +
                                            "Assignment{column=\"year\", expressions=[2014]}]}, " +
                                            "Table{only=false, books, partitionProperties=[]}]}");
        statement = SqlParser.createStatement("RESTORE SNAPSHOT my_repo.my_snapshot ALL");
        assertThat(statement).hasToString("RestoreSnapshot{" +
                                            "name=my_repo.my_snapshot, " +
                                            "properties={}, " +
                                            "tables=[]}");
    }

    @Test
    public void testGeoShapeStmtBuilder() {
        printStatement("create table test (" +
                       "    col1 geo_shape," +
                       "    col2 geo_shape index using geohash" +
                       ")");
        printStatement("create table test(" +
                       "    col1 geo_shape index using quadtree with (precision='1m')" +
                       ")");
        printStatement("create table test(" +
                       "    col1 geo_shape," +
                       "    index geo_shape_i using quadtree(col1) with (precision='1m')" +
                       ")");
        printStatement("create table test(" +
                       "    col1 geo_shape INDEX OFF," +
                       "    index geo_shape_i using quadtree(col1) with (precision='1m')" +
                       ")");
    }

    @Test
    public void testCastStmtBuilder() {
        // double colon cast
        printStatement("select 1+4::integer");
        printStatement("select '2'::integer");
        printStatement("select 1.0::timestamp with time zone");
        printStatement("select 1.0::timestamp without time zone");
        printStatement("select 1+3::string");
        printStatement("select [0,1,5]::array(boolean)");
        printStatement("select field::boolean");
        printStatement("select port['http']::boolean");
        printStatement("select '4'::integer + 4");
        printStatement("select 4::string || ' apples'");
        printStatement("select '-4'::integer");
        printStatement("select -4::string");
        printStatement("select '-4'::integer + 10");
        printStatement("select -4::string || ' apples'");
        // cast
        printStatement("select cast(1+4 as integer) from foo");
        printStatement("select cast('2' as integer) from foo");
        // try cast
        printStatement("select try_cast(y as integer) from foo");
    }

    @Test
    public void test_row_type_access() {
        printStatement("SELECT (obj).col FROM tbl");
        printStatement("SELECT ((obj).x).y FROM tbl");
    }

    @Test
    public void testSubscriptExpression() {
        Expression expression = SqlParser.createExpression("a['sub']");
        assertThat(expression).isExactlyInstanceOf(SubscriptExpression.class);
        SubscriptExpression subscript = (SubscriptExpression) expression;
        assertThat(subscript.index()).isExactlyInstanceOf(StringLiteral.class);
        assertThat(((StringLiteral) subscript.index()).getValue()).isEqualTo("sub");

        assertThat(subscript.base()).isExactlyInstanceOf(QualifiedNameReference.class);

        expression = SqlParser.createExpression("[1,2,3][1]");
        assertThat(expression).isExactlyInstanceOf(SubscriptExpression.class);
        subscript = (SubscriptExpression) expression;
        assertThat(subscript.index()).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(((IntegerLiteral) subscript.index()).getValue()).isEqualTo(1);
        assertThat(subscript.base()).isExactlyInstanceOf(ArrayLiteral.class);
    }

    @Test
    public void testSafeSubscriptExpression() {
        MatchPredicate matchPredicate = (MatchPredicate) SqlParser.createExpression("match (a['1']['2'], 'abc')");
        assertThat(matchPredicate.idents().get(0).columnIdent()).hasToString("\"a\"['1']['2']");

        matchPredicate = (MatchPredicate) SqlParser.createExpression("match (a['1']['2']['4'], 'abc')");
        assertThat(matchPredicate.idents().get(0).columnIdent()).hasToString("\"a\"['1']['2']['4']");

        assertThatThrownBy(
            () -> SqlParser.createExpression("match ([1]['1']['2'], 'abc')"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:8: mismatched input '[' expecting {");
    }

    @Test
    public void testCaseSensitivity() {
        Expression expression = SqlParser.createExpression("\"firstName\" = 'myName'");
        QualifiedNameReference nameRef = (QualifiedNameReference) ((ComparisonExpression) expression).getLeft();
        StringLiteral myName = (StringLiteral) ((ComparisonExpression) expression).getRight();
        assertThat(nameRef.getName().getSuffix()).isEqualTo("firstName");
        assertThat(myName.getValue()).isEqualTo("myName");

        expression = SqlParser.createExpression("FIRSTNAME = 'myName'");
        nameRef = (QualifiedNameReference) ((ComparisonExpression) expression).getLeft();
        assertThat(nameRef.getName().getSuffix()).isEqualTo("firstname");

        expression = SqlParser.createExpression("ABS(1)");
        QualifiedName functionName = ((FunctionCall) expression).getName();
        assertThat(functionName.getSuffix()).isEqualTo("abs");
    }

    @Test
    public void testArrayComparison() {
        Expression anyExpression = SqlParser.createExpression("1 = ANY (arrayColumnRef)");
        assertThat(anyExpression).isExactlyInstanceOf(ArrayComparisonExpression.class);
        ArrayComparisonExpression arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
        assertThat(arrayComparisonExpression.quantifier()).isEqualTo(ArrayComparisonExpression.Quantifier.ANY);
        assertThat(arrayComparisonExpression.getLeft()).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(arrayComparisonExpression.getRight()).isExactlyInstanceOf(QualifiedNameReference.class);

        Expression someExpression = SqlParser.createExpression("1 = SOME (arrayColumnRef)");
        assertThat(someExpression).isExactlyInstanceOf(ArrayComparisonExpression.class);
        ArrayComparisonExpression someArrayComparison = (ArrayComparisonExpression) someExpression;
        assertThat(someArrayComparison.quantifier()).isEqualTo(ArrayComparisonExpression.Quantifier.ANY);
        assertThat(someArrayComparison.getLeft()).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(someArrayComparison.getRight()).isExactlyInstanceOf(QualifiedNameReference.class);

        Expression allExpression = SqlParser.createExpression("'StringValue' = ALL (arrayColumnRef)");
        assertThat(allExpression).isExactlyInstanceOf(ArrayComparisonExpression.class);
        ArrayComparisonExpression allArrayComparison = (ArrayComparisonExpression) allExpression;
        assertThat(allArrayComparison.quantifier()).isEqualTo(ArrayComparisonExpression.Quantifier.ALL);
        assertThat(allArrayComparison.getLeft()).isExactlyInstanceOf(StringLiteral.class);
        assertThat(allArrayComparison.getRight()).isExactlyInstanceOf(QualifiedNameReference.class);
    }

    @Test
    public void testArrayComparisonSubSelect() {
        Expression anyExpression = SqlParser.createExpression("1 = ANY ((SELECT 5))");
        assertThat(anyExpression).isExactlyInstanceOf(ArrayComparisonExpression.class);
        ArrayComparisonExpression arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
        assertThat(arrayComparisonExpression.quantifier()).isEqualTo(ArrayComparisonExpression.Quantifier.ANY);
        assertThat(arrayComparisonExpression.getLeft()).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(arrayComparisonExpression.getRight()).isExactlyInstanceOf(SubqueryExpression.class);

        // It's possible to omit the parenthesis
        anyExpression = SqlParser.createExpression("1 = ANY (SELECT 5)");
        assertThat(anyExpression).isExactlyInstanceOf(ArrayComparisonExpression.class);
        arrayComparisonExpression = (ArrayComparisonExpression) anyExpression;
        assertThat(arrayComparisonExpression.quantifier()).isEqualTo(ArrayComparisonExpression.Quantifier.ANY);
        assertThat(arrayComparisonExpression.getLeft()).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(arrayComparisonExpression.getRight()).isExactlyInstanceOf(SubqueryExpression.class);
    }

    @Test
    public void testArrayLikeExpression() {
        Expression expression = SqlParser.createExpression("'books%' LIKE ANY(race['interests'])");
        assertThat(expression).isExactlyInstanceOf(ArrayLikePredicate.class);
        ArrayLikePredicate arrayLikePredicate = (ArrayLikePredicate) expression;
        assertThat(arrayLikePredicate.inverse()).isFalse();
        assertThat(arrayLikePredicate.getEscape()).isNull();
        assertThat(arrayLikePredicate.getPattern()).hasToString("'books%'");
        assertThat(arrayLikePredicate.getValue()).hasToString("\"race\"['interests']");

        expression = SqlParser.createExpression("'b%' NOT LIKE ANY(race)");
        assertThat(expression).isExactlyInstanceOf(ArrayLikePredicate.class);
        arrayLikePredicate = (ArrayLikePredicate) expression;
        assertThat(arrayLikePredicate.inverse()).isTrue();
        assertThat(arrayLikePredicate.getEscape()).isNull();
        assertThat(arrayLikePredicate.getPattern()).hasToString("'b%'");
        assertThat(arrayLikePredicate.getValue()).hasToString("\"race\"");
    }

    @Test
    public void test_array_overlap_operator() {
        Expression expression = SqlParser.createExpression("[1] && [2]");
        assertThat(expression).isExactlyInstanceOf(FunctionCall.class);
        FunctionCall functionCall = (FunctionCall) expression;
        assertThat(functionCall.getName().toString()).isEqualTo("array_overlap");
        assertThat(functionCall.getArguments()).hasSize(2);
    }

    @Test
    public void testStringLiteral() {
        String[] testString = new String[]{
            "foo' or 1='1",
            "foo''bar",
            "foo\\bar",
            "foo\'bar",
            "''''",
            "''",
            ""
        };
        for (String s : testString) {
            Expression expr = SqlParser.createExpression(Literals.quoteStringLiteral(s));
            assertThat(((StringLiteral) expr).getValue()).isEqualTo(s);
        }
    }

    @Test
    public void testEscapedStringLiteral() {
        String input = "this is a triple-a:\\141\\x61\\u0061";
        String expectedValue = "this is a triple-a:aaa";
        Expression expr = SqlParser.createExpression(Literals.quoteEscapedStringLiteral(input));
        EscapedCharStringLiteral escapedCharStringLiteral = (EscapedCharStringLiteral) expr;
        assertThat(escapedCharStringLiteral.getRawValue()).isEqualTo(input);
        assertThat(escapedCharStringLiteral.getValue()).isEqualTo(expectedValue);
    }

    @Test
    public void testObjectLiteral() {
        Expression emptyObjectLiteral = SqlParser.createExpression("{}");
        assertThat(emptyObjectLiteral)
            .isExactlyInstanceOf(ObjectLiteral.class)
            .extracting(ol -> ((ObjectLiteral) ol).values())
            .asInstanceOf(InstanceOfAssertFactories.MAP)
            .isEmpty();

        ObjectLiteral objectLiteral = (ObjectLiteral) SqlParser.createExpression("{a=1, aa=-1, b='str', c=[], d={}}");
        assertThat(objectLiteral.values()).hasSize(5);
        assertThat(objectLiteral.values().get("a")).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(objectLiteral.values().get("aa")).isExactlyInstanceOf(NegativeExpression.class);
        assertThat(objectLiteral.values().get("b")).isExactlyInstanceOf(StringLiteral.class);
        assertThat(objectLiteral.values().get("c")).isExactlyInstanceOf(ArrayLiteral.class);
        assertThat(objectLiteral.values().get("d")).isExactlyInstanceOf(ObjectLiteral.class);

        ObjectLiteral quotedObjectLiteral = (ObjectLiteral) SqlParser.createExpression("{\"AbC\"=123}");
        assertThat(quotedObjectLiteral.values()).hasSize(1);
        assertThat(quotedObjectLiteral.values().get("AbC")).isExactlyInstanceOf(IntegerLiteral.class);
        assertThat(quotedObjectLiteral.values().get("abc")).isNull();
        assertThat(quotedObjectLiteral.values().get("ABC")).isNull();

        SqlParser.createExpression("{a=func('abc')}");
        SqlParser.createExpression("{b=identifier}");
        SqlParser.createExpression("{c=1+4}");
        SqlParser.createExpression("{d=sub['script']}");
    }

    @Test
    public void testArrayLiteral() {
        ArrayLiteral emptyArrayLiteral = (ArrayLiteral) SqlParser.createExpression("[]");
        assertThat(emptyArrayLiteral.values()).isEmpty();

        ArrayLiteral singleArrayLiteral = (ArrayLiteral) SqlParser.createExpression("[1]");
        assertThat(singleArrayLiteral.values()).hasSize(1);
        assertThat(singleArrayLiteral.values().get(0)).isExactlyInstanceOf(IntegerLiteral.class);

        ArrayLiteral multipleArrayLiteral = (ArrayLiteral) SqlParser.createExpression(
            "['str', -12.56, {}, ['another', 'array']]");
        assertThat(multipleArrayLiteral.values()).hasSize(4);
        assertThat(multipleArrayLiteral.values().get(0)).isExactlyInstanceOf(StringLiteral.class);
        assertThat(multipleArrayLiteral.values().get(1)).isExactlyInstanceOf(NegativeExpression.class);
        assertThat(multipleArrayLiteral.values().get(2)).isExactlyInstanceOf(ObjectLiteral.class);
        assertThat(multipleArrayLiteral.values().get(3)).isExactlyInstanceOf(ArrayLiteral.class);
    }

    @Test
    public void testParameterNode() {
        printStatement("select foo, $1 from foo where a = $2 or a = $3");

        final AtomicInteger counter = new AtomicInteger(0);

        Expression inExpression = SqlParser.createExpression("x in (?, ?, ?)");
        inExpression.accept(new DefaultTraversalVisitor<>() {
            @Override
            public Object visitParameterExpression(ParameterExpression node, Object context) {
                assertThat(node.position()).isEqualTo(counter.incrementAndGet());
                return super.visitParameterExpression(node, context);
            }
        }, null);

        assertThat(counter.get()).isEqualTo(3);
        counter.set(0);

        Expression andExpression = SqlParser.createExpression("a = ? and b = ? and c = $3");
        andExpression.accept(new DefaultTraversalVisitor<>() {
            @Override
            public Object visitParameterExpression(ParameterExpression node, Object context) {
                assertThat(node.position()).isEqualTo(counter.incrementAndGet());
                return super.visitParameterExpression(node, context);
            }
        }, null);
        assertThat(counter.get()).isEqualTo(3);
    }

    @Test
    public void testShowCreateTable() {
        Statement stmt = SqlParser.createStatement("SHOW CREATE TABLE foo");
        assertThat(stmt)
            .isExactlyInstanceOf(ShowCreateTable.class)
            .extracting(s -> ((ShowCreateTable<?>) s).table().getName().toString())
            .isEqualTo("foo");
        stmt = SqlParser.createStatement("SHOW CREATE TABLE my_schema.foo");
        assertThat(stmt)
            .isExactlyInstanceOf(ShowCreateTable.class)
            .extracting(s -> ((ShowCreateTable<?>) s).table().getName().toString())
            .isEqualTo("my_schema.foo");
    }

    @Test
    public void testCreateTableWithGeneratedColumn() {
        printStatement("create table test (col1 int, col2 AS date_trunc('day', col1))");
        printStatement("create table test (col1 int, col2 AS (date_trunc('day', col1)))");
        printStatement("create table test (col1 int, col2 AS date_trunc('day', col1) INDEX OFF)");

        printStatement("create table test (col1 int, col2 GENERATED ALWAYS AS date_trunc('day', col1))");
        printStatement("create table test (col1 int, col2 GENERATED ALWAYS AS (date_trunc('day', col1)))");

        printStatement("create table test (col1 int, col2 string GENERATED ALWAYS AS date_trunc('day', col1))");
        printStatement("create table test (col1 int, col2 string GENERATED ALWAYS AS (date_trunc('day', col1)))");

        printStatement("create table test (col1 int, col2 AS cast(col1 as string))");
        printStatement("create table test (col1 int, col2 AS (cast(col1 as string)))");
        printStatement("create table test (col1 int, col2 AS col1 + 1)");
        printStatement("create table test (col1 int, col2 AS (col1 + 1))");

        printStatement("create table test (col1 int, col2 AS col1['name'] + 1)");
    }

    @Test
    public void testCreateTableWithCheckConstraints() {
        printStatement("create table t (a int check(a >= 0), b boolean)");
        printStatement("create table t (a int constraint over_zero check(a >= 0), b boolean)");
        printStatement("create table t (a int, b boolean, check(a >= 0))");
        printStatement("create table t (a int, b boolean, constraint over_zero check(a >= 0))");
        printStatement("create table t (a int, b boolean check(b))");
        printStatement("create table t (a int, b boolean constraint b check(b))");
    }

    @Test
    public void testAlterTableAddColumnWithCheckConstraint() {
        printStatement("alter table t add column a int check(a >= 0)");
        printStatement("alter table t add column a int constraint over_zero check(a >= 0)");
    }

    @Test
    public void testAlterTableDropCheckConstraint() {
        printStatement("alter table t drop constraint check");
    }

    @Test
    public void testAddGeneratedColumn() {
        printStatement("alter table t add col2 AS date_trunc('day', col1)");
        printStatement("alter table t add col2 AS date_trunc('day', col1) INDEX USING PLAIN");
        printStatement("alter table t add col2 AS (date_trunc('day', col1))");

        printStatement("alter table t add col2 GENERATED ALWAYS AS date_trunc('day', col1)");
        printStatement("alter table t add col2 GENERATED ALWAYS AS (date_trunc('day', col1))");

        printStatement("alter table t add col2 string GENERATED ALWAYS AS date_trunc('day', col1)");
        printStatement("alter table t add col2 string GENERATED ALWAYS AS (date_trunc('day', col1))");

        printStatement("alter table t add col2 AS cast(col1 as string)");
        printStatement("alter table t add col2 AS (cast(col1 as string))");
        printStatement("alter table t add col2 AS col1 + 1");
        printStatement("alter table t add col2 AS (col1 + 1)");

        printStatement("alter table t add col2 AS col1['name'] + 1");
    }

    @Test
    public void testAlterTableAddColumnTypeOrGeneratedExpressionAreDefined() {
        assertThatThrownBy(
            () -> printStatement("alter table t add column col2"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Column [\"col2\"]: data type needs to be provided or column " +
                        "should be defined as a generated expression");
    }


    @Test
    public void testAddColumnWithDefaultExpressionIsNotSupported() {
        assertThatThrownBy(
            () -> printStatement("mismatched input 'default'"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:1: mismatched input 'mismatched' expecting {'");
    }


    @Test
    public void testAlterTableOpenClose() {
        printStatement("alter table t close");
        printStatement("alter table t open");

        printStatement("alter table t partition (partitioned_col=1) close");
        printStatement("alter table t partition (partitioned_col=1) open");
    }

    @Test
    public void testAlterTableRenameTable() {
        printStatement("alter table t rename to t2");
    }

    @Test
    public void testAlterTableReroute() {
        printStatement("alter table t reroute move shard 1 from 'node1' to 'node2'");
        printStatement("alter table t reroute move shard '2'::short from 'node1' to 'node2'");
        printStatement("alter table t reroute move shard CAST(? AS long) from 'node1' to 'node2'");
        printStatement("alter table t partition (parted_col = ?) reroute move shard ? from ? to ?");
        printStatement("alter table t reroute allocate replica shard 1 on 'node1'");
        printStatement("alter table t reroute allocate replica shard '12'::int on 'node1'");
        printStatement("alter table t reroute cancel shard 1 on 'node1'");
        printStatement("alter table t reroute cancel shard CAST('12' AS int) on 'node1'");
        printStatement("alter table t reroute cancel shard ?::long on 'node1'");
        printStatement("alter table t reroute cancel shard 1 on 'node1' with (allow_primary = true)");
        printStatement("ALTER TABLE t REROUTE PROMOTE REPLICA SHARD 1 ON 'node1' WITH (accept_data_loss = true, foo = ?)");
        printStatement("ALTER TABLE t REROUTE PROMOTE REPLICA SHARD ? ON ? ");
        printStatement("ALTER TABLE t REROUTE PROMOTE REPLICA SHARD '12'::short ON ? ");
    }

    @Test
    public void testAlterUser() {
        printStatement("alter user crate set (password = 'password')");
        printStatement("alter user crate set (password = null, session_setting='foo')");
        printStatement("alter user crate set (password = null, session_setting=?)");
        printStatement("alter user crate reset session_setting");
        printStatement("alter user crate reset \"session.setting\"");
        printStatement("alter user crate reset all");
    }

    @Test
    public void testAlterRole() {
        printStatement("alter role r1 set (password = 'password')");
        printStatement("alter role r1 set (password = null, session_setting='foo')");
        printStatement("alter role r1 set (session_setting=?)");
        printStatement("alter role r1 reset session_setting");
        printStatement("alter role r1 reset \"session.setting\"");
        printStatement("alter role r1 reset all");
    }

    @Test
    public void testAlterUserWithMissingProperties() {
        assertThatThrownBy(
            () -> printStatement("alter user crate"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:17: no viable alternative at input 'alter user crate'");
    }

    @Test
    public void testAlterRoleWithMissingProperties() {
        assertThatThrownBy(
            () -> printStatement("alter role r1"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:14: no viable alternative at input 'alter role r1'");
    }

    @Test
    public void testSubSelects() {
        printStatement("select * from (select * from foo) as f");
        printStatement("select * from (select * from (select * from foo) as f1) as f2");
        printStatement("select * from (select * from foo) f");
        printStatement("select * from (select * from (select * from foo) f1) f2");
    }

    @Test
    public void testJoins() {
        printStatement("select * from foo inner join bar on foo.id = bar.id");

        printStatement("select * from foo left outer join bar on foo.id = bar.id");
        printStatement("select * from foo left join bar on foo.id = bar.id");
        printStatement("select * from foo right outer join bar on foo.id = bar.id");
        printStatement("select * from foo right join bar on foo.id = bar.id");
        printStatement("select * from foo full outer join bar on foo.id = bar.id");
        printStatement("select * from foo full join bar on foo.id = bar.id");
    }

    @Test
    public void testConditionals() {
        printStatement("SELECT a," +
                       "       CASE WHEN a=1 THEN 'one'" +
                       "            WHEN a=2 THEN 'two'" +
                       "            ELSE 'other'" +
                       "       END" +
                       "    FROM test");
        printStatement("SELECT a," +
                       "       CASE a WHEN 1 THEN 'one'" +
                       "              WHEN 2 THEN 'two'" +
                       "              ELSE 'other'" +
                       "       END" +
                       "    FROM test");
        printStatement("SELECT a WHERE CASE WHEN x <> 0 THEN y/x > 1.5 ELSE false END");
    }

    @Test
    public void testUnions() {
        printStatement("select * from foo union select * from bar");
        printStatement("select * from foo union all select * from bar");
        printStatement("select * from foo union distinct select * from bar");
        printStatement("select 1 " +
                       "union select 2 " +
                       "union distinct select 3 " +
                       "union all select 4");
        printStatement("select 1 union " +
                       "select 2 union all " +
                       "select 3 union " +
                       "select 4 union all " +
                       "select 5 union distinct " +
                       "select 6 " +
                       "order by 1");
    }

    @Test
    public void testCreateViewParsing() {
        printStatement("CREATE VIEW myView AS SELECT * FROM foobar");
        printStatement("CREATE VIEW myView AS ( SELECT * FROM foobar )");
        printStatement("CREATE VIEW myView AS ((( SELECT * FROM foobar )))");
        printStatement("CREATE OR REPLACE VIEW myView AS SELECT * FROM foobar");
    }

    @Test
    public void testDropViewParsing() {
        printStatement("DROP VIEW myView");
        printStatement("DROP VIEW v1, v2, x.v3");
        printStatement("DROP VIEW IF EXISTS myView");
        printStatement("DROP VIEW IF EXISTS v1, x.v2, y.v3");
    }

    @Test
    public void test_values_as_top_relation_parsing() {
        printStatement("VALUES (1, 2), (2, 3), (3, 4)");
        printStatement("VALUES (1), (2), (3)");
        printStatement("VALUES (1, 2, 3 + 3, (SELECT 1))");
    }

    @Test
    public void test_wildcard_aggregate_with_filter_clause() {
        printStatement("SELECT COUNT(*) FILTER (WHERE x > 10) FROM t");
    }

    @Test
    public void test_distinct_aggregate_with_filter_clause() {
        printStatement("SELECT AVG(DISTINCT x) FILTER (WHERE 1 = 1) FROM t");
    }

    @Test
    public void test_multiple_aggregates_with_filter_clauses() {
        printStatement(
            "SELECT " +
            "   SUM(x) FILTER (WHERE x > 10), " +
            "   AVG(x) FILTER (WHERE 1 = 1) " +
            "FROM t");
    }

    @Test
    public void test_create_table_with_parametrized_varchar_data_type() {
        printStatement("create table test(col varchar(1))");
        printStatement("create table test(col character varying(2))");
    }

    @Test
    public void test_restore_snapshot() {
        printStatement("RESTORE SNAPSHOT repo1.snap1 ALL");
        printStatement("RESTORE SNAPSHOT repo1.snap1 ALL WITH (wait_for_completion = true)");
        printStatement("RESTORE SNAPSHOT repo1.snap1 TABLE t PARTITION (parted_col = ?)");
        printStatement("RESTORE SNAPSHOT repo1.snap1 METADATA");
        printStatement("RESTORE SNAPSHOT repo1.snap1 USERS WITH (some_option = true)");
        printStatement("RESTORE SNAPSHOT repo1.snap1 USERS, PRIVILEGES");
        printStatement("RESTORE SNAPSHOT repo1.snap1 TABLES, PRIVILEGES");
    }

    @Test
    public void test_scalar_subquery_in_selectlist() {
        printStatement("SELECT (SELECT 1)");
        printStatement("SELECT (SELECT 1) = ANY([5])");
    }

    @Test
    public void test_create_publication() {
        printStatement("CREATE PUBLICATION pub1 FOR ALL TABLES");
        printStatement("CREATE PUBLICATION \"myPublication\" FOR TABLE t1");
        printStatement("CREATE PUBLICATION pub1 FOR TABLE s1.t1");
        printStatement("CREATE PUBLICATION pub1 FOR TABLE t1, s2.t2");
        printStatement("CREATE PUBLICATION pub1");
    }

    @Test
    public void test_drop_publication() {
        printStatement("DROP PUBLICATION pub1");
        printStatement("DROP PUBLICATION IF EXISTS pub1");
    }

    @Test
    public void test_alter_publication() {
        printStatement("ALTER PUBLICATION pub1 ADD TABLE t1");
        printStatement("ALTER PUBLICATION \"myPublication\" SET TABLE t1");
        printStatement("ALTER PUBLICATION pub1 DROP TABLE s1.t1");
    }

    @Test
    public void test_create_subscription() {
        printStatement("CREATE SUBSCRIPTION sub1" +
                       " CONNECTION 'postgresql://user@localhost/crate:5432'" +
                       " PUBLICATION pub1");
        printStatement("CREATE SUBSCRIPTION sub1" +
                       " CONNECTION 'postgresql://user@localhost/crate:5432'" +
                       " PUBLICATION pub1, pub2");
        printStatement("CREATE SUBSCRIPTION sub1" +
                       " CONNECTION 'postgresql://user@localhost/crate:5432'" +
                       " PUBLICATION pub1" +
                       " WITH (enabled=true)");
    }

    @Test
    public void test_drop_subscription() {
        printStatement("DROP SUBSCRIPTION sub1");
        printStatement("DROP SUBSCRIPTION IF EXISTS sub1");
    }

    @Test
    public void test_alter_subscription() {
        printStatement("ALTER SUBSCRIPTION sub1 ENABLE");
        printStatement("ALTER SUBSCRIPTION \"mySub\" DISABLE");
    }

    @Test
    public void test_with_statement() {
        printStatement("WITH r AS (SELECT * FROM t1)\n" +
            "SELECT * FROM t2, r");
        printStatement("WITH r AS (SELECT * FROM t1)," +
            " s AS (SELECT * FROM t2)\n" +
            "SELECT * FROM t2, r");
        printStatement("WITH r AS (SELECT * FROM t1)," +
            " s AS (WITH r AS (SELECT * FROM t2) SELECT * FROM r)\n" +
            "SELECT * FROM t2, r");
    }

    @Test
    public void test_add_column_drop_column_not_supported_in_the_same_stmt() {
        assertThatThrownBy(() -> printStatement("alter table t add column a int drop column a"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:32: mismatched input 'drop' expecting {<EOF>, ';'}");
        assertThatThrownBy(() -> printStatement("alter table t drop column a, add column a int"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:30: mismatched input 'add' expecting 'DROP'");
    }

    @Test
    public void test_like_with_pattern_as_a_c_style_string() {
        printStatement("select 'TextToMatch' LIKE E'Te\\%tch'");
        printStatement("select 'TextToMatch' NOT LIKE E'Te\\%tch'");
        printStatement("select 'TextToMatch' ILIKE E'te\\%tch'");
        printStatement("select 'TextToMatch' NOT ILIKE E'te\\%tch'");
        printStatement("select 'TextToMatch' LIKE ANY (array[E'Te\\%tch'])");
        printStatement("select 'TextToMatch' NOT LIKE ANY (array[E'Te\\%tch'])");
        printStatement("select 'TextToMatch' ILIKE ANY (array[E'te\\%tch'])");
        printStatement("select 'TextToMatch' NOT ILIKE ANY (array[E'te\\%tch'])");
    }

    @Test
    public void test_escape_in_like() {
        printStatement("select 'ab' LIKE 'a%' ESCAPE ''");  // Empty
        printStatement("select 'ab' LIKE 'a%' ESCAPE ?");   // Parameter
        printStatement("select 'ab' LIKE 'a%' ESCAPE 't'"); // Regular character
    }

    @Test
    public void test_create_server() throws Exception {
        printStatement("create server jarvis foreign data wrapper postgres_fdw");
        printStatement("create server if not exists jarvis foreign data wrapper postgres_fdw");
        printStatement("create server jarvis foreign data wrapper postgres_fdw options (host 'foo', dbname 'foodb', port '5432')");
        printStatement("create server jarvis foreign data wrapper postgres_fdw options (host ?, xs array[1, 2, 3])");
    }

    @Test
    public void test_alter_server() {
        printStatement("ALTER SERVER jarvis OPTIONS (ADD host 'foo', SET dbname 'cratedb', DROP port)");
        printStatement("ALTER SERVER jarvis OPTIONS (ADD host 'foo')");
        printStatement("ALTER SERVER jarvis OPTIONS (SET dbname 'cratedb')");
        printStatement("ALTER SERVER jarvis OPTIONS (DROP port)");
        printStatement("ALTER SERVER jarvis OPTIONS (host 'foo')");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_alter_server_default_option_operation_is_add() {
        AlterServer<Expression> alterServer = (AlterServer<Expression>) SqlParser.createStatement("ALTER SERVER jarvis OPTIONS (host 'foo')");
        assertThat(alterServer.options()).contains(new AlterServer.Option<>(AlterServer.Operation.ADD, "host", new StringLiteral("foo")));
    }

    @Test
    public void test_drop_server() throws Exception {
        printStatement("drop server jarvis");
        printStatement("drop server pg1, pg2");
        printStatement("drop server if exists pg1, pg2");
        printStatement("drop server if exists pg1, pg2 cascade");
        printStatement("drop server if exists pg1, pg2 restrict");
    }

    @Test
    public void test_create_foreign_table() throws Exception {
        printStatement("create foreign table tbl (x int) server pg");
        printStatement("create foreign table if not exists tbl (x int) server pg");
        printStatement("create foreign table tbl (x int) server pg options (schema_name 'public')");
        printStatement("create foreign table tbl (x int) server pg options (schema_name 'public', dummy 'xy')");
    }

    @Test
    public void test_drop_foreign_table() throws Exception {
        printStatement("drop foreign table tbl");
        printStatement("drop foreign table tbl cascade");
        printStatement("drop foreign table if exists tbl");
        printStatement("drop foreign table if exists t1, t2, t3");
        printStatement("drop foreign table if exists doc.t1, s1.t2, t3");
        printStatement("drop foreign table if exists t1, t2, t3 cascade");
        printStatement("drop foreign table if exists t1, t2, t3 restrict");
    }


    @Test
    public void test_create_user_mapping() throws Exception {
        printStatement("create user mapping for crate server pg");
        printStatement("create user mapping if not exists for crate server pg");
        printStatement("create user mapping for CURRENT_ROLE server pg");
        printStatement("create user mapping for USER server pg");
        printStatement("create user mapping for CURRENT_USER server pg");
        printStatement("create user mapping if not exists for arthur server pg options (\"username\" 'bob', password 'secret')");
    }

    @Test
    public void test_drop_user_mapping() throws Exception {
        printStatement("drop user mapping for crate server pg");
        printStatement("drop user mapping if exists for crate server pg");
        printStatement("drop user mapping for current_role server pg");
        printStatement("drop user mapping for current_user server pg");
        printStatement("drop user mapping for user server pg");
    }

    private static void printStatement(String sql) {
        println(sql.trim());
        println("");

        Statement statement = SqlParser.createStatement(sql);
        println(statement.toString());
        println("");

        // TODO: support formatting all statement types
        if (statement instanceof Query ||
            statement instanceof CreateTable ||
            statement instanceof CreateTableAs ||
            statement instanceof CreateForeignTable ||
            statement instanceof CopyFrom ||
            statement instanceof SwapTable ||
            statement instanceof GCDanglingArtifacts ||
            statement instanceof CreateFunction ||
            statement instanceof CreateRole ||
            statement instanceof GrantPrivilege ||
            statement instanceof DenyPrivilege ||
            statement instanceof RevokePrivilege ||
            statement instanceof AlterRoleSet ||
            statement instanceof AlterRoleReset ||
            statement instanceof DropRole ||
            statement instanceof DropAnalyzer ||
            statement instanceof DropFunction ||
            statement instanceof DropTable ||
            statement instanceof DropBlobTable ||
            statement instanceof DropView ||
            statement instanceof DropRepository ||
            statement instanceof DropSnapshot ||
            statement instanceof Update ||
            statement instanceof Insert ||
            statement instanceof Explain ||
            statement instanceof SetSessionAuthorizationStatement ||
            statement instanceof Window ||
            statement instanceof CreatePublication ||
            statement instanceof DropPublication ||
            statement instanceof AlterPublication ||
            statement instanceof CreateSubscription ||
            statement instanceof DropSubscription ||
            statement instanceof AlterSubscription ||
            statement instanceof With ||
            statement instanceof Declare ||
            statement instanceof Fetch ||
            statement instanceof Close ||
            statement instanceof CreateServer ||
            statement instanceof AlterServer ||
            statement instanceof CreateUserMapping ||
            statement instanceof DropServer ||
            statement instanceof DropForeignTable ||
            statement instanceof DropUserMapping) {


            println(SqlFormatter.formatSql(statement));
            println("");
            assertFormattedSql(statement, SqlFormatter::formatSql);
            assertFormattedSql(statement, SqlFormatter::formatSqlInline);
        }

        println("=".repeat(60));
        println("");
    }

    private static void println(String s) {
        if (Boolean.parseBoolean(System.getProperty("printParse"))) {
            System.out.print(s + "\n");
        }
    }

    private static String getTpchQuery(int q)
        throws IOException {
        return readResource("tpch/queries/" + q + ".sql");
    }

    private static void printTpchQuery(int query, Object... values)
        throws IOException {
        String sql = getTpchQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(format(":%s", i + 1), String.valueOf(values[i]));
        }

        assertThat(sql.matches("(?s).*:[0-9].*"))
            .as("Not all bind parameters were replaced: " + sql)
            .isFalse();

        sql = fixTpchQuery(sql);
        printStatement(sql);
    }

    private static String readResource(String name) throws IOException {
        try (var inputStream = TestStatementBuilder.class.getClassLoader().getResourceAsStream(name)) {
            if (inputStream == null) {
                throw new IOException("'" + name + "' resource is not found");
            }
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String fixTpchQuery(String s) {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }
}
