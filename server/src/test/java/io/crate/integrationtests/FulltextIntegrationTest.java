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

package io.crate.integrationtests;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.Asserts;

public class FulltextIntegrationTest extends IntegTestCase {

    @Test
    public void testSelectMatch() throws Exception {
        execute("create table quotes (quote string)");

        execute("insert into quotes values (?)", new Object[] {"don't panic"});
        refresh();

        execute("select quote from quotes where match(quote, ?)", new Object[] {"don't panic"});
        assertThat(response).hasRows(
            "don't panic"
        );
    }

    @Test
    public void testSelectNotMatch() throws Exception {
        execute("create table quotes (quote string) with (number_of_replicas = 0)");

        execute("insert into quotes values (?), (?)", new Object[]{"don't panic", "hello"});
        refresh();

        execute("select quote from quotes where not match(quote, ?)",
            new Object[]{"don't panic"});
        assertThat(response).hasRows(
            "hello"
        );
    }

    @Test
    public void testMatchUnsupportedInSelect() {
        execute("create table quotes (quote string)");
        Asserts.assertSQLError(() -> execute("select match(quote, 'the quote') from quotes"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("match predicate cannot be selected");
    }

    @Test
    public void testSelectOrderByScore() throws Exception {
        execute("create table quotes (quote string index off," +
                "index quote_ft using fulltext(quote)) clustered into 1 shards");
        execute("insert into quotes values (?)",
            new Object[]{"Would it save you a lot of time if I just gave up and went mad now?"}
        );
        execute("insert into quotes values (?)",
            new Object[]{"Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select quote from quotes where match(quote_ft, ?) " +
                "order by \"_score\" desc",
            new Object[]{"time"}
        );
        assertThat(response).hasRows(
            "Time is an illusion. Lunchtime doubly so",
            "Would it save you a lot of time if I just gave up and went mad now?"
        );
    }

    @Test
    public void testSelectScoreMatchAll() throws Exception {
        execute("create table quotes (quote string) with (number_of_replicas = 0)");

        execute("insert into quotes values (?), (?)",
            new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                "Time is an illusion. Lunchtime doubly so"}
        );
        execute("refresh table quotes");

        execute("select quote, \"_score\" from quotes");
        assertThat(response).hasRowCount(2L);
        assertThat(response.rows()[0][1]).isEqualTo(1.0f);
        assertThat(response.rows()[1][1]).isEqualTo(1.0f);
    }

    @Test
    public void testSelectWhereScore() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_ft using fulltext(quote)) clustered into 1 shards with (number_of_replicas = 0)");

        execute("insert into quotes values (?), (?)",
            new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                "Time is an illusion. Lunchtime doubly so. Take your time."}
        );
        refresh();

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 1.12");
        assertThat(response).hasRowCount(1L);
        assertThat((Float) response.rows()[0][1]).isGreaterThanOrEqualTo(1.12f);

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 1.12 order by quote");
        assertThat(response).hasRowCount(1L);
        assertThat(Float.isNaN((Float) response.rows()[0][1])).isEqualTo(false);
        assertThat((Float) response.rows()[0][1]).isGreaterThanOrEqualTo(1.12f);
    }

    @Test
    public void testCopyValuesFromStringArrayToIndex() throws Exception {
        execute("CREATE TABLE t_string_array (" +
                "  id INTEGER PRIMARY KEY," +
                "  keywords ARRAY(STRING) INDEX USING FULLTEXT," +
                "  INDEX keywords_ft USING FULLTEXT(keywords)" +
                ")");

        execute("INSERT INTO t_string_array (id, keywords) VALUES (1, ['foo bar'])");
        refresh();
        // matching a term must work
        execute("SELECT id, keywords FROM t_string_array WHERE match(keywords_ft, 'foo')");
        assertThat(response).hasRowCount(1L);
        // also equals term must work
        execute("SELECT id, keywords FROM t_string_array WHERE keywords_ft = 'foo'");
        assertThat(response).hasRows(
            "1| [foo bar]"
        );

        // equals original whole string must not work
        execute("SELECT id, keywords FROM t_string_array WHERE keywords_ft = 'foo bar'");
        assertThat(response).hasRowCount(0L);
    }

    @Test
    public void test_can_add_text_array_column() throws Exception {
        execute("create table t(id int)");
        execute("alter table t add column keywords ARRAY(STRING) INDEX USING FULLTEXT");
        execute("insert into t(id, keywords) VALUES (1, ['foo bar'])");
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("select column_name, data_type from information_schema.columns where table_name = 't' order by 1");
        assertThat(printedTable(response.rows())).isEqualTo(
            """
            id| integer
            keywords| text_array
            """
        );
    }
}
