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
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import io.crate.testing.TestingHelpers;

public class FulltextIntegrationTest extends SQLIntegrationTestCase  {

    @Test
    public void testSelectMatch() throws Exception {
        execute("create table quotes (quote string)");

        execute("insert into quotes values (?)", new Object[]{"don't panic"});
        refresh();

        execute("select quote from quotes where match(quote, ?)", new Object[]{"don't panic"});
        assertEquals(1L, response.rowCount());
        assertEquals("don't panic", response.rows()[0][0]);
    }

    @Test
    public void testSelectNotMatch() throws Exception {
        execute("create table quotes (quote string) with (number_of_replicas = 0)");

        execute("insert into quotes values (?), (?)", new Object[]{"don't panic", "hello"});
        refresh();

        execute("select quote from quotes where not match(quote, ?)",
            new Object[]{"don't panic"});
        assertEquals(1L, response.rowCount());
        assertEquals("hello", response.rows()[0][0]);
    }

    @Test
    public void testMatchUnsupportedInSelect() {
        execute("create table quotes (quote string)");
        assertThrowsMatches(() -> execute("select match(quote, 'the quote') from quotes"),
                     isSQLError(is("match predicate cannot be selected"),
                                INTERNAL_ERROR, BAD_REQUEST, 4004));
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

        execute("select * from quotes");
        execute("select quote, \"_score\" from quotes where match(quote_ft, ?) " +
                "order by \"_score\" desc",
            new Object[]{"time"}
        );
        assertEquals(2L, response.rowCount());
        assertEquals("Time is an illusion. Lunchtime doubly so", response.rows()[0][0]);
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
        assertEquals(2L, response.rowCount());
        assertEquals(1.0f, response.rows()[0][1]);
        assertEquals(1.0f, response.rows()[1][1]);
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
        assertEquals(1L, response.rowCount());
        assertThat((Float) response.rows()[0][1], greaterThanOrEqualTo(1.12f));

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 1.12 order by quote");
        assertEquals(1L, response.rowCount());
        assertEquals(false, Float.isNaN((Float) response.rows()[0][1]));
        assertThat((Float) response.rows()[0][1], greaterThanOrEqualTo(1.12f));
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
        assertThat(response.rowCount(), is(1L));
        // also equals term must work
        execute("SELECT id, keywords FROM t_string_array WHERE keywords_ft = 'foo'");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "1| [foo bar]\n"
        ));

        // equals original whole string must not work
        execute("SELECT id, keywords FROM t_string_array WHERE keywords_ft = 'foo bar'");
        assertThat(response.rowCount(), is(0L));
    }
}
