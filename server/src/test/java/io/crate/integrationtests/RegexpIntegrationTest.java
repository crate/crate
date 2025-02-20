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

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.expression.operator.RegexpMatchCaseInsensitiveOperator;
import io.crate.expression.operator.RegexpMatchOperator;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.netty.handler.codec.http.HttpResponseStatus;

public class RegexpIntegrationTest extends IntegTestCase {

    private final Setup setup = new Setup(sqlExecutor);

    @Test
    public void testRegexpReplaceIsNull() throws Exception {
        execute("create table regex_test (i integer, s string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into regex_test(i, s) values (?, ?)", new Object[][]{
            new Object[]{1, "foo is first"},
            new Object[]{2, "bar is second"},
            new Object[]{3, "foobar is great"},
            new Object[]{4, "crate is greater"},
            new Object[]{5, "foo"},
            new Object[]{6, null}
        });
        execute("refresh table regex_test");
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is not null");
        assertThat(response).hasRowCount(5);
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is null");
        assertThat(response).hasRowCount(1);
    }

    @Test
    public void testInvalidPatternSyntax() throws Exception {
        execute("create table phone (phone string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into phone (phone) values (?)", new Object[][]{
            new Object[]{"+1234567890"}
        });
        execute("refresh table phone");
        Asserts.assertSQLError(() -> execute("select * from phone where phone ~* '+1234567890'"))
                .hasPGError(PGErrorStatus.INTERNAL_ERROR)
                .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000)
                .hasMessageContaining("Dangling meta character '+' near index");
        ensureYellow();
    }

    /**
     * Test querying using regular expressions based on RegexpQuery,
     * which in turn is based on the fast finite-state automata
     * regular expression engine implementation `dk.brics.automaton`.
     * <p>
     * This engine is the default when using the regexp tilde operator `~`.
     *
     * @see {@link org.apache.lucene.search.RegexpQuery}
     * @see {@link org.apache.lucene.util.automaton.RegExp}
     * @see <a href="http://www.brics.dk/automaton/">http://www.brics.dk/automaton/</a>
     * @see <a href="http://tusker.org/regex/regex_benchmark.html">http://tusker.org/regex/regex_benchmark.html</a>
     */
    @Test
    public void testRegexpMatchQueryOperatorFast() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        execute("refresh table locations");
        execute("select distinct name from locations where name ~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response).hasRows(
            "Aldebaran",
            "Algol",
            "Altair",
            "Argabuthon",
            "Bartledan");

        execute("select name from locations where name !~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response).hasRows(
            "",
            "Allosimanius Syneca",
            "Alpha Centauri",
            "Arkintoofle Minor",
            "End of the Galaxy",
            "Galactic Sector QQ7 Active J Gamma",
            "North West Ripple",
            "Outer Eastern Rim");

        execute("select name from locations where name ~ '~(Galactic Sector QQ7 Active J Gamma)' order by name");
        assertThat(response).hasRows(
            "",
            "Aldebaran",
            "Algol",
            "Allosimanius Syneca",
            "Alpha Centauri",
            "Altair",
            "Argabuthon",
            "Arkintoofle Minor",
            "Bartledan",
            "End of the Galaxy",
            "North West Ripple",
            "Outer Eastern Rim");
    }

    /**
     * Test querying using regular expressions based on CrateRegexQuery,
     * which in turn uses the regular expression engine of the
     * Java standard library.
     * <p>
     * This engine is active when using the case-insensitive regexp tilde operator `~*`.
     *
     * @see {@link CrateRegexQuery}
     * @see {@link java.util.regex}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithCaseInsensitivity() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        execute("refresh table locations");
        execute("select distinct name from locations where name ~* 'aldebaran'");
        assertThat(response).hasRows("Aldebaran");

        execute("select distinct name from locations where name !~* 'aldebaran|algol|altair' and name != '' order by name");
        assertThat(response).hasRows(
            "Allosimanius Syneca",
            "Alpha Centauri",
            "Argabuthon", "Arkintoofle Minor",
            "Bartledan",
            "End of the Galaxy",
            "Galactic Sector QQ7 Active J Gamma",
            "North West Ripple",
            "Outer Eastern Rim");
    }

    /**
     * Test querying using regular expressions based on CrateRegexQuery,
     * which in turn uses the regular expression engine of the
     * Java standard library.
     * <p>
     * This engine is active when using the regular regexp tilde operator `~`,
     * but the pattern used contains PCRE features, which the fast regex
     * implementation {@link org.apache.lucene.util.automaton.RegExp}
     * isn't capable of.
     *
     * @see {@link CrateRegexQuery}
     * @see {@link java.util.regex}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithPcre() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        execute("refresh table locations");

        // character class shortcut aliases
        execute("select distinct name from locations where name ~ 'Alpha\\sCentauri'");
        assertThat(response).hasRows("Alpha Centauri");

        // word boundaries: positive
        execute("select distinct name from locations where name ~ '.*\\bCentauri\\b.*'");
        assertThat(response).hasRows("Alpha Centauri");

        // word boundaries: negative
        execute("select distinct name from locations where name ~ '.*\\bauri\\b.*'");
        assertThat(response).hasRowCount(0L);

        // embedded flag expressions
        execute("select distinct name from locations where name ~ '(?i).*centauri.*'");
        assertThat(response).hasRows("Alpha Centauri");

        execute("select count(name) from locations where name ~ '(?i).*centauri.*'");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat((Long) response.rows()[0][0]).isEqualTo(1L);
    }

    /**
     * Test ~ with PCRE features, but on system tables, using {@link RegexpMatchOperator#evaluate}
     */
    @Test
    public void testRegexpMatchQueryOperatorOnSysShards() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        execute("refresh table locations");

        execute("select table_name, * from sys.shards where table_name ~ '(?i)LOCATIONS' order by table_name");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat((String) response.rows()[0][0]).isEqualTo("locations");
    }

    /**
     * Test ~* with PCRE features, but on system tables, using {@link RegexpMatchCaseInsensitiveOperator#evaluate}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithCaseInsensitivityOnSysShards() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        execute("refresh table locations");

        execute("select table_name, * from sys.shards where table_name ~* 'LOCATIONS' order by table_name");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat((String) response.rows()[0][0]).isEqualTo("locations");
    }
}


