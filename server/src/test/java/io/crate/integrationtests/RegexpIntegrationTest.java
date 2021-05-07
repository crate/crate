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

import io.crate.protocols.postgres.PGErrorStatus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Test;

import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;

public class RegexpIntegrationTest extends SQLIntegrationTestCase {

    private Setup setup = new Setup(sqlExecutor);

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
        refresh();
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is not null");
        assertThat(response.rowCount(), is(5L));
        execute("select i from regex_test where regexp_replace(s, 'is', 'was') is null");
        assertThat(response.rowCount(), is(1L));
    }

    @Test
    public void testInvalidPatternSyntax() throws Exception {
        execute("create table phone (phone string) with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into phone (phone) values (?)", new Object[][]{
            new Object[]{"+1234567890"}
        });
        refresh();
        assertThrows(() -> execute("select * from phone where phone ~* '+1234567890'"),
                     isSQLError(containsString("Dangling meta character '+' near index"),
                                PGErrorStatus.INTERNAL_ERROR, HttpResponseStatus.BAD_REQUEST, 4000)
        );
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
        refresh();
        execute("select distinct name from locations where name ~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(5L));
        assertThat((String) response.rows()[0][0], is("Aldebaran"));
        assertThat((String) response.rows()[1][0], is("Algol"));
        assertThat((String) response.rows()[2][0], is("Altair"));
        assertThat((String) response.rows()[3][0], is("Argabuthon"));
        assertThat((String) response.rows()[4][0], is("Bartledan"));

        execute("select name from locations where name !~ '[A-Z][a-z0-9]+' order by name");
        assertThat(response.rowCount(), is(8L));
        assertThat((String) response.rows()[0][0], is(""));
        assertThat((String) response.rows()[1][0], is("Allosimanius Syneca"));
        assertThat((String) response.rows()[2][0], is("Alpha Centauri"));
        assertThat((String) response.rows()[3][0], is("Arkintoofle Minor"));
        assertThat((String) response.rows()[4][0], is("End of the Galaxy"));
        assertThat((String) response.rows()[5][0], is("Galactic Sector QQ7 Active J Gamma"));
        assertThat((String) response.rows()[6][0], is("North West Ripple"));
        assertThat((String) response.rows()[7][0], is("Outer Eastern Rim"));
    }

    /**
     * Test querying using regular expressions based on RegexQuery,
     * which in turn uses the regular expression engine of the
     * Java standard library.
     * <p>
     * This engine is active when using the case-insensitive regexp tilde operator `~*`.
     *
     * @see {@link org.apache.lucene.sandbox.queries.regex.RegexQuery}
     * @see {@link java.util.regex}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithCaseInsensitivity() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();
        execute("select distinct name from locations where name ~* 'aldebaran'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Aldebaran"));

        execute("select distinct name from locations where name !~* 'aldebaran|algol|altair' and name != '' order by name");
        assertThat(response.rowCount(), is(9L));
        assertThat((String) response.rows()[0][0], is("Allosimanius Syneca"));
        assertThat((String) response.rows()[1][0], is("Alpha Centauri"));

    }

    /**
     * Test querying using regular expressions based on RegexQuery,
     * which in turn uses the regular expression engine of the
     * Java standard library.
     * <p>
     * This engine is active when using the regular regexp tilde operator `~`,
     * but the pattern used contains PCRE features, which the fast regex
     * implementation {@link org.apache.lucene.util.automaton.RegExp}
     * isn't capable of.
     *
     * @see {@link org.apache.lucene.sandbox.queries.regex.RegexQuery}
     * @see {@link java.util.regex}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithPcre() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();

        // character class shortcut aliases
        execute("select distinct name from locations where name ~ 'Alpha\\sCentauri'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Alpha Centauri"));

        // word boundaries: positive
        execute("select distinct name from locations where name ~ '.*\\bCentauri\\b.*'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Alpha Centauri"));

        // word boundaries: negative
        execute("select distinct name from locations where name ~ '.*\\bauri\\b.*'");
        assertThat(response.rowCount(), is(0L));

        // embedded flag expressions
        execute("select distinct name from locations where name ~ '(?i).*centauri.*'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Alpha Centauri"));

        execute("select count(name) from locations where name ~ '(?i).*centauri.*'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(1L));

    }

    /**
     * Same as above except that the code path is different as a countOperation is used for count(*) queries
     *
     * @see {@link org.elasticsearch.index.query.RegexpQueryParser}
     * @see {@link org.elasticsearch.index.mapper.core.AbstractFieldMapper#regexpQuery}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithPcreViaElasticSearchForCount() throws Exception {
        this.setup.setUpLocations();
        ensureYellow();
        refresh();

        execute("select count(*) from locations where name ~ '(?i).*centauri.*'");
        assertThat(response.rowCount(), is(1L));
        assertThat((Long) response.rows()[0][0], is(1L));

    }

    /**
     * Same as above, running through the same code path for DELETE expressions.
     *
     * @see {@link org.elasticsearch.index.query.RegexpQueryParser}
     * @see {@link org.elasticsearch.index.mapper.core.AbstractFieldMapper#regexpQuery}
     */
    @Test
    public void testRegexpMatchQueryOperatorWithPcreViaElasticSearchForDelete() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();

        execute("delete from locations where name ~ '(?i).*centauri.*'");
        assertThat(response.rowCount(), is(1L));
    }

    /**
     * Also test ~ and ~* operators with PCRE features, but on system tables.
     */
    @Test
    public void testRegexpMatchQueryOperatorOnSysShards() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();

        execute("select table_name, * from sys.shards where table_name ~ '(?i)LOCATIONS' order by table_name");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("locations"));
    }

    @Test
    public void testRegexpMatchQueryOperatorWithCaseInsensitivityOnSysShards() throws Exception {
        this.setup.setUpLocations();
        ensureGreen();
        refresh();

        execute("select table_name, * from sys.shards where table_name ~* 'LOCATIONS' order by table_name");
        assertThat(response.rowCount(), is(2L));
        assertThat((String) response.rows()[0][0], is("locations"));
    }

}


