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

package io.crate.analysis.common;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.junit.Test;

import io.crate.integrationtests.SQLIntegrationTestCase;
import io.crate.integrationtests.Setup;
import io.crate.testing.TestingHelpers;

public class FulltextITest extends SQLIntegrationTestCase {

    private Setup setup = new Setup(sqlExecutor);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }

    @Test
    public void testMatchOptions() throws Exception {
        this.setup.setUpLocationsWithFTIndex();
        refresh();

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (analyzer='english') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           End of the Galaxy| 0.74187315
                           Altair| 0.63013375
                           Outer Eastern Rim| 0.39226836
                           North West Ripple| 0.38249224
                           """);

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (fuzziness=0.5) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           End of the Galaxy| 0.74187315
                           Altair| 0.63013375
                           Outer Eastern Rim| 0.39226836
                           North West Ripple| 0.38249224
                           """);

        execute("select name, _score from locations where match((kind, name_description_ft), 'galay') " +
                "using best_fields with (fuzziness='AUTO') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           End of the Galaxy| 0.5934985
                           Altair| 0.504107
                           Outer Eastern Rim| 0.3138147
                           North West Ripple| 0.3059938
                           """);

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using best_fields with (operator='or', minimum_should_match=2) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows())).isEmpty();

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using phrase_prefix with (slop=1) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           Outer Eastern Rim| 1.1364496
                           Algol| 0.8532188
                           Galactic Sector QQ7 Active J Gamma| 0.7837707
                           End of the Galaxy| 0.74187315
                           Altair| 0.63013375
                           North West Ripple| 0.38249224
                           """);

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using phrase with (tie_breaker=1.0) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           End of the Galaxy| 0.74187315
                           Altair| 0.63013375
                           Outer Eastern Rim| 0.39226836
                           North West Ripple| 0.38249224
                           """);

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (zero_terms_query='all') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           End of the Galaxy| 0.74187315
                           Altair| 0.63013375
                           Outer Eastern Rim| 0.39226836
                           North West Ripple| 0.38249224
                           """);
    }

    @Test
    public void testMatchNotOnSubColumn() {
        execute("create table matchbox (" +
                "  s string index using fulltext with (analyzer='german')," +
                "  o object as (" +
                "    s string index using fulltext with (analyzer='german')," +
                "    m string index using fulltext with (analyzer='german')" +
                "  )," +
                "  o_ignored object(ignored)" +
                ") with (number_of_replicas=0)");
        execute("insert into matchbox (s, o) values ('Arthur Dent', {s='Zaphod Beeblebroox', m='Ford Prefect'})");
        refresh();
        execute("select * from matchbox where match(s, 'Arthur')");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select * from matchbox where match(o['s'], 'Arthur')");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("select * from matchbox where match(o['s'], 'Zaphod')");
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select * from matchbox where match(s, 'Zaphod')");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("select * from matchbox where match(o['m'], 'Ford')");
        assertThat(response.rowCount()).isEqualTo(1L);

        assertThrowsMatches(() -> execute("select * from matchbox where match(o_ignored['a'], 'Ford')"),
                     isSQLError(is("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on 'undefined'"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @Test
    public void testMatchTypes() throws Exception {
        this.setup.setUpLocationsWithFTIndex();
        refresh();

        execute("select name, _score from locations where match((kind 0.8, name_description_ft 0.6), 'planet earth') " +
                "using best_fields order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           Alpha Centauri| 0.62114334
                           Bartledan| 0.45822048
                           | 0.2120642
                           Allosimanius Syneca| 0.1592113
                           Galactic Sector QQ7 Active J Gamma| 0.12744746
                           """);

        execute("select name, _score from locations where match((kind 0.6, name_description_ft 0.8), 'planet earth') using most_fields order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("Alpha Centauri| 0.8281911\nBartledan| 0.6109606\n| 0.28275228\nAllosimanius Syneca| 0.21228172\nGalactic Sector QQ7 Active J Gamma| 0.16992995\n");

        execute("select name, _score from locations where match((kind 0.4, name_description_ft 1.0), 'planet earth') using cross_fields order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("Alpha Centauri| 1.0352387\nBartledan| 0.7637007\n| 0.35344034\nAllosimanius Syneca| 0.26535213\nGalactic Sector QQ7 Active J Gamma| 0.21241242\n");

        execute("select name, _score from locations where match((kind 1.0, name_description_ft 0.4), 'Alpha Centauri') using phrase");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("Alpha Centauri| 0.8281911\n");

        execute("select name, _score from locations where match(name_description_ft, 'Alpha Centauri') using phrase_prefix");
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("Alpha Centauri| 2.0704775\n");
    }

    @Test
    public void testSelectWhereMultiColumnMatchDifferentTypesDifferentScore() throws Exception {
        this.setup.setUpLocationsWithFTIndex();
        refresh();
        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using most_fields with (analyzer='english') order by _score desc");
        assertThat(response.rowCount()).isEqualTo(5L);
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.5176194
                           Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.38185036
                           | This Planet doesn't really exist| Planet| 0.17672017
                           Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.13267606
                           Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.10620621
                           """);

        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using cross_fields order by _score desc");
        assertThat(response.rowCount()).isEqualTo(5L);
        assertThat(TestingHelpers.printedTable(response.rows()))
            .isEqualTo("""
                           Alpha Centauri| 4.1 light-years northwest of earth| Star System| 1.0352387
                           Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.7637007
                           | This Planet doesn't really exist| Planet| 0.35344034
                           Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.26535213
                           Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.21241242
                           """);
    }

    @Test
    public void testSelectMatchAnd() {
        execute("create table quotes (id int, quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english')) with (number_of_replicas = 0)");
        execute("insert into quotes (id, quote) values (?, ?), (?, ?)",
            new Object[]{
                1, "Would it save you a lot of time if I just gave up and went mad now?",
                2, "Time is an illusion. Lunchtime doubly so"}
        );
        execute("refresh table quotes");

        execute("select quote from quotes where match(quote_fulltext, 'time') and id = 1");
        assertThat(response.rowCount()).isEqualTo(1L);
    }

    @Test
    public void testSimpleMatchWithBoost() {
        execute("create table characters ( " +
                "  id int primary key, " +
                "  name string, " +
                "  quote string, " +
                "  INDEX name_ft using fulltext(name) with (analyzer = 'english'), " +
                "  INDEX quote_ft using fulltext(quote) with (analyzer = 'english') " +
                ") clustered into 5 shards ");
        ensureYellow();
        execute("insert into characters (id, name, quote) values (?, ?, ?)", new Object[][]{
            new Object[]{1, "Arthur", "What a country. It's terribly small, tiny little country."},
            new Object[]{2, "Trillian", " No, it's a country. Off the coast of Africa."},
            new Object[]{3, "Marvin", " It won't work, I have an exceptionally large mind."}
        });
        refresh();
        execute("select characters.name AS characters_name, _score " +
                "from characters " +
                "where match(characters.quote_ft 2.0, 'country') order by _score desc");
        System.out.println(TestingHelpers.printedTable(response.rows()));
        assertThat(response.rows().length).isEqualTo(2);
        assertThat(response.rows()[0][0]).isEqualTo("Arthur");
        assertThat(response.rows()[0][1]).isEqualTo(0.3596026F);
        assertThat(response.rows()[1][0]).isEqualTo("Trillian");
        assertThat(response.rows()[1][1]).isEqualTo(0.26152915F);
    }
}
