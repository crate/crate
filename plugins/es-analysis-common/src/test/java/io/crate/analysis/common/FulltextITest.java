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
import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.integrationtests.Setup;

public class FulltextITest extends IntegTestCase {

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
        execute("refresh table locations");

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (analyzer='english') order by _score desc");
        assertThat(response).hasRows(
            "End of the Galaxy| 0.895417",
            "Altair| 0.49754602",
            "Outer Eastern Rim| 0.47476405",
            "North West Ripple| 0.46413797");

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (fuzziness=0.5) order by _score desc");
        assertThat(response).hasRows(
            "End of the Galaxy| 0.895417",
            "Altair| 0.49754602",
            "Outer Eastern Rim| 0.47476405",
            "North West Ripple| 0.46413797");

        execute("select name, _score from locations where match((kind, name_description_ft), 'galay') " +
                "using best_fields with (fuzziness='AUTO') order by _score desc");
        assertThat(response).hasRows(
            "End of the Galaxy| 0.7163336",
            "Altair| 0.3980368",
            "Outer Eastern Rim| 0.37981126",
            "North West Ripple| 0.37131035");

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using best_fields with (operator='or', minimum_should_match=2) order by _score desc");
        assertThat(response).hasRowCount(0);

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using phrase_prefix with (slop=1) order by _score desc");
        assertThat(response).hasRows(
            "Outer Eastern Rim| 1.3327041",
            "End of the Galaxy| 0.895417",
            "Galactic Sector QQ7 Active J Gamma| 0.8121827",
            "Algol| 0.70797026",
            "Altair| 0.49754602",
            "North West Ripple| 0.46413797");

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using phrase with (tie_breaker=1.0) order by _score desc");
        assertThat(response).hasRows(
            "End of the Galaxy| 0.895417",
            "Altair| 0.49754602",
            "Outer Eastern Rim| 0.47476405",
            "North West Ripple| 0.46413797");

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (zero_terms_query='all') order by _score desc");
        assertThat(response).hasRows(
            "End of the Galaxy| 0.895417",
            "Altair| 0.49754602",
            "Outer Eastern Rim| 0.47476405",
            "North West Ripple| 0.46413797");
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
        execute("refresh table matchbox");
        execute("select * from matchbox where match(s, 'Arthur')");
        assertThat(response).hasRowCount(1);

        execute("select * from matchbox where match(o['s'], 'Arthur')");
        assertThat(response).hasRowCount(0);

        execute("select * from matchbox where match(o['s'], 'Zaphod')");
        assertThat(response).hasRowCount(1);

        execute("select * from matchbox where match(s, 'Zaphod')");
        assertThat(response).hasRowCount(0);

        execute("select * from matchbox where match(o['m'], 'Ford')");
        assertThat(response).hasRowCount(1);

        assertSQLError(() -> execute("select * from matchbox where match(o_ignored['a'], 'Ford')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on 'undefined'");
    }

    @Test
    public void testMatchTypes() throws Exception {
        this.setup.setUpLocationsWithFTIndex();
        execute("refresh table locations");

        execute("select name, _score from locations where match((kind 0.8, name_description_ft 0.6), 'planet earth') " +
                "using best_fields order by _score desc");
        assertThat(response).hasRows(
            "Alpha Centauri| 0.6880375",
            "Bartledan| 0.3849704",
            "Galactic Sector QQ7 Active J Gamma| 0.34459937",
            "| 0.2462059",
            "Allosimanius Syneca| 0.16214824");

        execute("select name, _score from locations where match((kind 0.6, name_description_ft 0.8), 'planet earth') using most_fields order by _score desc");
        assertThat(response).hasRows(
            "Alpha Centauri| 0.91738325",
            "Bartledan| 0.5132938",
            "Galactic Sector QQ7 Active J Gamma| 0.4594658",
            "| 0.3282745",
            "Allosimanius Syneca| 0.21619764");

        execute("select name, _score from locations where match((kind 0.4, name_description_ft 1.0), 'planet earth') using cross_fields order by _score desc");
        assertThat(response).hasRows(
            "Alpha Centauri| 1.1467291",
            "Bartledan| 0.6416173",
            "Galactic Sector QQ7 Active J Gamma| 0.57433224",
            "| 0.41034314",
            "Allosimanius Syneca| 0.27024707");

        execute("select name, _score from locations where match((kind 1.0, name_description_ft 0.4), 'Alpha Centauri') using phrase");
        assertThat(response).hasRows("Alpha Centauri| 0.91738325");

        execute("select name, _score from locations where match(name_description_ft, 'Alpha Centauri') using phrase_prefix");
        assertThat(response).hasRows("Alpha Centauri| 2.2934582");
    }

    @Test
    public void testSelectWhereMultiColumnMatchDifferentTypesDifferentScore() throws Exception {
        this.setup.setUpLocationsWithFTIndex();
        execute("refresh table locations");
        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using most_fields with (analyzer='english') order by _score desc");
        assertThat(response).hasRows(
            "Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.57336456",
            "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.32080865",
            "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.28716612",
            "| This Planet doesn't really exist| Planet| 0.20517157",
            "Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.13512354");

        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using cross_fields order by _score desc");
        assertThat(response).hasRows(
            "Alpha Centauri| 4.1 light-years northwest of earth| Star System| 1.1467291",
            "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.6416173",
            "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.57433224",
            "| This Planet doesn't really exist| Planet| 0.41034314",
            "Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.27024707");
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
        assertThat(response).hasRowCount(1);
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
        execute("refresh table characters");
        execute("select characters.name AS characters_name, _score " +
                "from characters " +
                "where match(characters.quote_ft 2.0, 'country') order by _score desc");
        assertThat(response).hasRows("Arthur| 0.3596026", "Trillian| 0.26152915");
    }
}
