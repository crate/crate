/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import io.crate.action.sql.SQLActionException;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class FulltextIntegrationTest extends SQLTransportIntegrationTest  {

    private Setup setup = new Setup(sqlExecutor);

    @Test
    public void testSelectMatch() throws Exception {
        execute("create table quotes (quote string)");
        ensureYellow();

        execute("insert into quotes values (?)", new Object[]{"don't panic"});
        refresh();

        execute("select quote from quotes where match(quote, ?)", new Object[]{"don't panic"});
        assertEquals(1L, response.rowCount());
        assertEquals("don't panic", response.rows()[0][0]);
    }

    @Test
    public void testSelectNotMatch() throws Exception {
        execute("create table quotes (quote string) with (number_of_replicas = 0)");
        ensureYellow();

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
        ensureYellow();
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("UnsupportedFeatureException: match predicate cannot be selected");
        execute("select match(quote, 'the quote') from quotes");
    }

    @Test
    public void testSelectOrderByScore() throws Exception {
        execute("create table quotes (quote string index off," +
                "index quote_ft using fulltext(quote))");
        ensureYellow();
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
        ensureYellow();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
            .actionGet().isExists());

        execute("insert into quotes values (?), (?)",
            new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                "Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select quote, \"_score\" from quotes");
        assertEquals(2L, response.rowCount());
        assertEquals(1.0f, response.rows()[0][1]);
        assertEquals(1.0f, response.rows()[1][1]);
    }

    @Test
    public void testSelectWhereScore() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_ft using fulltext(quote)) with (number_of_replicas = 0)");
        ensureYellow();

        execute("insert into quotes values (?), (?)",
            new Object[]{"Would it save you a lot of time if I just gave up and went mad now?",
                "Time is an illusion. Lunchtime doubly so. Take your time."}
        );
        refresh();

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 0.98");
        assertEquals(1L, response.rowCount());
        assertThat((Float) response.rows()[0][1], greaterThanOrEqualTo(0.98f));

        execute("select quote, \"_score\" from quotes where match(quote_ft, 'time') " +
                "and \"_score\" >= 0.98 order by quote");
        assertEquals(1L, response.rowCount());
        assertEquals(false, Float.isNaN((Float) response.rows()[0][1]));
        assertThat((Float) response.rows()[0][1], greaterThanOrEqualTo(0.98f));
    }

    @Test
    public void testSelectMatchAnd() throws Exception {
        execute("create table quotes (id int, quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='english')) with (number_of_replicas = 0)");
        ensureYellow();
        assertTrue(client().admin().indices().exists(new IndicesExistsRequest("quotes"))
            .actionGet().isExists());

        execute("insert into quotes (id, quote) values (?, ?), (?, ?)",
            new Object[]{
                1, "Would it save you a lot of time if I just gave up and went mad now?",
                2, "Time is an illusion. Lunchtime doubly so"}
        );
        refresh();

        execute("select quote from quotes where match(quote_fulltext, 'time') and id = 1");
        assertEquals(1L, response.rowCount());
    }
    @Test
    public void testMatchNotOnSubColumn() throws Exception {
        execute("create table matchbox (" +
                "  s string index using fulltext with (analyzer='german')," +
                "  o object as (" +
                "    s string index using fulltext with (analyzer='german')," +
                "    m string index using fulltext with (analyzer='german')" +
                "  )," +
                "  o_ignored object(ignored)" +
                ") with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into matchbox (s, o) values ('Arthur Dent', {s='Zaphod Beeblebroox', m='Ford Prefect'})");
        refresh();
        execute("select * from matchbox where match(s, 'Arthur')");
        assertThat(response.rowCount(), is(1L));

        execute("select * from matchbox where match(o['s'], 'Arthur')");
        assertThat(response.rowCount(), is(0L));

        execute("select * from matchbox where match(o['s'], 'Zaphod')");
        assertThat(response.rowCount(), is(1L));

        execute("select * from matchbox where match(s, 'Zaphod')");
        assertThat(response.rowCount(), is(0L));

        execute("select * from matchbox where match(o['m'], 'Ford')");
        assertThat(response.rowCount(), is(1L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("Can only use MATCH on columns of type STRING or GEO_SHAPE, not on 'null'");

        execute("select * from matchbox where match(o_ignored['a'], 'Ford')");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testMatchOptions() throws Exception {
        this.setup.setUpLocations();
        refresh();

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (analyzer='english') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 0.65826756\nAltair| 0.3518162\nNorth West Ripple| 0.20364113\nOuter Eastern Rim| 0.20364113\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (fuzziness=0.5) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 1.1972358\nAltair| 0.4790727\nNorth West Ripple| 0.37037593\nOuter Eastern Rim| 0.37037593\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galay') " +
                "using best_fields with (fuzziness='AUTO') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 1.1972358\nAltair| 0.4790727\nNorth West Ripple| 0.37037593\nOuter Eastern Rim| 0.37037593\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using best_fields with (operator='or', minimum_should_match=2) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is(""));

        execute("select name, _score from locations where match((kind, name_description_ft), 'gala') " +
                "using phrase_prefix with (slop=1) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 0.53688645\nOuter Eastern Rim| 0.49600732\nAlgol| 0.3770475\nGalactic Sector QQ7 Active J Gamma| 0.35930455\nAltair| 0.33875558\nNorth West Ripple| 0.16609077\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using phrase with (tie_breaker=2.0) order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 0.4428768\nAltair| 0.19800007\nNorth West Ripple| 0.13700803\nOuter Eastern Rim| 0.13700803\n"));

        execute("select name, _score from locations where match((kind, name_description_ft), 'galaxy') " +
                "using best_fields with (zero_terms_query='all') order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("End of the Galaxy| 0.65826756\nAltair| 0.3518162\nNorth West Ripple| 0.20364113\nOuter Eastern Rim| 0.20364113\n"));
    }

    @Test
    public void testMatchTypes() throws Exception {
        this.setup.setUpLocations();
        refresh();

        SearchRequest searchRequest = new SearchRequest("locations")
            .source("{\"query\": {\"multi_match\": {\"fields\": [\"kind^0.8\", \"name_description_ft^0.6\"], \"query\": \"planet earth\"}}}");
        SearchResponse searchResponse = client().search(searchRequest).actionGet();

        execute("select name, _score from locations where match((kind 0.8, name_description_ft 0.6), 'planet earth') using best_fields order by _score desc");

        SearchHit[] hits = searchResponse.getHits().getHits();
        for (int i = 0; i < hits.length; i++) {
            assertThat(hits[i].score(), is(((float) response.rows()[i][1])));
        }

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 0.2600391\nBartledan| 0.15168947\n| 0.10750017\nAllosimanius Syneca| 0.053750087\nGalactic Sector QQ7 Active J Gamma| 0.040312566\n"));


        execute("select name, _score from locations where match((kind 0.6, name_description_ft 0.8), 'planet earth') using most_fields order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 0.13054572\nBartledan| 0.07615167\n| 0.053683124\nAllosimanius Syneca| 0.026841562\nGalactic Sector QQ7 Active J Gamma| 0.02013117\n"));

        execute("select name, _score from locations where match((kind 0.4, name_description_ft 1.0), 'planet earth') using cross_fields order by _score desc");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 0.14860114\nBartledan| 0.086684\n| 0.061013106\nAllosimanius Syneca| 0.030506553\nGalactic Sector QQ7 Active J Gamma| 0.022879915\n"));

        execute("select name, _score from locations where match((kind 1.0, name_description_ft 0.4), 'Alpha Centauri') using phrase");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 1.1095\n"));

        execute("select name, _score from locations where match(name_description_ft, 'Alpha Centauri') using phrase_prefix");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 1.7897208\n"));
    }

    @Test
    public void testSelectWhereMultiColumnMatchDifferentTypesDifferentScore() throws Exception {
        this.setup.setUpLocations();
        refresh();
        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using most_fields with (analyzer='english') order by _score desc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.05715186\n" +
               "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.033338584\n| This Planet doesn't really exist| Planet| 0.023602562\n" +
               "Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.011801281\n" +
               "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.008850961\n"));

        execute("select name, description, kind, _score from locations " +
                "where match((kind, name_description_ft 0.5), 'Planet earth') using cross_fields order by _score desc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("Alpha Centauri| 4.1 light-years northwest of earth| Star System| 0.07601598\n" +
               "Bartledan| An Earthlike planet on which Arthur Dent lived for a short time, Bartledan is inhabited by Bartledanians, a race that appears human but only physically.| Planet| 0.044342656\n" +
               "| This Planet doesn't really exist| Planet| 0.031368554\n" +
               "Allosimanius Syneca| Allosimanius Syneca is a planet noted for ice, snow, mind-hurtling beauty and stunning cold.| Planet| 0.015684277\n" +
               "Galactic Sector QQ7 Active J Gamma| Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, the planet Preliumtarn of the famed Sevorbeupstry and Quentulus Quazgar Mountains.| Galaxy| 0.011763208\n"));
    }

    @Test
    public void testSimpleMatchWithBoost() throws Exception {
        execute("create table characters ( " +
                "  id int primary key, " +
                "  name string, " +
                "  quote string, " +
                "  INDEX name_ft using fulltext(name) with (analyzer = 'english'), " +
                "  INDEX quote_ft using fulltext(quote) with (analyzer = 'english') " +
                ") clustered into 5 shards ");
        ensureYellow();
        execute("insert into characters (id, name, quote) values (?, ?, ?)", new Object[][]{
            new Object[]{1, "Arthur", "It's terribly small, tiny little country."},
            new Object[]{2, "Trillian", " No, it's a country. Off the coast of Africa."},
            new Object[]{3, "Marvin", " It won't work, I have an exceptionally large mind."}
        });
        refresh();
        execute("select characters.name AS characters_name, _score " +
                "from characters " +
                "where match(characters.quote_ft 1.0, 'country') order by _score desc");
        assertThat(response.rows().length, is(2));
        assertThat(response.rows()[0][0], is("Trillian"));
        assertThat(response.rows()[0][1], is(0.15342641f));
        assertThat(response.rows()[1][0], is("Arthur"));
        assertThat(response.rows()[1][1], is(0.13424811f));
    }

    @Test
    public void testCopyValuesFromStringArrayToIndex() throws Exception {
        execute("CREATE TABLE t_string_array (" +
                "  id INTEGER PRIMARY KEY," +
                "  keywords ARRAY(STRING) INDEX USING FULLTEXT," +
                "  INDEX keywords_ft USING FULLTEXT(keywords)" +
                ")");

        execute("INSERT INTO t_string_array (id, keywords) VALUES (1, ['foo', 'bar', 'foo bar'])");
        ensureYellow();
        refresh();
        execute("SELECT id, keywords FROM t_string_array WHERE match(keywords_ft, 'foo')");
        assertThat(response.rows().length, is(1));

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("t_string_array");

        // we need to use ES search request to aggregate on terms
        String q = "{\n" +
                   "    \"from\": 0,\n" +
                   "    \"query\": {\n" +
                   "      \"match_all\": {}\n" +
                   "    },\n" +
                   "    \"aggregations\": {\n" +
                   "        \"kw\": {\n" +
                   "            \"terms\": {\"field\": \"keywords_ft\"}\n" +
                   "        }\n" +
                   "    },\n" +
                   "    \"size\": 0\n" +
                   "}";
        BytesReference bytesRef = new BytesArray(q);
        searchRequest.source(bytesRef);
        SearchResponse searchResponse = client().search(searchRequest).get();
        StringTerms terms = searchResponse.getAggregations().get("kw");
        List<String> termValues = new ArrayList<>(terms.getBuckets().size());
        terms.getBuckets().forEach(b -> termValues.add(b.getKeyAsString()));
        assertThat(termValues, containsInAnyOrder("foo", "bar"));
    }
}
