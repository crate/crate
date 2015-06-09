/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.Constants;
import io.crate.action.sql.SQLResponse;
import io.crate.testing.SQLTransportExecutor;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class Setup {

    private final SQLTransportExecutor transportExecutor;

    public Setup(SQLTransportExecutor transportExecutor) {
        this.transportExecutor = transportExecutor;
    }

    public void setUpLocations() throws Exception {
        transportExecutor.exec("create table locations (" +
            " id string primary key," +
            " name string," +
            " date timestamp," +
            " kind string," +
            " position integer," +
            " description string," +
            " race object," +
            " index name_description_ft using fulltext(name, description) with (analyzer='english')" +
            ") clustered by(id) into 2 shards with(number_of_replicas=0)");

        String insertStmt = "insert into locations " +
                "(id, name, date, kind, position, description, race) " +
                "values (?, ?, ?, ?, ?, ?, ?)";
        Object[][] rows = new Object[][]{
                new Object[]{"1", "North West Ripple", "1979-10-12",
                        "Galaxy", 1, "Relative to life on NowWhat, living on an affluent " +
                        "world in the North West ripple of the Galaxy is said to be easier " +
                        "by a factor of about seventeen million.", null
                },
                new Object[]{
                        "2", "Outer Eastern Rim", "1979-10-12", "Galaxy", 2, "The Outer Eastern Rim " +
                        "of the Galaxy where the Guide has supplanted the Encyclopedia Galactica " +
                        "among its more relaxed civilisations.", null
                },
                new Object[]{
                        "3","Galactic Sector QQ7 Active J Gamma", "2013-05-01",  "Galaxy",  4,
                        "Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, " +
                        "the planet Preliumtarn of the famed Sevorbeupstry and " +
                        "Quentulus Quazgar Mountains.", null
                },
                new Object[]{
                        "4", "Aldebaran", "2013-07-16",  "Star System",  1,
                        "Max Quordlepleen claims that the only thing left after the end " +
                        "of the Universe will be the sweets trolley and a fine selection " +
                        "of Aldebaran liqueurs.", null
                },
                new Object[]{
                        "5",  "Algol", "2013-07-16",  "Star System",  2,
                        "Algol is the home of the Algolian Suntiger, " +
                        "the tooth of which is one of the ingredients of the " +
                        "Pan Galactic Gargle Blaster.", null
                },
                new Object[]{
                        "6",  "Alpha Centauri", "1979-10-12",  "Star System",  3,
                        "4.1 light-years northwest of earth", null
                },
                new Object[]{
                        "7",  "Altair", "2013-07-16",  "Star System",  4,
                        "The Altairian dollar is one of three freely convertible currencies in the galaxy, " +
                        "though by the time of the novels it had apparently recently collapsed.",
                        null
                },
                new Object[]{
                        "8",  "Allosimanius Syneca", "2013-07-16",  "Planet",  1,
                        "Allosimanius Syneca is a planet noted for ice, snow, " +
                        "mind-hurtling beauty and stunning cold.", null
                },
                new Object[]{
                        "9",  "Argabuthon", "2013-07-16",  "Planet",  2,
                        "It is also the home of Prak, a man placed into solitary confinement " +
                        "after an overdose of truth drug caused him to tell the Truth in its absolute " +
                        "and final form, causing anyone to hear it to go insane.", null,
                },
                new Object[]{
                        "10",  "Arkintoofle Minor", "1979-10-12",  "Planet",  3,
                        "Motivated by the fact that the only thing in the Universe that " +
                        "travels faster than light is bad news, the Hingefreel people native " +
                        "to Arkintoofle Minor constructed a starship driven by bad news.", null
                },
                new Object[]{
                        "11",  "Bartledan", "2013-07-16",  "Planet",  4,
                        "An Earthlike planet on which Arthur Dent lived for a short time, " +
                                "Bartledan is inhabited by Bartledanians, a race that appears human but only physically.",
                        new HashMap<String, Object>(){{
                            put("name", "Bartledannians");
                            put("description", "Similar to humans, but do not breathe");
                            put("interests", "netball");
                        }}
                },
                new Object[]{
                        "12",  "", "2013-07-16",  "Planet",  5,  "This Planet doesn't really exist", null
                },
                new Object[]{
                        "13",  "End of the Galaxy", "2013-07-16",  "Galaxy",  6,  "The end of the Galaxy.%", null
                }
        };
        transportExecutor.exec(insertStmt, rows);
        transportExecutor.refresh("locations");
    }

    public void groupBySetup() throws Exception {
        groupBySetup("integer");
    }

    public void groupBySetup(String numericType) throws Exception {
        transportExecutor.exec(String.format("create table characters (" +
            " race string," +
            " gender string," +
            " age %s," +
            " birthdate timestamp," +
            " name string," +
            " details object as (job string)," +
            " details_ignored object(ignored)" +
            ")", numericType));
        transportExecutor.ensureYellowOrGreen();

        Map<String, String> details = newHashMap();
        details.put("job", "Sandwitch Maker");
        transportExecutor.exec("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"Human", "male", 34, "1975-10-01", "Arthur Dent", details});

        details = newHashMap();
        details.put("job", "Mathematician");
        transportExecutor.exec("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"Human", "female", 32, "1978-10-11", "Trillian", details});
        transportExecutor.exec("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"Human", "female", 43, "1970-01-01", "Anjie", null});
        transportExecutor.exec("insert into characters (race, gender, age, name) values (?, ?, ?, ?)",
            new Object[]{"Human", "male", 112, "Ford Perfect"});

        transportExecutor.exec("insert into characters (race, gender, name) values ('Android', 'male', 'Marving')");
        transportExecutor.exec("insert into characters (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        transportExecutor.exec("insert into characters (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        transportExecutor.refresh("characters");
    }

    public void setUpEmployees() {
        transportExecutor.exec("create table employees (" +
            " name string, " +
            " department string," +
            " hired timestamp, " +
            " age short," +
            " income double, " +
            " good boolean" +
            ") with (number_of_replicas=0)");
        transportExecutor.ensureGreen();
        transportExecutor.exec("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"dilbert", "engineering", "1985-01-01", 47, 4000.0, true});
        transportExecutor.exec("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"wally", "engineering", "2000-01-01", 54, 6000.0, true});
        transportExecutor.exec("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"pointy haired boss", "management", "2010-10-10", 45, Double.MAX_VALUE, false});

        transportExecutor.exec("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"catbert", "HR", "1990-01-01", 12, 999999999.99, false});
        transportExecutor.exec("insert into employees (name, department, income) values (?, ?, ?)",
            new Object[]{"ratbert", "HR", 0.50});
        transportExecutor.exec("insert into employees (name, department, age) values (?, ?, ?)",
            new Object[]{"asok", "internship", 28});
        transportExecutor.refresh("employees");
    }

    public void setUpObjectTable() {
        transportExecutor.exec("create table ot (" +
                "  title string," +
                "  author object(dynamic) as (" +
                "    name object(strict) as (" +
                "      first_name string," +
                "      last_name string" +
                "    )," +
                "    age integer" +
                "  )," +
                "  details object(ignored) as (" +
                "    num_pages integer" +
                "  )" +
                ") with (number_of_replicas = 0)");
        transportExecutor.exec("insert into ot (title, author, details) values (?, ?, ?)",
                new Object[]{
                        "The Hitchhiker's Guide to the Galaxy",
                        new HashMap<String, Object>() {{
                            put("name", new HashMap<String, Object>() {{
                                put("first_name", "Douglas");
                                put("last_name", "Adams");
                            }});
                            put("age", 49);
                        }},
                        new HashMap<String, Object>() {{
                            put("num_pages", 224);
                        }}
                }
        );
        transportExecutor.refresh("ot");
    }

    public void setUpObjectMappingWithUnknownTypes() throws Exception {
        transportExecutor.prepareCreate("ut")
                .setSettings(ImmutableSettings.builder().put("number_of_replicas", 0).put("number_of_shards", 2).build())
                .addMapping(Constants.DEFAULT_MAPPING_TYPE, new HashMap<String, Object>(){{
                    put("properties", new HashMap<String, Object>(){{
                        put("name", new HashMap<String, Object>(){{
                            put("type", "string");
                            put("store", "false");
                            put("index", "not_analyzed");
                        }});
                        put("location", new HashMap<String, Object>(){{
                            put("type", "geo_shape");
                        }});
                        put("o", new HashMap<String, Object>(){{
                            put("type", "object");
                        }});
                        put("population", new HashMap<String, Object>(){{
                            put("type", "long");
                            put("store", "false");
                            put("index", "not_analyzed");
                        }});
                    }});
                }}).execute().actionGet();
        transportExecutor.client().prepareIndex("ut", Constants.DEFAULT_MAPPING_TYPE, "id1")
                .setSource("{\"name\":\"Berlin\",\"location\":{\"type\": \"point\", \"coordinates\": [52.5081, 13.4416]}, \"population\":3500000}")
                .execute().actionGet();
        transportExecutor.client().prepareIndex("ut", Constants.DEFAULT_MAPPING_TYPE, "id2")
                .setSource("{\"name\":\"Dornbirn\",\"location\":{\"type\": \"point\", \"coordinates\": [47.3904,9.7562]}, \"population\":46080}")
                .execute().actionGet();
        transportExecutor.refresh("ut");
    }

    public void setUpArrayTables() {
        transportExecutor.exec("create table any_table (" +
                "  id int primary key," +
                "  temps array(double)," +
                "  names array(string)," +
                "  tags array(string)" +
                ") with (number_of_replicas=0)");
        transportExecutor.ensureGreen();
        SQLResponse response = transportExecutor.exec("insert into any_table (id, temps, names, tags) values (?,?,?,?), (?,?,?,?), (?,?,?,?), (?,?,?,?)",
                        1, Arrays.asList(0L, 0L, 0L), Arrays.asList("Dornbirn", "Berlin", "St. Margrethen"), Arrays.asList("cool"),
                        2, Arrays.asList(0, 1, -1), Arrays.asList("Dornbirn", "Dornbirn", "Dornbirn"), Arrays.asList("cool", null),
                        3, Arrays.asList(42, -42), Arrays.asList("Hangelsberg", "Berlin"), Arrays.asList("kuhl", "cool"),
                        4, null, null, Arrays.asList("kuhl", null)
                );
        assertThat(response.rowCount(), is(4L));
        transportExecutor.refresh("any_table");
    }

    public void partitionTableSetup() {
        transportExecutor.exec("create table parted (" +
                "id int primary key," +
                "date timestamp primary key," +
                "o object(ignored)" +
                ") partitioned by (date) with (number_of_replicas=0)");
        transportExecutor.ensureGreen();
        transportExecutor.exec("insert into parted (id, date) values (1, '2014-01-01')");
        transportExecutor.exec("insert into parted (id, date) values (2, '2014-01-01')");
        transportExecutor.exec("insert into parted (id, date) values (3, '2014-02-01')");
        transportExecutor.exec("insert into parted (id, date) values (4, '2014-02-01')");
        transportExecutor.exec("insert into parted (id, date) values (5, '2014-02-01')");
        transportExecutor.refresh("parted");
    }

    public void createTestTableWithPrimaryKey() {
        transportExecutor.exec("create table test (" +
                "  pk_col string primary key, " +
                "  message string" +
                ") with (number_of_replicas=0)");
        transportExecutor.ensureGreen();
    }

    public void setUpCharacters() {
        transportExecutor.exec("create table characters (id int primary key, name string, female boolean, details object)");
        transportExecutor.ensureGreen();
        transportExecutor.exec("insert into characters (id, name, female) values (?, ?, ?)",
                new Object[][]{
                        new Object[] { 1, "Arthur", false},
                        new Object[] { 2, "Ford", false},
                        new Object[] { 3, "Trillian", true},
                        new Object[] { 4, "Arthur", true}
                }
        );
        transportExecutor.refresh("characters");
    }

    public void setUpPartitionedTableWithName() {
        transportExecutor.exec("create table parted (id int, name string, date timestamp) partitioned by (date)");
        transportExecutor.ensureGreen();
        transportExecutor.exec("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Trillian", null,
                        2, null, 0L,
                        3, "Ford", 1396388720242L
                });
        transportExecutor.ensureGreen();
        transportExecutor.refresh("parted");
    }
}
