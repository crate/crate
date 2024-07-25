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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.testing.SQLResponse;
import io.crate.testing.SQLTransportExecutor;

public class Setup {

    private final SQLTransportExecutor transportExecutor;

    public Setup(SQLTransportExecutor transportExecutor) {
        this.transportExecutor = transportExecutor;
    }

    public void setUpLocationsWithFTIndex() throws Exception {
        transportExecutor.exec("create table locations (" +
                               " id string primary key," +
                               " name string," +
                               " date timestamp with time zone," +
                               " kind string," +
                               " position integer," +
                               " description string," +
                               " race object," +
                               " index name_description_ft using fulltext(name, description) with (analyzer='english')" +
                               ") clustered by(id) into 2 shards with(number_of_replicas=0)");
        insertLocations();
    }

    public void setUpLocations() throws Exception {
        transportExecutor.exec("create table locations (" +
                               " id string primary key," +
                               " name string," +
                               " date timestamp with time zone," +
                               " kind string," +
                               " position integer," +
                               " description string," +
                               " race object" +
                               ") clustered by(id) into 2 shards with(number_of_replicas=0)");
        insertLocations();
    }


    private void insertLocations() throws Exception {
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
                "3", "Galactic Sector QQ7 Active J Gamma", "2013-05-01", "Galaxy", 4,
                "Galactic Sector QQ7 Active J Gamma contains the Sun Zarss, " +
                    "the planet Preliumtarn of the famed Sevorbeupstry and " +
                    "Quentulus Quazgar Mountains.", null
            },
            new Object[]{
                "4", "Aldebaran", "2013-07-16", "Star System", 1,
                "Max Quordlepleen claims that the only thing left after the end " +
                    "of the Universe will be the sweets trolley and a fine selection " +
                    "of Aldebaran liqueurs.", null
            },
            new Object[]{
                "5", "Algol", "2013-07-16", "Star System", 2,
                "Algol is the home of the Algolian Suntiger, " +
                    "the tooth of which is one of the ingredients of the " +
                    "Pan Galactic Gargle Blaster.", null
            },
            new Object[]{
                "6", "Alpha Centauri", "1979-10-12", "Star System", 3,
                "4.1 light-years northwest of earth", null
            },
            new Object[]{
                "7", "Altair", "2013-07-16", "Star System", 4,
                "The Altairian dollar is one of three freely convertible currencies in the galaxy, " +
                    "though by the time of the novels it had apparently recently collapsed.",
                null
            },
            new Object[]{
                "8", "Allosimanius Syneca", "2013-07-16", "Planet", 1,
                "Allosimanius Syneca is a planet noted for ice, snow, " +
                    "mind-hurtling beauty and stunning cold.", null
            },
            new Object[]{
                "9", "Argabuthon", "2013-07-16", "Planet", 2,
                "It is also the home of Prak, a man placed into solitary confinement " +
                    "after an overdose of truth drug caused him to tell the Truth in its absolute " +
                    "and final form, causing anyone to hear it to go insane.", null,
            },
            new Object[]{
                "10", "Arkintoofle Minor", "1979-10-12", "Planet", 3,
                "Motivated by the fact that the only thing in the Universe that " +
                    "travels faster than light is bad news, the Hingefreel people native " +
                    "to Arkintoofle Minor constructed a starship driven by bad news.", null
            },
            new Object[]{
                "11", "Bartledan", "2013-07-16", "Planet", 4,
                "An Earthlike planet on which Arthur Dent lived for a short time, " +
                    "Bartledan is inhabited by Bartledanians, a race that appears human but only physically.",
                Map.of(
                    "name", "Bartledannians",
                    "description", "Similar to humans, but do not breathe",
                    "interests", "netball")
            },
            new Object[]{
                "12", "", "2013-07-16", "Planet", 5, "This Planet doesn't really exist", null
            },
            new Object[]{
                "13", "End of the Galaxy", "2013-07-16", "Galaxy", 6, "The end of the Galaxy.%", null
            }
        };
        transportExecutor.execBulk(insertStmt, rows);
    }

    public void groupBySetup() throws Exception {
        groupBySetup("integer");
    }

    public void groupBySetup(String numericType) {
        transportExecutor.exec(String.format(Locale.ENGLISH, "create table characters (" +
                                                             " race string," +
                                                             " gender string," +
                                                             " age %s," +
                                                             " birthdate timestamp with time zone," +
                                                             " name string," +
                                                             " details object as (job string)," +
                                                             " details_ignored object(ignored)" +
                                                             ")", numericType));
        transportExecutor.ensureYellowOrGreen();

        Map<String, String> details = new HashMap<>();
        details.put("job", "Sandwitch Maker");
        transportExecutor.exec("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
            new Object[]{"Human", "male", 34, "1975-10-01", "Arthur Dent", details});

        details = new HashMap<>();
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
        transportExecutor.exec("refresh table characters");
    }

    public void setUpEmployees() {
        transportExecutor.exec("create table employees (" +
                               " name string, " +
                               " department string," +
                               " hired timestamp with time zone, " +
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
        transportExecutor.exec("refresh table employees");
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
                Map.of(
                    "name", Map.of(
                        "first_name", "Douglas",
                        "last_name", "Adams"),
                    "age", 49),
                Map.of("num_pages", 224)
            }
        );
        transportExecutor.exec("refresh table ot");
    }

    public void setUpArrayTables() {
        transportExecutor.exec("create table any_table (" +
                               "  id int primary key," +
                               "  temps array(double)," +
                               "  names array(string)," +
                               "  tags array(string)" +
                               ") with (number_of_replicas=0)");
        transportExecutor.ensureGreen();
        SQLResponse response =
            transportExecutor.exec(
                "insert into any_table (id, temps, names, tags) values (?,?,?,?), (?,?,?,?), (?,?,?,?), (?,?,?,?)",
                new Object[] {
                    1, Arrays.asList(0L, 0L, 0L), Arrays.asList("Dornbirn", "Berlin", "St. Margrethen"), Arrays.asList("cool"),
                    2, Arrays.asList(0, 1, -1), Arrays.asList("Dornbirn", "Dornbirn", "Dornbirn"), Arrays.asList("cool", null),
                    3, Arrays.asList(42, -42), Arrays.asList("Hangelsberg", "Berlin"), Arrays.asList("kuhl", "cool"),
                    4, null, null, Arrays.asList("kuhl", null)
                }
            );
        assertThat(response).hasRowCount(4L);
        transportExecutor.exec("refresh table any_table");
    }

    public void partitionTableSetup() {
        partitionTableSetup(null);
    }

    public void partitionTableSetup(@Nullable Integer numberOfShards) {
        String stmt = "create table parted (" +
            "id int primary key," +
            "date timestamp with time zone primary key," +
            "o object(ignored)) ";
        if (numberOfShards != null) {
            stmt += "clustered into " + numberOfShards + " shards ";
        }
        stmt += "partitioned by (date) with (number_of_replicas=0)";
        transportExecutor.exec(stmt);
        transportExecutor.ensureGreen();
        transportExecutor.exec("insert into parted (id, date) values (1, '2014-01-01')");
        transportExecutor.exec("insert into parted (id, date) values (2, '2014-01-01')");
        transportExecutor.exec("insert into parted (id, date) values (3, '2014-02-01')");
        transportExecutor.exec("insert into parted (id, date) values (4, '2014-02-01')");
        transportExecutor.exec("refresh table parted");
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
        transportExecutor.ensureYellowOrGreen();
        transportExecutor.execBulk("insert into characters (id, name, female) values (?, ?, ?)",
            new Object[][]{
                new Object[]{1, "Arthur", false},
                new Object[]{2, "Ford", false},
                new Object[]{3, "Trillian", true},
                new Object[]{4, "Arthur", true}
            }
        );
        transportExecutor.exec("refresh table characters");
    }

    public void setUpJobs() {
        transportExecutor.exec("create table jobs (id int primary key, department string, min_salary double, max_salary double)");
        transportExecutor.ensureYellowOrGreen();
        transportExecutor.execBulk("insert into jobs (id, department, min_salary, max_salary) values (?, ?, ?, ?)",
            new Object[][]{
                new Object[]{1, "engineering", 8200.0, 16000.0},
                new Object[]{2, "HR", 6000.0, 12000.0},
                new Object[]{3, "management", 20000.0, 40000.0},
                new Object[]{4, "internship", 3000.0, 6000.0}
            }
        );
        transportExecutor.exec("refresh table jobs");


        transportExecutor.exec(
            "create table job_history (" +
                "job_id int, " +
                "from_ts timestamp with time zone, " +
                "to_ts timestamp with time zone)"
        );
        transportExecutor.ensureYellowOrGreen();
        transportExecutor.execBulk("insert into job_history (job_id, from_ts, to_ts) values (?, ?, ?)",
            new Object[][]{
                new Object[]{1, "2017-01-01", "2017-02-02"},
                new Object[]{1, "2017-05-05", "2017-01-12"},
                new Object[]{2, "2015-12-01", "2016-10-25"},
                new Object[]{3, "2016-05-20", "2017-12-31"},
                new Object[]{4, "2014-11-11", "2016-04-04"}
            }
        );
        transportExecutor.exec("refresh table job_history");
    }
}
