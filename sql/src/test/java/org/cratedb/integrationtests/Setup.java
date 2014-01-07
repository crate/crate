package org.cratedb.integrationtests;

import org.cratedb.SQLTransportIntegrationTest;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class Setup {

    private final SQLTransportIntegrationTest test;

    public Setup(SQLTransportIntegrationTest test) {
        this.test = test;
    }

    public void setUpLocations() throws Exception {
        test.execute("create table locations (" +
                " id string primary key," +
                " name string," +
                " date timestamp," +
                " kind string," +
                " position integer," +
                " description string," +
                " race object," +
                " index name_description_ft using fulltext(name, description) with (analyzer='english')" +
                ") clustered by(id) into 2 shards replicas 0");

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
                            put("interests", new String[]{"netball"});
                        }}
                },
                new Object[]{
                        "12",  "", "2013-07-16",  "Planet",  5,  "This Planet doesn't really exist", null
                },
                new Object[]{
                        "13",  null, "2013-07-16",  "Galaxy",  6,  "The end of the Galaxy.%", null
                }
        };
        for (Object[] args : rows) {
            test.execute(insertStmt, args);
        }
    }

    public void groupBySetup() throws Exception {
        groupBySetup("integer");
    }

    public void groupBySetup(String numericType) throws Exception {

        test.execute(String.format("create table characters (" +
                " race string," +
                " gender string," +
                " age %s," +
                " birthdate timestamp," +
                " name string," +
                " details object" +
                ")", numericType));
        test.ensureGreen();

        Map<String, String> details = newHashMap();
        details.put("job", "Sandwitch Maker");
        test.execute("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
                new Object[]{"Human", "male", 34, "1975-10-01", "Arthur Dent", details});

        details = newHashMap();
        details.put("job", "Mathematician");
        test.execute("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
                new Object[] {"Human", "female", 32, "1978-10-11", "Trillian", details});
        test.execute("insert into characters (race, gender, age, birthdate, name, details) values (?, ?, ?, ?, ?, ?)",
                new Object[] {"Human", "female", 43, "1970-01-01", "Anjie", null});
        test.execute("insert into characters (race, gender, age, name) values (?, ?, ?, ?)",
                new Object[] {"Human", "male", 112, "Ford Perfect"});

        test.execute("insert into characters (race, gender, name) values ('Android', 'male', 'Marving')");
        test.execute("insert into characters (race, gender, name) values ('Vogon', 'male', 'Jeltz')");
        test.execute("insert into characters (race, gender, name) values ('Vogon', 'male', 'Kwaltz')");
        test.refresh();
    }

    public void setUpEmployees() {
        test.execute("create table employees (" +
                " name string, " +
                " department string," +
                " hired timestamp, " +
                " age short," +
                " income double, " +
                " good boolean" +
                ") replicas 0");
        test.ensureGreen();
        test.execute("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
                new Object[]{"dilbert", "engineering", "1985-01-01", 47, 4000.0, true});
        test.execute("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
                new Object[]{"wally", "engineering", "2000-01-01", 54, 6000.0, true});
        test.execute("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
                new Object[]{"pointy haired boss", "management", "2010-10-10", 45, Double.MAX_VALUE, false});

        test.execute("insert into employees (name, department, hired, age, income, good) values (?, ?, ?, ?, ?, ?)",
                new Object[]{"catbert", "HR", "1990-01-01", 12, 999999999.99, false});
        test.execute("insert into employees (name, department, income) values (?, ?, ?)",
                new Object[]{"ratbert", "HR", 0.50});
        test.execute("insert into employees (name, department, age) values (?, ?, ?)",
                new Object[]{"asok", "internship", 28});
        test.refresh();
    }

    public void setUpObjectTable() {
        test.execute("create table ot (" +
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
                ")");
        test.execute("insert into ot (title, author, details) values (?, ?, ?)",
                new Object[]{
                    "The Hitchhiker's Guide to the Galaxy",
                    new HashMap<String, Object>(){{
                        put("name", new HashMap<String, Object>(){{
                            put("first_name", "Douglas");
                            put("last_name", "Adams");
                        }});
                        put("age", 49);
                    }},
                    new HashMap<String, Object>(){{
                        put("num_pages", 224);
                    }}
                });
        test.refresh();
    }
}
