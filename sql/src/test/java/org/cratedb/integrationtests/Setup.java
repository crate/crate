package org.cratedb.integrationtests;

import org.cratedb.SQLCrateClusterTest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.cratedb.test.integration.PathAccessor.stringFromPath;

public class Setup {

    private final SQLCrateClusterTest test;

    public Setup(SQLCrateClusterTest test) {
        this.test = test;
    }

    public void setUpLocations() throws Exception {
        test.prepareCreate("locations").setSettings(
                test.randomSettingsBuilder().loadFromClasspath("/essetup/settings/test_a.json").build())
                .addMapping("default", stringFromPath("/essetup/mappings/test_a.json", Setup.class))
                .execute().actionGet();
        test.loadBulk("/essetup/data/test_a.json", Setup.class);
        test.refresh();
    }

    public void groupBySetup() throws Exception {
        groupBySetup("integer");
    }

    public void groupBySetup(String numericType) throws Exception {

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("default")
                    .startObject("properties")
                        .startObject("race")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("gender")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("age")
                            .field("type", numericType)
                        .endObject()
                        .startObject("birthdate")
                            .field("type", "date")
                        .endObject()
                        .startObject("name")
                            .field("type", "string")
                            .field("index", "not_analyzed")
                        .endObject()
                        .startObject("details")
                            .field("type", "object")
                            .field("index", "not_analyzed")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();


        test.prepareCreate("characters")
                .addMapping("default", mapping)
                .execute().actionGet();
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
}
