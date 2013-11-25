package org.cratedb.integrationtests;

import org.cratedb.SQLCrateClusterTest;
import org.cratedb.sql.SQLParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class GroupByAggregateTest extends SQLCrateClusterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    public void setUpEmployees() {
        execute("create table employees (" +
                " name string, " +
                " departement string," +
                " age short," +
                " income double" +
                ") replicas 0");
        ensureGreen();
        execute("insert into employees (name, departement, age, income) values (?, ?, ?)",
                new Object[]{"dilbert", "engineering", 47, 4.000});
        execute("insert into employees (name, departement, age, income) values (?, ?, ?)",
                new Object[]{"wally", "engineering", 54, 6.000});
        execute("insert into employees (name, departement, age, income) values (?, ?, ?)",
                new Object[]{"pointy haired boss", "management", 45, Double.MAX_VALUE});
        execute("insert into employees (name, departement, age, income) values (?, ?, ?)",
                new Object[]{"catbert", "HR", 12, 999999999.99});
        execute("insert into employees (name, departement, income) values (?, ?, ?)",
                new Object[]{"ratbert", "HR", 0.50});
        execute("insert into employees (name, departement, age) values (?, ?)",
                new Object[]{"asok", "internship", 28});
        refresh();
    }

    @Test
    public void testMinAggWithoutArgs() throws Exception {
        setUpEmployees();

        expectedException.expect(SQLParseException.class);
        execute("select min() from employees group by departement");
    }

    @Test
    public void testCountAggWithoutArgs() throws Exception {
        setUpEmployees();

        expectedException.expect(SQLParseException.class);
        execute("select count() from employees group by departement");
    }

    @Test
    public void selectGroupByAggregateMinStar() throws Exception {
        expectedException.expect(SQLParseException.class);

        execute("select min(*) from characters group by gender");
    }
}
