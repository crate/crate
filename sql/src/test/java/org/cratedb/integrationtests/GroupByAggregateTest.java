package org.cratedb.integrationtests;

import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLResponse;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.isIn;

public class GroupByAggregateTest extends SQLTransportIntegrationTest {

    private SQLResponse response;
    private Setup setup = new Setup(this);
    private boolean setUpDone = false;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();


    @Before
    public void initTestData() {
        if (!setUpDone) {
            this.setup.setUpEmployees();
            setUpDone = true;
        }
    }

    /**
     * override execute to store response in property for easier access
     */
    @Override
    public SQLResponse execute(String stmt, Object[] args) {
        response = super.execute(stmt, args);
        return response;
    }

    @Test
    public void selectGroupByAggregateMinInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select min(age) as minage, gender from characters group by gender order by gender");
        assertArrayEquals(new String[]{"minage", "gender"}, response.cols());
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(32, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(34, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select min(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("MIN(age)", response.cols()[0]);
        assertEquals(32.0f, response.rows()[0][0]);
        assertEquals(34.0f, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinDouble() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(32.0d, response.rows()[0][0]);
        assertEquals(34.0d, response.rows()[1][0]);
    }

    @Test
    public void testCountDistinctGlobal() throws Exception {
        execute("select count(distinct department), count(*) from employees");

        assertEquals(1L, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
        assertEquals(6L, response.rows()[0][1]);
    }

    @Test
    public void testCountDistinctGroupBy() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(*) desc");

        assertEquals(3L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
        assertEquals(4L, response.rows()[0][1]);
        assertEquals("Human", response.rows()[0][2]);

        assertEquals(1L, response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);
        assertEquals("Vogon", response.rows()[1][2]);

        assertEquals(1L, response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals("Android", response.rows()[2][2]);
    }

    @Test
    public void testCountDistinctGroupByOrderByCountDistinct() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select count(distinct gender), count(*), race from characters group by race order by count(distinct gender) desc");
        assertEquals(3L, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }

    @Test
    public void testCountDistinctManySame() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(*), count(distinct gender) " +
                "from characters group by race order by count(distinct gender) desc, " +
                "count(*) desc");

        assertEquals(3L, response.rowCount());

        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(4L, response.rows()[0][2]);
        assertEquals(2L, response.rows()[0][3]);

        assertEquals("Vogon", response.rows()[1][0]);
        assertEquals(1L, response.rows()[1][1]);
        assertEquals(2L, response.rows()[1][2]);
        assertEquals(1L, response.rows()[1][3]);

        assertEquals("Android", response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals(1L, response.rows()[2][2]);
        assertEquals(1L, response.rows()[2][3]);
    }

    @Test
    public void testCountDistinctManyDifferent() throws Exception {
        this.setup.groupBySetup("integer");
        execute("select race, count(distinct gender), count(distinct age) " +
                "from characters group by race order by count(distinct gender) desc, race desc");
        assertEquals(3L, response.rowCount());

        assertEquals("Human", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(4L, response.rows()[0][2]);


        assertEquals("Vogon", response.rows()[1][0]);
        assertEquals(1L, response.rows()[1][1]);
        assertEquals(0L, response.rows()[1][2]);

        assertEquals("Android", response.rows()[2][0]);
        assertEquals(1L, response.rows()[2][1]);
        assertEquals(0L, response.rows()[2][2]);
    }

    @Test
    public void selectGroupByAggregateMinOrderByMin() throws Exception {
        this.setup.groupBySetup("double");

        execute("select min(age) as minAge, gender from characters group by gender order by minAge desc");
        assertEquals(2L, response.rowCount());

        assertEquals("male", response.rows()[0][1]);
        assertEquals(34.0d, response.rows()[0][0]);

        assertEquals("female", response.rows()[1][1]);
        assertEquals(32.0d, response.rows()[1][0]);

        execute("select min(age), gender from characters group by gender order by min(age) asc");
        assertEquals(2L, response.rowCount());

        assertEquals("female", response.rows()[0][1]);
        assertEquals(32.0d, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(34.0d, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select min(age) as minAge, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(32L, response.rows()[0][0]);
        assertEquals(34L, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinString() throws Exception {
        this.setup.groupBySetup();

        execute("select min(name) as minName, gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("Anjie", response.rows()[0][0]);
        assertEquals("Arthur Dent", response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMinDate() throws Exception {
        this.setup.groupBySetup();

        execute("select min(birthdate) as minBirthdate, gender from characters group by gender " +
                "order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(0L, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(181353600000L, response.rows()[1][0]);
    }

    // MAX

    @Test
    public void selectGroupByAggregateMaxString() throws Exception {
        this.setup.groupBySetup();

        execute("select max(name), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("Trillian", response.rows()[0][0]);
        assertEquals("Marving", response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxLong() throws Exception {
        this.setup.groupBySetup("long");

        execute("select max(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals(43L, response.rows()[0][0]);
        assertEquals(112L, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxDate() throws Exception {

        execute("select max(hired), department from employees group by department " +
                "order by department asc");
        assertEquals(4L, response.rowCount());

        assertEquals("HR", response.rows()[0][1]);
        assertEquals(631152000000L, response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(946684800000L, response.rows()[1][0]);

        assertEquals("internship", response.rows()[2][1]);
        assertEquals(null, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(1286668800000L, response.rows()[3][0]);
    }

    @Test
    public void selectGroupByAggregateMaxInteger() throws Exception {
        this.setup.groupBySetup("integer");

        execute("select max(age), gender from characters group by gender order by gender");
        assertArrayEquals(new String[]{"MAX(age)", "gender"}, response.cols());
        assertEquals(2L, response.rowCount());
        assertEquals("female", response.rows()[0][1]);
        assertEquals(43, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(112, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxFloat() throws Exception {
        this.setup.groupBySetup("float");

        execute("select max(age), gender from characters group by gender order by gender");
        assertEquals(2L, response.rowCount());
        assertEquals("MAX(age)", response.cols()[0]);
        assertEquals(43.0f, response.rows()[0][0]);
        assertEquals(112.0f, response.rows()[1][0]);
    }

    @Test
    public void selectGroupByAggregateMaxDouble() throws Exception {

        execute("select max(income), department from employees group by department order by department");
        assertEquals(4L, response.rowCount());
        assertEquals(999999999.99d, response.rows()[0][0]);
        assertEquals("HR", response.rows()[0][1]);

        assertEquals(6000.0d, response.rows()[1][0]);
        assertEquals("engineering", response.rows()[1][1]);

        assertEquals(null, response.rows()[2][0]);
        assertEquals("internship", response.rows()[2][1]);

        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
        assertEquals("management", response.rows()[3][1]);
    }

    @Test
    public void selectGroupByAggregateMaxOrderByMax() throws Exception {
        this.setup.groupBySetup("double");

        execute("select max(age) as maxage, gender from characters group by gender order by maxage desc");
        assertEquals(2L, response.rowCount());

        assertEquals("male", response.rows()[0][1]);
        assertEquals(112.0d, response.rows()[0][0]);

        assertEquals("female", response.rows()[1][1]);
        assertEquals(43.0d, response.rows()[1][0]);

        execute("select max(age), gender from characters group by gender order by max(age) asc");
        assertEquals(2L, response.rowCount());

        assertEquals("female", response.rows()[0][1]);
        assertEquals(43.0d, response.rows()[0][0]);

        assertEquals("male", response.rows()[1][1]);
        assertEquals(112.0d, response.rows()[1][0]);
    }
    @Test
    public void testGroupByAggSumDouble() throws Exception {

        execute("select sum(income), department from employees group by department order by sum(income) asc");
        assertEquals(4, response.rowCount());

        assertEquals("internship", response.rows()[0][1]);
        assertNull(response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(10000.0, response.rows()[1][0]);

        assertEquals("HR", response.rows()[2][1]);
        assertEquals(1000000000.49, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
    }

    @Test
    public void testGroupByAggSumShort() throws Exception {

        execute("select sum(age), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][1]);
        assertEquals(12.0d, response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(101.0d, response.rows()[1][0]);

        assertEquals("internship", response.rows()[2][1]);
        assertEquals(28.0d, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(45.0d, response.rows()[3][0]);

    }

    @Test
    public void testGroupByAvgDouble() throws Exception {
        execute("select avg(income), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][1]);
        assertEquals(500000000.245d, response.rows()[0][0]);

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(5000.0d, response.rows()[1][0]);

        assertEquals("internship", response.rows()[2][1]);
        assertEquals(null, response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
    }

    @Test
    public void testGroupByAvgMany() throws Exception {
        execute("select avg(income), avg(age), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][2]);
        assertEquals(500000000.245d, response.rows()[0][0]);
        assertEquals(12.0d, response.rows()[0][1]);

        assertEquals("engineering", response.rows()[1][2]);
        assertEquals(5000.0d, response.rows()[1][0]);
        assertEquals(50.5d, response.rows()[1][1]);

        assertEquals("internship", response.rows()[2][2]);
        assertEquals(null, response.rows()[2][0]);
        assertEquals(28.0d, response.rows()[2][1]);

        assertEquals("management", response.rows()[3][2]);
        assertEquals(Double.MAX_VALUE, response.rows()[3][0]);
        assertEquals(45.0d, response.rows()[3][1]);
    }

    @Test
    public void testGroupByAny() throws Exception {
        this.setup.groupBySetup();

        execute("select any(name), race from characters group by race order by race asc");
        SQLResponse any_response = response;
        assertEquals(3, any_response.rowCount());

        assertEquals("Android", any_response.rows()[0][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Android", any_response.rows()[0][0]})
                        .rowCount()
        );
        assertEquals("Human", any_response.rows()[1][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Human", any_response.rows()[1][0]})
                        .rowCount()
        );
        assertEquals("Vogon", any_response.rows()[2][1]);
        assertEquals(1,
                execute("select name from characters where race=? AND name=? ",
                        new Object[]{"Vogon", any_response.rows()[2][0]})
                        .rowCount()
        );

    }

    @Test
    public void testGlobalAggregateAny() throws Exception {
        this.setup.groupBySetup();

        execute("select any(age) from characters");
        assertEquals(1, response.rowCount());
        assertEquals(1,
                execute("select count(*) from characters where age=?",
                        new Object[]{response.rows()[0][0]})
                        .rowCount()
        );
    }

    @Test
    public void testAggregateAnyOnBoolean() throws Exception {

        execute("select any(good) from employees");
        assertEquals(1, response.rowCount());
        assertThat(response.rows()[0][0], isIn(new Object[]{true, false, null}));

        execute("select any(good) from employees where name='dilbert'");
        assertEquals(1, response.rowCount());
        assertEquals(true, response.rows()[0][0]);

        execute("select any(good), department from employees group by department order by department asc");
        assertEquals(4, response.rowCount());
        assertEquals("HR", response.rows()[0][1]);
        assertThat(response.rows()[0][0], isIn(new Object[]{false, null}));

        assertEquals("engineering", response.rows()[1][1]);
        assertEquals(true, response.rows()[1][0]);  // by accident only single values exist in group

        assertEquals("internship", response.rows()[2][1]);
        assertNull(response.rows()[2][0]);

        assertEquals("management", response.rows()[3][1]);
        assertEquals(false, response.rows()[3][0]);
    }

    public void testGroupByCountOnColumn() throws Exception {
        execute("select department, count(income), count(*) " +
                "from employees group by department order by department asc");
        assertEquals(4, response.rowCount());

        assertEquals("HR", response.rows()[0][0]);
        assertEquals(2L, response.rows()[0][1]);
        assertEquals(2L, response.rows()[0][2]);

        assertEquals("engineering", response.rows()[1][0]);
        assertEquals(2L, response.rows()[1][1]);
        assertEquals(2L, response.rows()[1][2]);

        assertEquals("internship", response.rows()[2][0]);
        assertEquals(0L, response.rows()[2][1]);
        assertEquals(1L, response.rows()[2][2]);

        assertEquals("management", response.rows()[3][0]);
        assertEquals(1L, response.rows()[3][1]);
        assertEquals(1L, response.rows()[3][2]);
    }

    @Test
    public void testGlobalCountOnColumn() throws Exception {

        execute("select count(*), count(good), count(distinct good) from employees");
        assertEquals(1, response.rowCount());
        assertEquals(6L, response.rows()[0][0]);
        assertEquals(4L, response.rows()[0][1]);
        assertEquals(2L, response.rows()[0][2]);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {

        execute("select count(distinct good) from employees");
        assertEquals(1, response.rowCount());
        assertEquals(2L, response.rows()[0][0]);
    }


}
