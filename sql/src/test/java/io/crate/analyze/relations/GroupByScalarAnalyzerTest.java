package io.crate.analyze.relations;

import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;


public class GroupByScalarAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor executor;

    @Before
    public void prepare() {
        executor = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testScalarFunctionArgumentsNotAllInGroupByThrowsException() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("column '(id * other_id)' must appear in the GROUP BY clause or be used in an aggregation function");
        executor.analyze("select id * other_id from users group by id");
    }

    @Test
    public void testValidGroupByWithScalarAndMultipleColumns() throws Exception {
        SelectAnalyzedStatement statement = executor.analyze("select id * other_id from users group by id, other_id");
        assertThat(statement.relation().fields().get(0).path().outputName(), is("(id * other_id)"));
    }

    @Test
    public void testValidGroupByWithScalar() throws Exception {
        SelectAnalyzedStatement statement = executor.analyze("select id * 2 from users group by id");
        assertThat(statement.relation().fields().get(0).path().outputName(), is("(id * 2)"));
    }

    @Test
    public void testValidGroupByWithMultipleScalarFunctions() throws Exception {
        SelectAnalyzedStatement statement = executor.analyze("select abs(id * 2) from users group by id");
        assertThat(statement.relation().fields().get(0).path().outputName(), is("abs((id * 2))"));
    }
}
