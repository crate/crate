package io.crate.analyze;


import io.crate.exceptions.RelationUnknown;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class IngestRuleDCLAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void initExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testDropRuleSimple() {
        DropIngestionRuleAnalysedStatement analysis = e.analyze("DROP INGEST RULE v4");
        assertThat(analysis.ruleName(), is("v4"));
        analysis = e.analyze("DROP INGEST RULE \"V4\"");
        assertThat(analysis.ruleName(), is("V4"));
        assertThat(analysis.ifExists(), is(false));
    }

    @Test
    public void testDropRuleIfExists() {
        DropIngestionRuleAnalysedStatement analysis = e.analyze("DROP INGEST RULE v4");
        assertThat(analysis.ruleName(), is("v4"));
        analysis = e.analyze("DROP INGEST RULE IF EXISTS \"V4\"");
        assertThat(analysis.ruleName(), is("V4"));
        assertThat(analysis.ifExists(), is(true));
    }


    @Test
    public void testCreateRuleSimple() {
        CreateIngestionRuleAnalysedStatement analysis = e.analyze("CREATE INGEST RULE v4 ON mqtt WHERE topic = 1 INTO t3");
        assertThat(analysis.ruleName(), is("v4"));
        assertThat(analysis.sourceName(), is("mqtt"));
        assertThat(analysis.targetTable().toString(), is("doc.t3"));
        assertThat(analysis.whereClause(), notNullValue());
        //noinspection ConstantConditions
        assertThat(analysis.whereClause().toString(), is("\"topic\" = 1"));
    }

    @Test
    public void testRuleWhereClauseParameterIsNotReplaced() {
        CreateIngestionRuleAnalysedStatement analysis = e.analyze("CREATE INGEST RULE v4 ON mqtt WHERE topic = ? INTO t3",
            new Object[]{"topicValue"});
        assertThat(analysis.whereClause(), notNullValue());
        //noinspection ConstantConditions
        assertThat(analysis.whereClause().toString(), is("\"topic\" = $1"));
    }

    @Test
    public void testCreateRuleSimpleMissingWhereClause() {
        CreateIngestionRuleAnalysedStatement analysis = e.analyze("CREATE INGEST RULE v4 ON mqtt INTO t3");
        assertThat(analysis.ruleName(), is("v4"));
        assertThat(analysis.sourceName(), is("mqtt"));
        assertThat(analysis.targetTable().toString(), is("doc.t3"));
        assertThat(analysis.whereClause(), nullValue());
    }

    @Test
    public void testCreateRuleIntoUnknownTableThrowsException() {
        expectedException.expect(RelationUnknown.class);
        expectedException.expectMessage("Relation 'doc.hoichi' unknown");
        e.analyze("CREATE INGEST RULE v4 ON mqtt INTO doc.hoichi");
    }

}
