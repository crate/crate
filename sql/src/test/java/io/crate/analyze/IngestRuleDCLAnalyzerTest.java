package io.crate.analyze;


import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

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
    }


    @Test
    public void testCreateRuleSimple() {
        CreateIngestionRuleAnalysedStatement analysis = e.analyze("CREATE INGEST RULE v4 ON mqtt WHERE topic = 1 INTO t3");
        assertThat(analysis.ruleName(), is("v4"));
        assertThat(analysis.sourceName(), is("mqtt"));
        assertThat(analysis.targetTable().toString(), is("doc.t3"));
        assertThat(analysis.whereClause(), is("(\"topic\" = 1)"));
    }

    @Test
    public void testCreateRuleSimpleMissingWhereClause() {
        CreateIngestionRuleAnalysedStatement analysis = e.analyze("CREATE INGEST RULE v4 ON mqtt INTO t3");
        assertThat(analysis.ruleName(), is("v4"));
        assertThat(analysis.sourceName(), is("mqtt"));
        assertThat(analysis.targetTable().toString(), is("doc.t3"));
        assertThat(analysis.whereClause(), is(""));
    }

    @Test
    public void testCreateRuleIntoUnknownTableThrowsException() {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Table 'doc.hoichi' unknown");
        e.analyze("CREATE INGEST RULE v4 ON mqtt INTO doc.hoichi");
    }

}
