package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertVisitorTest {


    private ParsedStatement getParsedStatement(String sql, Object[] params) throws
            StandardException {

        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        NodeExecutionContext.TableExecutionContext tec = mock(
                NodeExecutionContext.TableExecutionContext.class);
        when(nec.tableContext("locations")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("name", "kind"));

        return new ParsedStatement(sql, params, nec);
    }


    @Test
    public void testInsertToCreateIndex() throws StandardException, IOException {

        String sql = "insert into locations values(?, ?)";
        Object[] params = new Object[]{"North West Ripple", "Galaxy"};

        ParsedStatement statement = getParsedStatement(sql, params);
        assertEquals(statement.type(), ParsedStatement.INSERT_ACTION);

        IndexRequest indexRequest = statement.buildIndexRequest();

        assertEquals("locations", indexRequest.index());
        assertEquals("{\"name\":\"North West Ripple\",\"kind\":\"Galaxy\"}",
                new String(indexRequest.source().toBytes()));
        assertEquals(IndexRequest.OpType.CREATE, indexRequest.opType());
    }

    @Test
    public void testMultiRowInsertToCreateIndex() throws Exception {

        String sql = "insert into locations values(?, ?), (?, ?)";
        Object[] params = new Object[]{"North West Ripple", "Galaxy", "Bartledan", "Planet"};

        ParsedStatement statement = getParsedStatement(sql, params);
        assertEquals(statement.type(), ParsedStatement.BULK_ACTION);

        BulkRequest bulkRequest = statement.buildBulkRequest();
        for (ActionRequest actionRequest : bulkRequest.requests()) {
            assertTrue(actionRequest instanceof IndexRequest);

            IndexRequest indexRequest = (IndexRequest)actionRequest;

            assertEquals("locations", indexRequest.index());
            assertEquals(IndexRequest.OpType.CREATE, indexRequest.opType());
        }
    }
}
