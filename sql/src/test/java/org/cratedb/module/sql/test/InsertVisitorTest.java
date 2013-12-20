package org.cratedb.module.sql.test;

import com.google.common.collect.ImmutableSet;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.service.SQLParseService;
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
        TableExecutionContext tec = mock(TableExecutionContext.class);
        when(nec.tableContext(null, "locations")).thenReturn(tec);
        when(tec.allCols()).thenReturn(ImmutableSet.of("name", "kind"));

        SQLParseService parseService = new SQLParseService(nec);
        return parseService.parse(sql, params);
    }


    @Test
    public void testInsertToCreateIndex() throws StandardException, IOException {

        String sql = "insert into locations values(?, ?)";
        Object[] params = new Object[]{"North West Ripple", "Galaxy"};

        ParsedStatement statement = getParsedStatement(sql, params);
        assertEquals(statement.type(), ParsedStatement.ActionType.INSERT_ACTION);

        ESRequestBuilder requestBuilder = new ESRequestBuilder(statement);
        IndexRequest indexRequest = requestBuilder.buildIndexRequest();

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
        assertEquals(statement.type(), ParsedStatement.ActionType.BULK_ACTION);

        ESRequestBuilder requestBuilder = new ESRequestBuilder(statement);
        BulkRequest bulkRequest = requestBuilder.buildBulkRequest();

        assertEquals(2, bulkRequest.requests().size());

        ActionRequest actionRequest1 = bulkRequest.requests().get(0);
        ActionRequest actionRequest2 = bulkRequest.requests().get(1);

        assertTrue(actionRequest1 instanceof IndexRequest);
        assertTrue(actionRequest2 instanceof IndexRequest);

        assertEquals("locations", ((IndexRequest)actionRequest1).index());
        assertEquals(IndexRequest.OpType.CREATE, ((IndexRequest)actionRequest1).opType());
        assertEquals("{\"name\":\"North West Ripple\",\"kind\":\"Galaxy\"}",
                new String(((IndexRequest)actionRequest1).source().toBytes()));

        assertEquals("locations", ((IndexRequest)actionRequest2).index());
        assertEquals(IndexRequest.OpType.CREATE, ((IndexRequest)actionRequest2).opType());
        assertEquals("{\"name\":\"Bartledan\",\"kind\":\"Planet\"}",
                new String(((IndexRequest)actionRequest2).source().toBytes()));

    }
}
