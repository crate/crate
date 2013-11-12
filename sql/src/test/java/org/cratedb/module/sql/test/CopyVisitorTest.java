package org.cratedb.module.sql.test;

import org.cratedb.action.import_.ImportRequest;
import org.cratedb.action.parser.ESRequestBuilder;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.TableExecutionContext;
import org.cratedb.core.Constants;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.sql.parser.StandardException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CopyVisitorTest {

    private ParsedStatement stmt;
    private ESRequestBuilder requestBuilder;

    private String filePath = CopyVisitorTest.class.getResource(
            "/essetup/data/copy/test_copy_from.json").getPath();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ParsedStatement execStatement(String stmt) throws StandardException {
        return execStatement(stmt, new Object[]{});
    }

    private ParsedStatement execStatement(String sql, Object[] args) throws StandardException {
        NodeExecutionContext nec = mock(NodeExecutionContext.class);
        TableExecutionContext tec = mock(TableExecutionContext.class);
        when(nec.tableContext(null, "quotes")).thenReturn(tec);

        SQLParseService parseService = new SQLParseService(nec);
        stmt = parseService.parse(sql, args);
        requestBuilder = new ESRequestBuilder(stmt);
        return stmt;
    }

    @Test
    public void testCopyFromFile() throws Exception {
        execStatement("copy quotes from '" + filePath + "'");

        assertEquals(ParsedStatement.ActionType.COPY_IMPORT_ACTION, stmt.type());
        assertEquals(filePath, stmt.importPath);

        ImportRequest importRequest = requestBuilder.buildImportRequest();
        assertEquals("quotes", importRequest.index());
        assertEquals(Constants.DEFAULT_MAPPING_TYPE, importRequest.type());
    }

    @Test
    public void testCopyFromFileParameter() throws Exception {
        execStatement("copy quotes from ?", new Object[]{filePath});

        assertEquals(ParsedStatement.ActionType.COPY_IMPORT_ACTION, stmt.type());
        assertEquals(filePath, stmt.importPath);

        ImportRequest importRequest = requestBuilder.buildImportRequest();
        assertEquals("quotes", importRequest.index());
        assertEquals(Constants.DEFAULT_MAPPING_TYPE, importRequest.type());
    }

    @Test
    public void testCopyFromFileUnknownTable() throws Exception {
        expectedException.expect(TableUnknownException.class);
        expectedException.expectMessage("Unknown table");
        execStatement("copy invalid_table from ?", new Object[]{filePath});
    }


}
