package org.cratedb.module.sql.test;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static junit.framework.Assert.assertEquals;

public class ExpressionTest {

    protected ParsedStatement stmt;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    protected ParsedStatement execute(String stmt) throws Exception {
        return execute(stmt, new Object[0]);
    }

    protected ParsedStatement execute(String stmt, Object[] args) throws Exception {
        NodeExecutionContext nec = HitchhikerMocks.nodeExecutionContext();
        SQLParseService service = new SQLParseService(nec);
        this.stmt = service.parse(stmt, args);
        return this.stmt;
    }

    @Test
    public void testSelectExpressionFromAnotherTable() throws Exception {
        execute("select \"_crate.cluster.name\" from characters");
        assertEquals("characters", stmt.tableName());
        assertEquals("_crate.cluster.name", stmt.outputFields().get(0).v1());
    }


}
