package org.cratedb.service;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.TableUnknownException;
import org.elasticsearch.cluster.AbstractZenNodesTests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.internal.InternalNode;
import org.junit.Test;

public class SQLParseServiceTest extends AbstractZenNodesTests {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private InternalNode startNode() {
        return (InternalNode) startNode("node1", ImmutableSettings.EMPTY);
    }

    @Test(expected = TableUnknownException.class)
    public void testParse() throws Exception {
        InternalNode node = startNode();
        SQLParseService parseService = node.injector().getInstance(SQLParseService.class);
        ParsedStatement stmt = parseService.parse("select mycol from mytable where mycol = 1");

        // this test just ensures that the ParseService calls a Visitor and begins to parse the statement
        // the actual result isn't really enforced. Feel free to change the expected TableUnknownException
        // later on if something else makes more sense.
    }
}
