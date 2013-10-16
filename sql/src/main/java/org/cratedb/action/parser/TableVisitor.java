package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class TableVisitor extends XContentVisitor {

    private boolean stopTraversal;

    private XContentBuilder builder;

    @Override
    public XContentBuilder getXContentBuilder() throws StandardException {
        return builder;
    }

    public TableVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        try {
            builder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException ex) {
            throw new StandardException(ex);
        }
        stopTraversal = false;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        DDLStatementNode ddlNode = (DDLStatementNode) node;
        switch(ddlNode.getNodeType()) {
            case NodeTypes.CREATE_TABLE_NODE:
                stopTraversal = true;
                return visit((CreateTableNode)node);
            default:
                throw new SQLParseException("Unsupported DDL Statement");
        }
    }

    public Visitable visit(CreateTableNode node) throws StandardException {
        // validation
        if (node.isWithData()) {
            throw new SQLParseException("Create Table With Data is not Supported.");
        }
        // TODO handle existence checks (IF NOT EXISTS, IF EXISTS ...)
        String tableName = node.getObjectName().getTableName();
        try {
            // build
            builder.startObject();
            for (TableElementNode tableElement : node.getTableElementList()) {
                visit(tableElement);
            }
            builder.endObject();
            // TODO: set to CreateIndexRequest
            builder.flush();
        } catch (IOException e) {
            throw new StandardException(e);
        }
        return node;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraversal;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
