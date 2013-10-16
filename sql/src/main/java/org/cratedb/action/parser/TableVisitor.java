package org.cratedb.action.parser;

import com.google.common.collect.ImmutableMap;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.NodeType;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.cratedb.sql.parser.types.TypeId;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.collect.Maps.immutableEnumMap;
import static com.google.common.collect.Maps.newHashMap;

public class TableVisitor extends XContentVisitor {

    private boolean stopTraversal;
    private XContentBuilder mappingBuilder;
    private Map<String, Object> indexSettings = newHashMap();
    private Map<String, Object> mappingProperties = newHashMap();
    private Map<String, Object> mappingMeta = newHashMap();

    @Override
    public XContentBuilder getXContentBuilder() throws StandardException {
        return mappingBuilder;
    }

    public TableVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        try {
            mappingBuilder = XContentFactory.jsonBuilder().startObject();
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
        if (node.getQueryExpression() != null) {
            throw new SQLParseException("Create Table from Query is not Supported.");
        }
        // TODO handle existence checks (IF NOT EXISTS, IF EXISTS ...)
        String tableName = node.getObjectName().getTableName();
        stmt.addIndex(tableName);

        // TODO: get from CreateTableNode
        indexSettings.put("number_of_replicas", 0);
        indexSettings.put("number_of_shards", 5);

        try {
            // build mapping

            for (TableElementNode tableElement : node.getTableElementList()) {
                switch(tableElement.getNodeType()) {
                    case NodeTypes.COLUMN_DEFINITION_NODE:
                        visit((ColumnDefinitionNode) tableElement);
                        break;
                    case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                        visit((ConstraintDefinitionNode)tableElement);
                }
            }
            mappingBuilder.startObject("mappings").startObject("default");
            mappingBuilder.field("_meta", mappingMeta);
            mappingBuilder.field("properties", mappingProperties);
            mappingBuilder.endObject().endObject();
        } catch (IOException e) {
            throw new StandardException(e);
        }
        return node;
    }

    public Visitable visit(ColumnDefinitionNode node) {

        String columnName = node.getColumnName();
        mappingProperties.put(columnName, newHashMap());
        return node;
    }


    public Visitable visit(ConstraintDefinitionNode node) {
        switch(node.getConstraintType()) {
            case PRIMARY_KEY:
                String[] pkColumnNames = node.getColumnList().getColumnNames();
                if (pkColumnNames.length > 1) {
                    throw new SQLParseException("Multiple Primary Key Columns not Supported.");
                }
                mappingMeta.put("primary_keys", pkColumnNames[0]);
                break;
            // TODO: handle INDEX-DEFINITION
            default:
                throw new SQLParseException("Unsupported Constraint");
        }
        return node;
    }

    String getTypeName(ColumnDefinitionNode node) {
        TypeId typeId = node.getType().getTypeId();
        if (typeId.isBooleanTypeId()) {
            return "boolean";
        } else if(typeId.isIntegerTypeId()) {
            // TODO: LONG, SHORT, BYTE ...
            return "integer";
        }

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
