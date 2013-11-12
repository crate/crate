package org.cratedb.action.parser.visitors;

import org.cratedb.action.parser.QueryPlanner;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.sql.parser.parser.*;

import java.util.Map;

public class BaseVisitor extends DispatchingVisitor {

    protected final Object[] args;
    protected final NodeExecutionContext context;
    protected QueryPlanner queryPlanner;
    protected ITableExecutionContext tableContext;

    public BaseVisitor(NodeExecutionContext context, ParsedStatement parsedStatement, Object[] args) {
        super(parsedStatement);
        this.context = context;
        this.args = args;
        if (context != null) {
            this.queryPlanner = context.queryPlanner();
        }
    }

    /**
     * set the table and schemaName from the {@link TableName}
     * This will also load the tableContext for the table.
     * @param tableName
     */
    protected void tableName(TableName tableName) {
        stmt.schemaName(tableName.getSchemaName());
        stmt.tableName(tableName.getTableName());
        tableContext = context.tableContext(tableName.getSchemaName(), tableName.getTableName());
        if (tableContext == null) {
            throw new TableUnknownException(tableName.getTableName());
        }
    }

    protected void visit(FromList fromList) throws Exception {
        if (fromList.size() != 1) {
            throw new SQLParseException(
                "Only exactly one table is allowed in the from clause, got: " + fromList.size()
            );
        }

        FromTable table = fromList.get(0);
        if (!(table instanceof FromBaseTable)) {
            throw new SQLParseException(
                "From type " + table.getClass().getName() + " not supported");
        }

        tableName(table.getTableName());
    }

    @Deprecated
    protected Object mappedValueFromNode(String name, ValueNode node) {
        Object unmappedValue = valueFromNode(node);
        Object value = mapRecursive(name, unmappedValue);
        if (value != null) {
            return value;
        }
        return unmappedValue;
    }

    @SuppressWarnings("unchecked")
    private Object mapRecursive(String name, Object unmappedValue) {
        if (unmappedValue == null) {
            return unmappedValue;
        } else if (unmappedValue.getClass().isArray()) {
            Object[] unmappedValues = (Object[])unmappedValue;
            Object[] value = new Object[unmappedValues.length];
            for (int i = 0; i < value.length; i++) {
                value[i] = mapRecursive(name, unmappedValues[i]);
            }
            return value;
        } else if (unmappedValue instanceof Map) {
            Map<Object, Object> valueMap = (Map<Object, Object>)unmappedValue;
            for (Map.Entry<Object, Object> entry : valueMap.entrySet()) {
                entry.setValue(mapRecursive(name + "." + entry.getKey().toString(), entry.getValue()));
            }
            return valueMap;
        } else {
            Object mappedValue = tableContext.mappedValue(name, unmappedValue);
            if (mappedValue != null) {
                return mappedValue;
            } else {
                return unmappedValue;
            }
        }
    }

    /**
     * extract the value from the Node.
     * This works for ConstantNode and ParameterNodes
     *
     * Note that the returned value is unmapped. Use {@link #mappedValueFromNode(String, org.cratedb.sql.parser.parser.ValueNode)}
     * to get the mapped value.
     * @param node
     * @return
     */
    protected Object valueFromNode(ValueNode node) {
        if (node == null) {
            return null;
        }
        if (node.getNodeType() == NodeTypes.PARAMETER_NODE) {
            if (args.length == 0) {
                throw new SQLParseException("Missing statement parameters");
            }

            int parameterNumber = ((ParameterNode)node).getParameterNumber();
            try {
                return args[parameterNumber];
            } catch (IndexOutOfBoundsException e) {
                throw new SQLParseException("Statement parameter value not found");
            }
        } else if (node instanceof ConstantNode) {
            return ((ConstantNode)node).getValue();
        } else {
            throw new SQLParseException("ValueNode type not supported " + node.getClass().getName());
        }
    }
}
