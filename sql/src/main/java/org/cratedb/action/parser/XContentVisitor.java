package org.cratedb.action.parser;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.ConstantNode;
import org.cratedb.sql.parser.parser.ParameterNode;
import org.cratedb.sql.parser.parser.ValueNode;
import org.cratedb.sql.parser.parser.Visitor;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.List;

/**
 * The XContentVisitor is an extended Visitor interface provided by the akiban SQL-Parser
 * See https://github.com/akiban/sql-parser for more information.
 *
 * Implementations purpose is generating XContent from SQL statements(Akiban nodes).
 *
 */
public abstract class XContentVisitor implements Visitor {

    public abstract XContentBuilder getXContentBuilder() throws StandardException;

    protected final ParsedStatement stmt;

    public XContentVisitor(ParsedStatement stmt) {
        this.stmt = stmt;
    }


    protected Object evaluateValueNode(
        NodeExecutionContext.TableExecutionContext tableContext,
        String name,
        ValueNode node
    ) throws StandardException
    {
        Object value;
        if (node instanceof ConstantNode) {
            value = tableContext.mappedValue(name, ((ConstantNode) node).getValue());
        } else if (node instanceof ParameterNode) {
            Object[] args = stmt.args();
            if (args.length == 0) {
                throw new StandardException("Missing statement parameters");
            }
            int parameterNumber = ((ParameterNode)node).getParameterNumber();
            try {
                value = args[parameterNumber];
            } catch (IndexOutOfBoundsException e) {
                throw new StandardException("Statement parameter value not found");
            }
        } else {
            throw new SQLParseException(
                "ValueNode type not supported " + node.getClass().getName());
        }
        return value;
    }

}