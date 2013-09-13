package org.cratedb.action.parser;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.FieldMappers;

import java.util.Stack;

public class WhereClauseVisitor extends BaseVisitor {

    private Query rootQuery;
    private Stack<BooleanQuery> queryStack = new Stack<BooleanQuery>();
    private DocumentFieldMappers documentFieldMappers;

    public Query query() {
        return rootQuery;
    }


    public WhereClauseVisitor(DocumentFieldMappers documentFieldMappers) {
        this.documentFieldMappers = documentFieldMappers;
    }

    @Override
    public void visit(SelectNode node) {
        if (node.getWhereClause() == null) {
            rootQuery = new MatchAllDocsQuery();
        } else {
            visit(null, node.getWhereClause());
        }
    }

    @Override
    public void visit(ValueNode parentNode, BinaryRelationalOperatorNode node) {
        if (parentNode == null) {
            rootQuery = queryFromNode(node);
            return;
        }

        BooleanQuery parentQuery = queryStack.peek();
        parentQuery.add(
            queryFromNode(node),
            parentNode.getNodeType() == NodeTypes.AND_NODE ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD
        );
    }

    @Override
    public void visit(ValueNode parentNode, AndNode node) {
        BooleanQuery query = newBoolNode(parentNode);
        queryStack.add(query);
        visit(node, node.getLeftOperand());
        visit(node, node.getRightOperand());
        queryStack.pop();
    }

    @Override
    public void visit(ValueNode parentNode, OrNode node) {
        BooleanQuery query = newBoolNode(parentNode);
        queryStack.add(query);
        visit(node, node.getLeftOperand());
        visit(node, node.getRightOperand());
        queryStack.pop();
    }

    @Override
    public void visit(ValueNode parentNode, IsNullNode node) {
    }

    private BooleanQuery newBoolNode(ValueNode parentNode) {
        BooleanQuery query = new BooleanQuery();
        query.setMinimumNumberShouldMatch(1);

        if (rootQuery == null) {
            rootQuery = query;
        } else {
            BooleanQuery parentQuery = queryStack.peek();
            parentQuery.add(query,
                parentNode.getNodeType() == NodeTypes.AND_NODE ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD
            );
        }

        return query;
    }

    private Query queryFromNode(BinaryRelationalOperatorNode node) {
        int operator = node.getOperatorType();
        String columnName;
        Object value;

        if (node.getLeftOperand() instanceof ColumnReference) {
            columnName = node.getLeftOperand().getColumnName();
            value = ((ConstantNode)node.getRightOperand()).getValue();
        } else {
            operator = swapOperator(operator, node);
            columnName = node.getRightOperand().getColumnName();
            value = ((ConstantNode)node.getLeftOperand()).getValue();
        }

        Object from = null;
        Object to = null;
        boolean includeLower = false;
        boolean includeUpper = false;
        if (operator == node.EQUALS_RELOP) {
            return new TermQuery(new Term(columnName, BytesRefs.toBytesRef(value)));
        } else if (operator == node.LESS_THAN_RELOP) {
            to = value;
            includeUpper = false;
        } else if (operator == node.LESS_EQUALS_RELOP) {
            to = value;
            includeUpper = true;
        } else if (operator == node.GREATER_THAN_RELOP) {
            from = value;
            includeLower = false;
        } else if (operator == node.GREATER_EQUALS_RELOP) {
            from = value;
            includeLower = true;
        }

        FieldMappers fieldMappers = documentFieldMappers.smartName(columnName);
        if (fieldMappers != null) {
            return fieldMappers.mapper().rangeQuery(
                from, to, includeLower, includeUpper, null
            );
        }

        return new TermRangeQuery(
            columnName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to),
            includeLower, includeUpper
        );
    }

    private int swapOperator(int operator, BinaryRelationalOperatorNode dummyNode) {
        if (operator == dummyNode.LESS_THAN_RELOP)
            return dummyNode.GREATER_THAN_RELOP;
        if (operator == dummyNode.LESS_EQUALS_RELOP)
            return dummyNode.GREATER_EQUALS_RELOP;
        if (operator == dummyNode.GREATER_THAN_RELOP)
            return dummyNode.LESS_THAN_RELOP;
        if (operator == dummyNode.GREATER_EQUALS_RELOP)
            return dummyNode.LESS_EQUALS_RELOP;

        return operator;
    }
}
