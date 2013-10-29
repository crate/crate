package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

public class CreateAnalyzerNode extends DDLStatementNode {

    private AnalyzerElements analyzerElements = null;

    private TableName extendsName = null;

    @Override
    public void init(
            Object name,
            Object extendsName,
            Object analyzerElements) throws StandardException {
        super.init(name);
        this.extendsName = (TableName) extendsName;
        this.analyzerElements = (AnalyzerElements) analyzerElements;
    }

    @Override
    public String statementToString() {
        return "CREATE ANALYZER";
    }

    public TableName getExtendsName() {
        return extendsName;
    }

    public AnalyzerElements getElements() {
        return analyzerElements;
    }

    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        CreateAnalyzerNode createAnalyzerNode = (CreateAnalyzerNode) node;
        this.extendsName = (TableName)getNodeFactory().copyNode(createAnalyzerNode.getExtendsName(), getParserContext());
        this.analyzerElements = (AnalyzerElements)getNodeFactory().copyNode(createAnalyzerNode.getElements(), getParserContext());
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (extendsName != null) {
            printLabel(depth, "extends: ");
            extendsName.treePrint(depth+1);
        }
        if (analyzerElements != null) {
            analyzerElements.treePrint(depth+1);
        }
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (extendsName != null) {
            extendsName.accept(v);
        }
        analyzerElements.accept(v);
    }
}
