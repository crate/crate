package io.crate.analyze;

import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.TableElement;

public class CreateTableStatementAnalyzer extends AstVisitor {

    @Override
    protected Object visitNode(Node node, Object context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Object visitCreateTable(CreateTable node, Object context) {
        node.clusteredBy();
        node.name();
        node.replicas();
        for (TableElement tableElement : node.tableElements()) {

        }

        return null;
    }
}
