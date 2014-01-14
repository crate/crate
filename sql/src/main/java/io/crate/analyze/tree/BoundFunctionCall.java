package io.crate.analyze.tree;

import io.crate.metadata.FunctionInfo;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Expression;

import java.util.List;

public class BoundFunctionCall extends Expression {

    private final List<Expression> argumentExpressions;
    private final FunctionInfo info;

    public BoundFunctionCall(FunctionInfo info, List<Expression> argumentExpressions) {
        this.argumentExpressions = argumentExpressions;
        this.info = info;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }
}
