package io.crate.operation.reference.sys;

import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.SysNodeExpression;

public abstract class SysNodeObjectReference extends NestedObjectExpression {

    public static final SysNodeExpression UNKNOWN_VALUE_EXPRESSION = new SysNodeExpression() {
        @Override
        public Short value() {
            return -1;
        }
    };

    protected abstract class ChildExpression<T> extends SysNodeExpression<T> {

    }
}
