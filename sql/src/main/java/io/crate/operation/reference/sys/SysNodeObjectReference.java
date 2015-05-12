package io.crate.operation.reference.sys;

import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.node.SysNodeExpression;

public abstract class SysNodeObjectReference extends NestedObjectExpression {

    protected abstract class ChildExpression<T> extends SysNodeExpression<T> {

    }
}
