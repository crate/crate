package io.crate.operation.reference.sys;

import io.crate.operation.reference.sys.node.SysNodeExpression;

public abstract class SysNodeObjectReference extends SysObjectReference {

    protected abstract class ChildExpression<T> extends SysNodeExpression<T> {

    }
}
