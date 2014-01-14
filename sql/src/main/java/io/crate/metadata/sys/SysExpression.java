package io.crate.metadata.sys;

import io.crate.metadata.ReferenceImplementation;
import io.crate.operator.Input;

/**
 * Base class for system expressions. Implementations of system expressions should inherit from it.
 * @param <T> The returnType of the expression
 */
public abstract class SysExpression<T> implements ReferenceImplementation, Input<T> {

    @Override
    public ReferenceImplementation getChildImplementation(String name) {
        return null;
    }

}
