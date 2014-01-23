package io.crate.operator.operator;

import io.crate.metadata.FunctionImplementation;
import io.crate.operator.Input;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;

public interface Operator extends FunctionImplementation<Function> {
}
