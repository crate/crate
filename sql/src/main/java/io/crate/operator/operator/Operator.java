package io.crate.operator.operator;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import org.cratedb.DataType;

public abstract class Operator<I> implements FunctionImplementation<Function>, Scalar<Boolean, I> {

    protected static FunctionInfo generateInfo(String name, DataType type) {
        return new FunctionInfo(new FunctionIdent(name, ImmutableList.of(type, type)), DataType.BOOLEAN);
    }

    protected boolean containsNull(Symbol left, Symbol right) {
        return left.symbolType() == SymbolType.NULL_LITERAL || right.symbolType() == SymbolType.NULL_LITERAL;
    }

}
