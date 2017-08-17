package io.crate.operation.operator;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.OperatorFormatSpec;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

public abstract class Operator<I> extends Scalar<Boolean, I> implements OperatorFormatSpec {

    public static final io.crate.types.DataType RETURN_TYPE = DataTypes.BOOLEAN;
    public static final String PREFIX = "op_";

    @Override
    public String operator(Function function) {
        // strip "op_" from function name
        return info().ident().name().substring(3).toUpperCase(Locale.ENGLISH);
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext transactionContext) {
        // all operators evaluates to NULL if one argument is NULL
        // let's handle this here to prevent unnecessary collect operations
        for (Symbol symbol : function.arguments()) {
            if (isNull(symbol)) {
                return Literal.NULL;
            }
        }
        return super.normalizeSymbol(function, transactionContext);
    }

    private static boolean isNull(Symbol symbol) {
        return symbol.symbolType().isValueSymbol() && ((Literal) symbol).value() == null;
    }

    protected static FunctionInfo generateInfo(String name, DataType type) {
        return new FunctionInfo(new FunctionIdent(name, ImmutableList.of(type, type)), RETURN_TYPE);
    }
}
