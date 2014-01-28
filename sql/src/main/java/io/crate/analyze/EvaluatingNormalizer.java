package io.crate.analyze;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.operator.Input;
import io.crate.operator.operator.GtOperator;
import io.crate.operator.operator.GteOperator;
import io.crate.operator.operator.LtOperator;
import io.crate.operator.operator.LteOperator;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Arrays;
import java.util.Map;


/**
 * the normalizer is responsible for simplifying function trees
 *
 * E.g.:
 *  The query
 *
 *      and(true, eq(column_ref, 'someliteral'))
 *
 *  will be changed to
 *
 *      eq(column_ref, 'someliteral')
 *
 * in addition any yoda statements like eq(1, column_ref) will be changed to eq(column_ref, 1)
 */
public class EvaluatingNormalizer extends SymbolVisitor<Void, Symbol> {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final Functions functions;
    private final RowGranularity granularity;
    private final ReferenceResolver referenceResolver;
    private final Map<String, String> swapTable = ImmutableMap.<String, String>builder()
            .put(GtOperator.NAME, LtOperator.NAME)
            .put(GteOperator.NAME, LteOperator.NAME)
            .put(LtOperator.NAME, GtOperator.NAME)
            .put(LteOperator.NAME, GteOperator.NAME)
            .build();

    public EvaluatingNormalizer(
            Functions functions, RowGranularity granularity, ReferenceResolver referenceResolver) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public Symbol visitFunction(Function function, Void context) {
        int i = 0;
        // copy function to not modify where clause
        Function copy = function.clone();
        for (Symbol symbol : function.arguments()) {
            copy.setArgument(i, symbol.accept(this, context));
            i++;
        }

        // swap statements like  eq(2, name) to eq(name, 2)
        if (copy.arguments().size() == 2) {
            Symbol left = copy.arguments().get(0);
            Symbol right = copy.arguments().get(1);

            if (left.symbolType().isLiteral() && right.symbolType() == SymbolType.REFERENCE) {
                copy = swapFunction(copy, left, right);
            }
        }

        return optimize(copy);
    }

    private Function swapFunction(Function function, Symbol left, Symbol right) {
        String swappedName = swapTable.get(function.info().ident().name());
        FunctionInfo newInfo;

        if (swappedName == null) {
            newInfo = function.info();
        } else {
            newInfo = functions.get(
                    new FunctionIdent(swappedName, function.info().ident().argumentTypes())).info();
        }

        return new Function(newInfo, Arrays.asList(right, left));
    }

    @SuppressWarnings("unchecked")
    private Symbol optimize(Function function) {
        FunctionImplementation impl = functions.get(function.info().ident());
        if (impl != null) {
            return impl.normalizeSymbol(function);
        }
        logger.warn("No implementation found for function {}", function);
        return function;
    }

    @Override
    public Symbol visitReference(Reference symbol, Void context) {
        if (symbol.info().granularity().ordinal() > granularity.ordinal()) {
            return symbol;
        }

        Input input = (Input) referenceResolver.getImplementation(symbol.info().ident());
        if (input != null) {
            return Literal.forType(symbol.info().type(), input.value());
        }

        logger.warn("Can't resolve reference {}", symbol);
        return symbol;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        return symbol;
    }
}
