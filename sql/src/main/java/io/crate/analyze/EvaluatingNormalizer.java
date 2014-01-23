package io.crate.analyze;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;


public class EvaluatingNormalizer extends SymbolVisitor<Void, Symbol> {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final Functions functions;
    private final RowGranularity granularity;
    private final ReferenceResolver referenceResolver;

    public EvaluatingNormalizer(
            Functions functions, RowGranularity granularity, ReferenceResolver referenceResolver) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public Symbol visitFunction(Function function, Void context) {
        int i = 0;
        for (Symbol symbol : function.arguments()) {
            function.setArgument(i, symbol.accept(this, context));
            i++;
        }

        return optimize(function);
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
