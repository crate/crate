package io.crate.analyze;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.List;


/**
 * the normalizer does symbol normalization and reference resolving if possible
 * <p/>
 * E.g.:
 * The query
 * <p/>
 * and(true, eq(column_ref, 'someliteral'))
 * <p/>
 * will be changed to
 * <p/>
 * eq(column_ref, 'someliteral')
 */
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
        // copy function to not modify where clause
        Function copy = function.clone();
        for (Symbol symbol : function.arguments()) {
            copy.setArgument(i, symbol.accept(this, context));
            i++;
        }

        return optimize(copy);
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

    public boolean evaluatesToFalse(@Nullable Symbol whereClause) {
        if (whereClause == null) {
            return false;
        }
        return (whereClause.symbolType() == SymbolType.NULL_LITERAL ||
                (whereClause.symbolType() == SymbolType.BOOLEAN_LITERAL &&
                        !((BooleanLiteral) whereClause).value()));

    }


    /**
     * return <code>true</code> if this function evaluates to <code>false</code> or <code>null</code>
     *
     * @param whereClause
     * @return false if whereClause evaluates to <code>false</code> or {@link io.crate.planner.symbol.Null}
     */
    public boolean evaluatesToFalse(@Nullable Function whereClause) {
        // no whereclause means match all
        if (whereClause == null) {
            return false;
        }
        return evaluatesToFalse(process(whereClause, null));
    }

    /**
     * normalizes the given list of symbols inplace
     */
    public void normalize(List<Symbol> symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            symbols.set(i, process(symbols.get(i), null));
        }
    }

}
