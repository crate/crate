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
import java.util.Arrays;
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
        List<Symbol> newArgs = normalize(function.arguments());
        if (newArgs != function.arguments()) {
            function = new Function(function.info(), newArgs);
        }
        return normalizeFunctionSymbol(function);
    }

    @SuppressWarnings("unchecked")
    private Symbol normalizeFunctionSymbol(Function function) {
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
        // no whereClause means match all
        if (whereClause == null) {
            return false;
        }
        return evaluatesToFalse(process(whereClause, null));
    }

    /**
     * Normalizes all symbols of a List. Does not return a new list if no changes occur.
     *
     * @param symbols the list to be normalized
     * @return a list with normalized symbols
     */
    public List<Symbol> normalize(List<Symbol> symbols) {
        if (symbols.size() > 0) {
            boolean changed = false;
            Symbol[] newArgs = new Symbol[symbols.size()];
            int i = 0;
            for (Symbol symbol : symbols) {
                Symbol newArg = normalize(symbol);
                changed = changed || newArg != symbol;
                newArgs[i++] = newArg;
            }
            if (changed) {
                return Arrays.asList(newArgs);
            }
        }
        return symbols;
    }

    /**
     * Normalizes all symbols of a List in place
     *
     * @param symbols the list to be normalized
     */
    public void normalizeInplace(@Nullable List<Symbol> symbols) {
        if (symbols != null){
            for (int i = 0; i < symbols.size(); i++) {
                symbols.set(i, normalize(symbols.get(i)));
            }
        }
    }

    public Symbol normalize(@Nullable Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return process(symbol, null);
    }
}
