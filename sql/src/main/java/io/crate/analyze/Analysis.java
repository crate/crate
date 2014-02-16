package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.Preconditions;

import javax.annotation.Nullable;
import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public abstract class Analysis {

    private final EvaluatingNormalizer normalizer;

    public static enum Type {
        SELECT,
        INSERT
    }

    public abstract Type type();

    protected final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final Object[] parameters;
    protected TableInfo table;

    private Map<Function, Function> functionSymbols = new HashMap<>();

    protected Map<ReferenceIdent, Reference> referenceSymbols = new IdentityHashMap<>();

    private List<String> outputNames = ImmutableList.of();
    private List<Symbol> outputSymbols = ImmutableList.of();

    protected boolean noMatch = false;
    protected List<Literal> primaryKeyLiterals;

    private boolean isDelete = false;
    protected Function whereClause;
    protected RowGranularity rowGranularity;
    protected boolean hasAggregates = false;

    public List<Literal> primaryKeyLiterals() {
        return primaryKeyLiterals;
    }

    public void primaryKeyLiterals(List<Literal> primaryKeyLiterals) {
        this.primaryKeyLiterals = primaryKeyLiterals;
    }

    public Analysis(ReferenceInfos referenceInfos, Functions functions, Object[] parameters,
                    ReferenceResolver referenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.parameters = parameters;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

    public void table(TableIdent tableIdent) {
        table = referenceInfos.getTableInfo(tableIdent);
        Preconditions.checkNotNull(table, "Table not found", tableIdent);
        updateRowGranularity(table.rowGranularity());
    }

    public TableInfo table() {
        return this.table;
    }

    public Reference allocateReference(ReferenceIdent ident) {
        Reference reference = referenceSymbols.get(ident);
        if (reference == null) {
            ReferenceInfo info = getReferenceInfo(ident);
            reference = new Reference(info);
            referenceSymbols.put(info.ident(), reference);
        }
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    /**
     * add a new reference for the given ident
     * and throw an error if this ident has already been added
     */
    public Reference allocateUniqueReference(ReferenceIdent ident) {
        if (referenceSymbols.get(ident) != null) {
            throw new IllegalArgumentException(String.format("reference '%s' repeated", ident));
        }
        ReferenceInfo info = getReferenceInfo(ident);
        Reference reference = new Reference(info);
        referenceSymbols.put(info.ident(), reference);
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceInfos.getReferenceInfo(ident);
        if (info == null) {
            throw new UnsupportedOperationException("TODO: unknown column reference: " + ident);
        }
        return info;
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException("TODO: unknown function? " + ident.toString());
        }
        return implementation.info();
    }

    public Collection<Reference> references() {
        return referenceSymbols.values();
    }

    public Collection<Function> functions() {
        return functionSymbols.values();
    }

    public Function allocateFunction(FunctionInfo info, List<Symbol> arguments) {
        if (info.isAggregate()) {
            hasAggregates = true;
        }
        Function function = new Function(info, arguments);
        Function existing = functionSymbols.get(function);
        if (existing != null) {
            return existing;
        } else {
            functionSymbols.put(function, function);
        }
        return function;
    }

    /**
     * Indicates that the statement will not match, so that there is no need to execute it
     */
    public boolean noMatch() {
        return noMatch;
    }

    public void noMatch(boolean noMatch) {
        this.noMatch = noMatch;
    }

    @Nullable
    public Function whereClause(@Nullable Symbol whereClause) {
        if (whereClause != null) {
            Symbol normalizedWhereClause = normalizer.process(whereClause, null);
            switch (normalizedWhereClause.symbolType()) {
                case FUNCTION:
                    this.whereClause = (Function) normalizedWhereClause;
                    break;
                case BOOLEAN_LITERAL:
                    noMatch = !((BooleanLiteral) normalizedWhereClause).value();
                    break;
                case NULL_LITERAL:
                    noMatch = true;
                default:
                    throw new UnsupportedOperationException("unsupported whereClause symbol: " + normalizedWhereClause);
            }
        }
        return this.whereClause;
    }

    public Function whereClause() {
        return whereClause;
    }

    /**
     * Updates the row granularity of this query if it is higher than the current row granularity.
     *
     * @param granularity the row granularity as seen by a reference
     * @return
     */
    protected RowGranularity updateRowGranularity(RowGranularity granularity) {
        if (rowGranularity == null || rowGranularity.ordinal() < granularity.ordinal()) {
            rowGranularity = granularity;
        }
        return rowGranularity;
    }

    public RowGranularity rowGranularity() {
        return rowGranularity;
    }

    public Object parameterAt(int idx) {
        Preconditions.checkElementIndex(idx, parameters.length);
        return parameters[idx];
    }

    public void addOutputName(String s) {
        this.outputNames.add(s);
    }

    public void outputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    public List<String> outputNames() {
        return outputNames;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public void outputSymbols(List<Symbol> symbols) {
        this.outputSymbols = symbols;
    }

    public boolean isDelete() {
        return isDelete;
    }

    public void isDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }
}
