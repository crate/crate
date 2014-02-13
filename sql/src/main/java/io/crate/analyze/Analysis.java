package io.crate.analyze;

import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Preconditions;

import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public abstract class Analysis {

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

    private List<String> outputNames;
    private List<Symbol> outputSymbols;

    private boolean isDelete = false;
    protected Function whereClause;
    protected RowGranularity rowGranularity;
    protected boolean hasAggregates = false;

    public Analysis(ReferenceInfos referenceInfos, Functions functions, Object[] parameters) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.parameters = parameters;
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

    public void whereClause(Function whereClause) {
        this.whereClause = whereClause;
    }

    public Function whereClause() {
        return whereClause;
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
