package io.crate.analyze;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Query;
import org.cratedb.sql.AmbiguousAliasException;
import org.elasticsearch.common.Preconditions;

import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public class Analysis {

    private Query query;

    private final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final Object[] parameters;
    private TableInfo table;

    private Map<Function, Function> functionSymbols = new HashMap<>();

    private Map<ReferenceIdent, Reference> referenceSymbols = new IdentityHashMap<>();

    private Multimap<String, Symbol> aliasMap = ArrayListMultimap.create();
    private List<String> outputNames;
    private List<Symbol> outputSymbols;
    private Integer limit;
    private int offset = 0;
    private List<Symbol> groupBy;
    private boolean[] reverseFlags;
    private List<Symbol> sortSymbols;
    private RowGranularity rowGranularity;
    private boolean hasAggregates = false;
    private Function whereClause;

    public Analysis(ReferenceInfos referenceInfos, Functions functions, Object[] parameters) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.parameters = parameters;
    }

    public void table(TableIdent tableIdent) {
        table = referenceInfos.getTableInfo(tableIdent);
        Preconditions.checkNotNull(table, "Table not found", tableIdent);
    }

    public TableInfo table() {
        return this.table;
    }

    public Query query() {
        return query;
    }

    public void query(Query query) {
        this.query = query;
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

    public void limit(Integer limit) {
        this.limit = limit;
    }

    public Integer limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public void groupBy(List<Symbol> groupBy) {
        this.groupBy = groupBy;
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    public boolean hasGroupBy() {
        return groupBy != null && groupBy.size() > 0;
    }

    public void reverseFlags(boolean[] reverseFlags) {
        this.reverseFlags = reverseFlags;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public void sortSymbols(List<Symbol> sortSymbols) {
        this.sortSymbols = sortSymbols;
    }

    public List<Symbol> sortSymbols() {
        return sortSymbols;
    }

    public boolean isSorted() {
        return sortSymbols != null && sortSymbols.size() > 0;
    }

    /**
     * Updates the row granularity of this query if it is higher than the current row granularity.
     *
     * @param granularity the row granularity as seen by a reference
     * @return
     */
    private RowGranularity updateRowGranularity(RowGranularity granularity) {
        if (rowGranularity == null || rowGranularity.ordinal() < granularity.ordinal()) {
            rowGranularity = granularity;
        }
        return rowGranularity;
    }

    public RowGranularity rowGranularity() {
        return rowGranularity;
    }

    public Collection<Reference> references() {
        return referenceSymbols.values();
    }

    public Collection<Function> functions() {
        return functionSymbols.values();
    }


    public boolean hasAggregates() {
        return hasAggregates;
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

    public void whereClause(Function whereClause) {
        this.whereClause = whereClause;
    }

    public Function whereClause() {
        return whereClause;
    }

    public void addAlias(String alias, Symbol symbol) {
        outputNames().add(alias);
        aliasMap.put(alias, symbol);
    }

    public Symbol symbolFromAlias(String alias) {
        Collection<Symbol> symbols = aliasMap.get(alias);
        if (symbols.size() > 1) {
            throw new AmbiguousAliasException(alias);
        }
        if (symbols.isEmpty()) {
            return null;
        }

        return symbols.iterator().next();
    }

    public void offset(int offset) {
        this.offset = offset;
    }

    public Object parameterAt(int idx) {
        Preconditions.checkElementIndex(idx, parameters.length);
        return parameters[idx];
    }

}
