package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.SubscriptExpression;
import org.cratedb.DataType;

import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public class Analysis {

    private final Routings routings;
    private Query query;

    private final ReferenceResolver referenceResolver;
    private final Functions functions;
    private TableIdent table;

    private Map<Function, Function> functionSymbols = new HashMap<>();

    //private Map<Aggregation, Aggregation> aggregationSymbols = new HashMap<>();
    private Map<ReferenceIdent, Reference> referenceSymbols = new IdentityHashMap<>();

    private Map<Expression, DataType> types = new IdentityHashMap<>();
    private Routing routing;
    private List<String> outputNames;
    private List<Symbol> outputSymbols;
    private Integer limit;
    private List<Symbol> groupBy;
    private boolean[] reverseFlags;
    private List<Symbol> sortSymbols;
    private RowGranularity rowGranularity;
    private boolean hasAggregates = false;

    public Analysis(ReferenceResolver referenceResolver, Functions functions, Routings routings) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
        this.routings = routings;
    }

    public void table(TableIdent tableIdent) {
        this.routing = routings.getRouting(tableIdent);
        this.table = tableIdent;
    }

    public Routing routing() {
        return routing;
    }

    public TableIdent table() {
        return this.table;
    }

    public Query query() {
        return query;
    }

    public void query(Query query) {
        this.query = query;
    }

    public Reference allocateReference(ReferenceIdent ident){
        Reference reference = referenceSymbols.get(ident);
        if (reference==null){
            ReferenceInfo info = getReferenceInfo(ident);
            reference = new Reference(info);
            referenceSymbols.put(info.ident(), reference);
        }
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceResolver.getInfo(ident);
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


    public void putType(Expression expression, DataType type) {
        types.put(expression, type);
    }


    public DataType getType(Expression expression) {
        Preconditions.checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        return types.get(expression);
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
     * @param granularity the row granularity as seen by a reference
     * @return
     */
    private RowGranularity updateRowGranularity(RowGranularity granularity){
        if (rowGranularity==null || rowGranularity.ordinal()<granularity.ordinal()){
            rowGranularity=granularity;
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
        if (info.isAggregate()){
            hasAggregates = true;
        }
        Function function = new Function(info, arguments);
        Function existing = functionSymbols.get(function);
        if (existing != null){
            return existing;
        } else {
            functionSymbols.put(function, function);
        }
        return function;
    }
}
