package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.planner.symbol.Symbol;
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

    private Map<FunctionCall, FunctionInfo> functionInfos = new IdentityHashMap<>();
    private Map<Expression, ReferenceInfo> referenceInfos = new IdentityHashMap<>();
    private List<ReferenceInfo> toCollect = new ArrayList<ReferenceInfo>();
    private Map<Expression, DataType> types = new IdentityHashMap<>();
    private Routing routing;
    private List<String> outputNames;
    private List<Symbol> outputSymbols;
    private Integer limit;
    private List<Symbol> groupBy;
    private List<Boolean> reverseFlags;
    private List<Symbol> sortSymbols;

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

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceResolver.getInfo(ident);
        if (info == null) {
            throw new UnsupportedOperationException("TODO: unknown column reference: " + ident);
        }
        return info;
    }

    public Collection<ReferenceInfo> referenceInfos() {
        return referenceInfos.values();
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException("TODO: unknown function? " + ident.toString());
        }
        return implementation.info();
    }

    public void putFunctionInfo(FunctionCall node, FunctionInfo functionInfo) {
        functionInfos.put(node, functionInfo);
        types.put(node, functionInfo.returnType());
    }

    public void putReferenceInfo(Expression node, ReferenceInfo info) {
        referenceInfos.put(node, info);
        types.put(node, info.type());
        if (!toCollect.contains(info)) {
            toCollect.add(info);
        }
    }

    public ReferenceInfo getReferenceInfo(SubscriptExpression node) {
        return referenceInfos.get(node);
    }

    public FunctionInfo getFunctionInfo(FunctionCall node) {
        return functionInfos.get(node);
    }

    public void putType(Expression expression, DataType type) {
        types.put(expression, type);
    }


    public DataType getType(Expression expression) {
        Preconditions.checkArgument(types.containsKey(expression), "Expression not analyzed: %s", expression);
        return types.get(expression);
    }


    public List<ReferenceInfo> toCollect() {
        return toCollect;
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

    public void reverseFlags(List<Boolean> reverseFlags) {
        this.reverseFlags = reverseFlags;
    }

    public List<Boolean> reverseFlags() {
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
}
