package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.sql.tree.*;
import org.cratedb.DataType;

import java.util.IdentityHashMap;
import java.util.Map;


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
    private Map<Expression, DataType> types = new IdentityHashMap<>();
    private Routing routing;

    public Analysis(ReferenceResolver referenceResolver, Functions functions, Routings routings) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
        this.routings = routings;
    }

    public void table(TableIdent tableIdent) {
        this.routing = routings.getRouting(tableIdent);
        this.table = tableIdent;
    }

    public Routing routing(){
        return routing;
    }

    public TableIdent table() {
        return this.table;
    }

    public Query query(){
        return query;
    }

    public void query(Query query){
        this.query = query;
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

    public void putFunctionInfo(FunctionCall node, FunctionInfo functionInfo) {
        functionInfos.put(node, functionInfo);
        types.put(node, functionInfo.returnType());
    }

    public void putReferenceInfo(Expression node, ReferenceInfo info) {
        referenceInfos.put(node, info);
        types.put(node, info.type());
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


}
