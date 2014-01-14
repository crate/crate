package io.crate.analyze;

import io.crate.metadata.*;
import io.crate.metadata.sys.SystemReferences;
import io.crate.sql.tree.*;
import org.cratedb.DataType;

import java.util.IdentityHashMap;
import java.util.Map;

public class BindingContext {

    private final ReferenceResolver referenceResolver;
    private final Functions functions;
    private TableIdent table;
    private Map<FunctionCall, FunctionInfo> functionInfos = new IdentityHashMap<>();
    private Map<Expression, ReferenceInfo> referenceInfos = new IdentityHashMap<>();

    public BindingContext(ReferenceResolver referenceResolver, Functions functions) {
        this.referenceResolver = referenceResolver;
        this.functions = functions;
    }

    public void table(TableIdent table) {
        this.table = table;
    }

    public TableIdent table() {
        return this.table;
    }

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceResolver.getInfo(ident);
        if (info == null) {
            throw new UnsupportedOperationException("TODO: unknown column reference?");
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
    }

    public void putReferenceInfo(Expression node, ReferenceInfo info) {
        referenceInfos.put(node, info);
    }

    public ReferenceInfo getReferenceInfo(SubscriptExpression node) {
        return referenceInfos.get(node);
    }

    public FunctionInfo getFunctionInfo(FunctionCall node) {
        return functionInfos.get(node);
    }
}
