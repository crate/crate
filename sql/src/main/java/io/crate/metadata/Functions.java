package io.crate.metadata;

import org.elasticsearch.common.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Functions {

    private final Map<FunctionIdent, FunctionImplementation> functionImplemnetations;

    @Inject
    public Functions(Map<FunctionIdent, FunctionImplementation> functionImplemnetations) {
        this.functionImplemnetations = functionImplemnetations;
    }

    public FunctionImplementation get(FunctionIdent ident) {
        return functionImplemnetations.get(ident);
    }
}
