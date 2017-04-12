package io.crate.operation.udf;

import io.crate.metadata.FunctionImplementation;

import javax.annotation.Nullable;
import javax.script.ScriptException;


public interface UDFLanguage {

    FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException;

    @Nullable
    String validate(UserDefinedFunctionMetaData metadata);

    String name();

}

