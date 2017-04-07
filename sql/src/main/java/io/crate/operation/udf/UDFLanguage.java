package io.crate.operation.udf;

import io.crate.metadata.FunctionImplementation;

import javax.script.ScriptException;


public interface UDFLanguage {

    public FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException;

    public void validate(UserDefinedFunctionMetaData metadata) throws Exception;

    public String name();

}

