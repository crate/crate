package io.crate.operation.udf;

import io.crate.metadata.FunctionImplementation;

import javax.script.ScriptException;
import javax.annotation.Nullable;


public interface UDFLanguage {

    public FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException;

    /**
     * Checks if the function definition is valid javascript,
     * if the function definition is not valid, returns a String
     * to be used in the error message. If the function definition
     * is valid null is returned.
     *
     * @param metadata the metadata of the user defined function.
     */
    @Nullable
    public String validate(UserDefinedFunctionMetaData metadata);

    public String name();

}

