package io.crate.operation.udf;

import io.crate.metadata.Scalar;

import javax.annotation.Nullable;
import javax.script.ScriptException;


/**
 * Common interface for languages for user-defined functions.
 *
 * A language must be registered with the {@link UserDefinedFunctionService} in order to be able to create functions
 * based on that language.
 *
 * A language must provide a unique name which is exposed via the {@link #name()} method.
 * It is also responsible for validating the function script provided by the meta data
 * as well as for creating the function implementation from the meta data.
 */
public interface UDFLanguage {

    /**
     * Create the function implementation for a function from its meta data.
     * @param metaData from the cluster state
     * @return the function implementation
     * @throws ScriptException if the implementation cannot be created
     */
    Scalar createFunctionImplementation(UserDefinedFunctionMetaData metaData) throws ScriptException;

    /**
     * Validate the function code provided by the meta data.
     * @param metadata holding information about the user-defined function
     * @return error message if validation of the function fails, otherwise null
     */
    @Nullable
    String validate(UserDefinedFunctionMetaData metadata);

    /**
     * @return name of the language
     */
    String name();

}

