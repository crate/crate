package io.crate.operation.udf;


import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptException;

@Singleton
public class JavaScriptLanguage implements UDFLanguage {

    private final String name;

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService) {
        this.name = "javascript";
        udfService.registerLanguage(this);
    }

    public FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData meta) throws ScriptException {
        CompiledScript compiledScript = ((Compilable) JavaScriptUserDefinedFunction.ENGINE)
            .compile(meta.definition);
        return new JavaScriptUserDefinedFunction(
            new FunctionIdent(meta.schema(), meta.name(), meta.argumentTypes()),
            meta.returnType,
            compiledScript
        );
    }

    public String validate(UserDefinedFunctionMetaData metadata) {
        return null;
    }

    public String name() {
        return this.name;
    }
}
