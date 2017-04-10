package io.crate.operation.language;


import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptException;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

@Singleton
public class JavaScriptLanguage implements UDFLanguage {

    private final String name;
    private final ESLogger logger;

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService, Settings settings) {
        logger = Loggers.getLogger(JavaScriptLanguage.class, settings);
        this.name = "javascript";
        udfService.registerLanguage(this);
    }

    public FunctionImplementation createFunctionImplementation(UserDefinedFunctionMetaData meta) throws ScriptException {
        CompiledScript compiledScript = ((Compilable) JavaScriptUserDefinedFunction.ENGINE)
            .compile(meta.definition());
        return new JavaScriptUserDefinedFunction(
            new FunctionIdent(meta.schema(), meta.name(), meta.argumentTypes()),
            meta.returnType(),
            compiledScript
        );
    }

    public void validate(UserDefinedFunctionMetaData metadata) throws Exception {

    }

    public String name() {
        return this.name;
    }
}
