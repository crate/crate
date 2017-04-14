package io.crate.operation.language;


import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Scalar;
import io.crate.operation.udf.UDFLanguage;
import io.crate.operation.udf.UserDefinedFunctionMetaData;
import io.crate.operation.udf.UserDefinedFunctionService;
import io.crate.types.DataType;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Locale;
import java.util.stream.Collectors;

public class JavaScriptLanguage implements UDFLanguage {

    private final String name;

    static final ScriptEngine ENGINE = new NashornScriptEngineFactory().getScriptEngine("--no-java", "--no-syntax-extensions");

    @Inject
    public JavaScriptLanguage(UserDefinedFunctionService udfService, Settings settings) {
        this.name = "javascript";
        udfService.registerLanguage(this);
    }

    public Scalar createFunctionImplementation(UserDefinedFunctionMetaData meta) throws ScriptException {
        CompiledScript compiledScript = ((Compilable) ENGINE).compile(meta.definition());

        return new JavaScriptUserDefinedFunction(
            new FunctionIdent(meta.schema(), meta.name(), meta.argumentTypes()),
            meta.returnType(),
            compiledScript
        );
    }

    @Nullable
    public String validate(UserDefinedFunctionMetaData meta) {
        try {
            ((Compilable) ENGINE).compile(meta.definition());
        } catch (ScriptException e){
            return  String.format(Locale.ENGLISH,
                "Invalid JavaScript in function '%s(%s)'",
                meta.name(),
                meta.argumentTypes().stream().map(DataType::getName)
                    .collect(Collectors.joining(", "))
            );
        }
        return null;
    }

    public String name() {
        return this.name;
    }
}
