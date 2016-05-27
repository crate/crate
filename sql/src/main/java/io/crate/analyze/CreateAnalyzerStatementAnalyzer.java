/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import com.google.common.base.Optional;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.sql.tree.*;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.Map;

@Singleton
public class CreateAnalyzerStatementAnalyzer extends DefaultTraversalVisitor<
    CreateAnalyzerAnalyzedStatement, CreateAnalyzerStatementAnalyzer.Context> {

    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private final DeprecationLogger deprecationLogger;

    @Inject
    public CreateAnalyzerStatementAnalyzer(FulltextAnalyzerResolver fulltextAnalyzerResolver, Settings settings) {
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        deprecationLogger = new DeprecationLogger(Loggers.getLogger(CreateAnalyzerStatementAnalyzer.class, settings));
    }

    public CreateAnalyzerAnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return super.process(node, new Context(analysis));
    }

    static class Context {

        Analysis analysis;
        CreateAnalyzerAnalyzedStatement statement;

        public Context(Analysis analysis) {
            this.analysis = analysis;
        }
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitCreateAnalyzer(CreateAnalyzer node, Context context) {
        String underscoreIdent = Strings.toUnderscoreCase(node.ident());
        if (!underscoreIdent.equalsIgnoreCase(node.ident())) {
            deprecationLogger.deprecated("Deprecated analyzer name [{}], use [{}] instead", node.ident(), underscoreIdent);
        }

        context.statement = new CreateAnalyzerAnalyzedStatement(fulltextAnalyzerResolver);
        context.statement.ident(node.ident());

        if (node.isExtending()) {
            context.statement.extendedAnalyzer(node.extendedAnalyzer().get());
        }

        for (AnalyzerElement element : node.elements()) {
            process(element, context);
        }

        return context.statement;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitTokenizer(Tokenizer tokenizer, Context context) {
        String name = tokenizer.ident();
        Optional<io.crate.sql.tree.GenericProperties> properties = tokenizer.properties();

        String underscoreName = Strings.toUnderscoreCase(name);
        if (!underscoreName.equalsIgnoreCase(name)) {
            deprecationLogger.deprecated("Deprecated tokenizer name [{}], use [{}] instead", name, underscoreName);
        }

        if (!properties.isPresent()) {
            // use a builtin tokenizer without parameters

            // validate
            if (!context.statement.analyzerService().hasBuiltInTokenizer(name)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Non-existing tokenizer '%s'", name));
            }
            // build
            context.statement.tokenDefinition(name, Settings.EMPTY);
        } else {
            // validate
            if (!context.statement.analyzerService().hasBuiltInTokenizer(name)) {
                // type mandatory
                String evaluatedType = extractType(properties.get(), context.analysis.parameterContext());
                if (!context.statement.analyzerService().hasBuiltInTokenizer(evaluatedType)) {
                    // only builtin tokenizers can be extended, for now
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Non-existing built-in tokenizer type '%s'", evaluatedType));
                }
            } else {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "tokenizer name '%s' is reserved", name));
            }
            // build
            // transform name as tokenizer is not publicly available
            name = String.format(Locale.ENGLISH, "%s_%s", context.statement.ident(), name);

            Settings.Builder builder = Settings.builder();
            for (Map.Entry<String, Expression> tokenizerProperty : properties.get().properties().entrySet()) {
                GenericPropertiesConverter.genericPropertyToSetting(builder,
                    context.statement.getSettingsKey("index.analysis.tokenizer.%s.%s", name, tokenizerProperty.getKey()),
                    tokenizerProperty.getValue(),
                    context.analysis.parameterContext());
            }
            context.statement.tokenDefinition(name, builder.build());
        }

        return null;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitGenericProperty(GenericProperty property, Context context) {
        GenericPropertiesConverter.genericPropertyToSetting(context.statement.genericAnalyzerSettingsBuilder(),
            context.statement.getSettingsKey("index.analysis.analyzer.%s.%s", context.statement.ident(), property.key()),
            property.value(),
            context.analysis.parameterContext()
        );
        return null;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitTokenFilters(TokenFilters tokenFilters, Context context) {
        for (NamedProperties tokenFilterNode : tokenFilters.tokenFilters()) {

            String name = tokenFilterNode.ident();
            Optional<GenericProperties> properties = tokenFilterNode.properties();

            String underscoreName = Strings.toUnderscoreCase(name);
            if (!underscoreName.equalsIgnoreCase(name)) {
                deprecationLogger.deprecated("Deprecated token_filter name [{}], use [{}] instead", name, underscoreName);
            }

            // use a builtin token-filter without parameters
            if (!properties.isPresent()) {
                // validate
                if (!context.statement.analyzerService().hasBuiltInTokenFilter(name)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Non-existing built-in token-filter '%s'", name));
                }
                // build
                context.statement.addTokenFilter(name, Settings.EMPTY);
            } else {
                // validate
                if (!context.statement.analyzerService().hasBuiltInTokenFilter(name)) {
                    // type mandatory when name is not a builtin filter
                    String evaluatedType = extractType(properties.get(), context.analysis.parameterContext());
                    if (!context.statement.analyzerService().hasBuiltInTokenFilter(evaluatedType)) {
                        // only builtin token-filters can be extended, for now
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Non-existing built-in token-filter type '%s'", evaluatedType));
                    }
                } else {
                    if (properties.get().get("type") != null) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "token-filter name '%s' is reserved, 'type' property forbidden here", name));
                    }
                }

                // build
                // transform name as token-filter is not publicly available
                name = String.format(Locale.ENGLISH, "%s_%s", context.statement.ident(), name);
                Settings.Builder builder = Settings.builder();
                for (Map.Entry<String, Expression> tokenFilterProperty : properties.get().properties().entrySet()) {
                    GenericPropertiesConverter.genericPropertyToSetting(builder,
                        context.statement.getSettingsKey("index.analysis.filter.%s.%s", name, tokenFilterProperty.getKey()),
                        tokenFilterProperty.getValue(),
                        context.analysis.parameterContext());
                }
                context.statement.addTokenFilter(name, builder.build());
            }
        }
        return null;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitCharFilters(CharFilters charFilters, Context context) {
        for (NamedProperties charFilterNode : charFilters.charFilters()) {

            String name = charFilterNode.ident();
            Optional<GenericProperties> properties = charFilterNode.properties();

            String underscoreName = Strings.toUnderscoreCase(name);
            if (!underscoreName.equalsIgnoreCase(name)) {
                deprecationLogger.deprecated("Deprecated char_filter name [{}], use [{}] instead", name, underscoreName);
            }

            // use a builtin char-filter without parameters
            if (!properties.isPresent()) {
                // validate
                if (!context.statement.analyzerService().hasBuiltInCharFilter(name)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Non-existing built-in char-filter '%s'", name));
                }
                // build
                context.statement.addCharFilter(name, Settings.EMPTY);
            } else {
                String type = extractType(properties.get(), context.analysis.parameterContext());
                if (!context.statement.analyzerService().hasBuiltInCharFilter(type)) {
                    // only builtin char-filters can be extended, for now
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Non-existing built-in char-filter" +
                        " type '%s'", type));
                }

                // build
                // transform name as char-filter is not publicly available
                name = String.format(Locale.ENGLISH, "%s_%s", context.statement.ident(), name);
                Settings.Builder builder = Settings.builder();
                for (Map.Entry<String, Expression> charFilterProperty : properties.get().properties().entrySet()) {
                    GenericPropertiesConverter.genericPropertyToSetting(builder,
                        context.statement.getSettingsKey("index.analysis.char_filter.%s.%s", name, charFilterProperty.getKey()),
                        charFilterProperty.getValue(),
                        context.analysis.parameterContext());
                }
                context.statement.addCharFilter(name, builder.build());
            }
        }
        return null;
    }


    /**
     * Validate and process `type` property
     */
    private String extractType(GenericProperties properties, ParameterContext parameterContext) {
        Expression expression = properties.get("type");
        if (expression == null) {
            throw new IllegalArgumentException("'type' property missing");
        }
        if (expression instanceof ArrayLiteral) {
            throw new IllegalArgumentException("'type' property is invalid");
        }

        return ExpressionToStringVisitor.convert(expression, parameterContext.parameters());
    }

}
