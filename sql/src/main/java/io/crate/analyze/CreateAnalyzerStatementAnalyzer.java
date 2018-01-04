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

import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.sql.tree.AnalyzerElement;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.CharFilters;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.NamedProperties;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.TokenFilters;
import io.crate.sql.tree.Tokenizer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.crate.analyze.CreateAnalyzerAnalyzedStatement.getSettingsKey;


class CreateAnalyzerStatementAnalyzer
    extends DefaultTraversalVisitor<CreateAnalyzerAnalyzedStatement, CreateAnalyzerStatementAnalyzer.Context> {

    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    CreateAnalyzerStatementAnalyzer(FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    public CreateAnalyzerAnalyzedStatement analyze(Node node, Analysis analysis) {
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
        GenericProperties properties = tokenizer.properties();

        if (properties.isEmpty()) {
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
                String evaluatedType = extractType(properties, context.analysis.parameterContext());
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
            for (Map.Entry<String, Expression> tokenizerProperty : properties.properties().entrySet()) {
                GenericPropertiesConverter.genericPropertyToSetting(builder,
                    getSettingsKey("index.analysis.tokenizer.%s.%s", name, tokenizerProperty.getKey()),
                    tokenizerProperty.getValue(),
                    context.analysis.parameterContext().parameters());
            }
            context.statement.tokenDefinition(name, builder.build());
        }

        return null;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitGenericProperty(GenericProperty property, Context context) {
        GenericPropertiesConverter.genericPropertyToSetting(context.statement.genericAnalyzerSettingsBuilder(),
            getSettingsKey("index.analysis.analyzer.%s.%s", context.statement.ident(), property.key()),
            property.value(),
            context.analysis.parameterContext().parameters()
        );
        return null;
    }

    @Override
    public CreateAnalyzerAnalyzedStatement visitTokenFilters(TokenFilters tokenFilters, Context context) {
        for (NamedProperties tokenFilterNode : tokenFilters.tokenFilters()) {

            String name = tokenFilterNode.ident();
            GenericProperties properties = tokenFilterNode.properties();

            // use a builtin token-filter without parameters
            if (properties.isEmpty()) {
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
                    String evaluatedType = extractType(properties, context.analysis.parameterContext());
                    if (!context.statement.analyzerService().hasBuiltInTokenFilter(evaluatedType)) {
                        // only builtin token-filters can be extended, for now
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Non-existing built-in token-filter type '%s'", evaluatedType));
                    }
                } else {
                    if (properties.get("type") != null) {
                        throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "token-filter name '%s' is reserved, 'type' property forbidden here", name));
                    }
                    properties.add(new GenericProperty("type", new StringLiteral(name)));
                }

                // build
                // transform name as token-filter is not publicly available
                name = String.format(Locale.ENGLISH, "%s_%s", context.statement.ident(), name);
                Settings.Builder builder = Settings.builder();
                for (Map.Entry<String, Expression> tokenFilterProperty : properties.properties().entrySet()) {
                    GenericPropertiesConverter.genericPropertyToSetting(builder,
                        getSettingsKey("index.analysis.filter.%s.%s", name, tokenFilterProperty.getKey()),
                        tokenFilterProperty.getValue(),
                        context.analysis.parameterContext().parameters());
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
            GenericProperties properties = charFilterNode.properties();

            // use a builtin char-filter without parameters
            if (properties.isEmpty()) {
                // validate
                if (!context.statement.analyzerService().hasBuiltInCharFilter(name)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Non-existing built-in char-filter '%s'", name));
                }
                validateCharFilterProperties(name, properties);
                // build
                context.statement.addCharFilter(name, Settings.EMPTY);
            } else {
                String type = extractType(properties, context.analysis.parameterContext());
                if (!context.statement.analyzerService().hasBuiltInCharFilter(type)) {
                    // only builtin char-filters can be extended, for now
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                        "Non-existing built-in char-filter" +
                        " type '%s'", type));
                }
                validateCharFilterProperties(type, properties);

                // build
                // transform name as char-filter is not publicly available
                name = String.format(Locale.ENGLISH, "%s_%s", context.statement.ident(), name);
                Settings.Builder builder = Settings.builder();
                for (Map.Entry<String, Expression> charFilterProperty : properties.properties().entrySet()) {
                    GenericPropertiesConverter.genericPropertyToSetting(builder,
                        getSettingsKey("index.analysis.char_filter.%s.%s", name, charFilterProperty.getKey()),
                        charFilterProperty.getValue(),
                        context.analysis.parameterContext().parameters());
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

    private static void validateCharFilterProperties(String type, GenericProperties properties) {
        if (properties.isEmpty() && !type.equals("html_strip")) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "CHAR_FILTER of type '%s' needs additional parameters", type));
        }
    }

}
