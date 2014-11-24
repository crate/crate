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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CreateAnalyzerStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateAnalyzerAnalyzedStatement> {

    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Inject
    public CreateAnalyzerStatementAnalyzer(FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    @Override
    public Void visitCreateAnalyzer(CreateAnalyzer node, CreateAnalyzerAnalyzedStatement context) {
        context.ident(node.ident());

        if (node.isExtending()) {
            context.extendedAnalyzer(node.extendedAnalyzer().get());
        }

        for (AnalyzerElement element : node.elements()) {
            process(element, context);
        }

        return null;
    }

    @Override
    public Void visitTokenizer(Tokenizer tokenizer, CreateAnalyzerAnalyzedStatement context) {
        String name = tokenizer.ident();
        Optional<io.crate.sql.tree.GenericProperties> properties = tokenizer.properties();


        if (!properties.isPresent()) {
            // use a builtin tokenizer without parameters

            // validate
            if (!context.analyzerService().hasBuiltInTokenizer(name)) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Non-existing tokenizer '%s'", name));
            }
            // build
            context.tokenDefinition(name, ImmutableSettings.EMPTY);
        } else {
            // validate
            if (!context.analyzerService().hasBuiltInTokenizer(name)) {
                // type mandatory
                String evaluatedType = extractType(properties.get(), context);
                if (!context.analyzerService().hasBuiltInTokenizer(evaluatedType)) {
                    // only builtin tokenizers can be extended, for now
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Non-existing built-in tokenizer type '%s'", evaluatedType));
                }
            } else {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH, "tokenizer name '%s' is reserved", name));
            }
            // build
            // transform name as tokenizer is not publicly available
            name = String.format("%s_%s", context.ident(), name);

            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            for (Map.Entry<String, Expression> tokenizerProperty : properties.get().properties().entrySet()) {
                genericPropertyToSetting(builder,
                        context.getSettingsKey("index.analysis.tokenizer.%s.%s", name, tokenizerProperty.getKey()),
                        tokenizerProperty.getValue(),
                        context);
            }
            context.tokenDefinition(name, builder.build());
        }


        return null;
    }

    @Override
    public Void visitGenericProperty(GenericProperty property, CreateAnalyzerAnalyzedStatement context) {
        genericPropertyToSetting(context.genericAnalyzerSettingsBuilder(),
                context.getSettingsKey("index.analysis.analyzer.%s.%s", context.ident(), property.key()),
                property.value(),
                context
        );
        return null;
    }

    @Override
    public Void visitTokenFilters(TokenFilters tokenFilters, CreateAnalyzerAnalyzedStatement context) {
        for (NamedProperties tokenFilterNode : tokenFilters.tokenFilters()) {

            String name = tokenFilterNode.ident();
            Optional<GenericProperties> properties = tokenFilterNode.properties();

            // use a builtin tokenfilter without parameters
            if (!properties.isPresent()) {
                // validate
                if (!context.analyzerService().hasBuiltInTokenFilter(name)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Non-existing built-in token-filter '%s'", name));
                }
                // build
                context.addTokenFilter(name, ImmutableSettings.EMPTY);
            } else {
                // validate
                if (!context.analyzerService().hasBuiltInTokenFilter(name)) {
                    // type mandatory when name is not a builtin filter
                    String evaluatedType = extractType(properties.get(), context);
                    if (!context.analyzerService().hasBuiltInTokenFilter(evaluatedType)) {
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
                name = String.format("%s_%s", context.ident(), name);
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, Expression> tokenFilterProperty : properties.get().properties().entrySet()) {
                    genericPropertyToSetting(builder,
                            context.getSettingsKey("index.analysis.filter.%s.%s", name, tokenFilterProperty.getKey()),
                            tokenFilterProperty.getValue(),
                            context);
                }
                context.addTokenFilter(name, builder.build());
            }
        }
        return null;
    }

    @Override
    public Void visitCharFilters(CharFilters charFilters, CreateAnalyzerAnalyzedStatement context) {
        for (NamedProperties charFilterNode : charFilters.charFilters()) {

            String name = charFilterNode.ident();
            Optional<GenericProperties> properties = charFilterNode.properties();

            // use a builtin tokenfilter without parameters
            if (!properties.isPresent()) {
                // validate
                if (!context.analyzerService().hasBuiltInCharFilter(name)) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Non-existing built-in char-filter '%s'", name));
                }
                // build
                context.addCharFilter(name, ImmutableSettings.EMPTY);
            } else {
                String type = extractType(properties.get(), context);
                if (!context.analyzerService().hasBuiltInCharFilter(type)) {
                    // only builtin char-filters can be extended, for now
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Non-existing built-in char-filter" +
                            " type '%s'", type));
                }

                // build
                // transform name as char-filter is not publicly available
                name = String.format("%s_%s", context.ident(), name);
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, Expression> charFilterProperty: properties.get().properties().entrySet()) {
                    genericPropertyToSetting(builder,
                            context.getSettingsKey("index.analysis.char_filter.%s.%s", name, charFilterProperty.getKey()),
                            charFilterProperty.getValue(),
                            context);
                }
                context.addCharFilter(name, builder.build());
            }
        }
        return null;
    }


    /**
     * Validate and process `type` property
     *
     * @param properties
     * @param context
     * @return
     */
    private String extractType(GenericProperties properties, CreateAnalyzerAnalyzedStatement context) {
        Expression expression = properties.get("type");
        if (expression == null) {
            throw new IllegalArgumentException("'type' property missing");
        }
        if (expression instanceof ArrayLiteral) {
            throw new IllegalArgumentException("'type' property is invalid");
        }

        return ExpressionToStringVisitor.convert(expression, context.parameters());
    }

    /**
     * Put a genericProperty into a settings-structure
     *
     * @param builder
     * @param name
     * @param value
     */
    private void genericPropertyToSetting(ImmutableSettings.Builder builder,
                                          String name,
                                          Expression value,
                                          CreateAnalyzerAnalyzedStatement context) {
        if (value instanceof ArrayLiteral) {
            ArrayLiteral array = (ArrayLiteral)value;
            List<String> values = new ArrayList<>(array.values().size());
            for (Expression expression : array.values()) {
                values.add(ExpressionToStringVisitor.convert(expression, context.parameters()));
            }
            builder.putArray(name, values.toArray(new String[values.size()]));
        } else  {
            builder.put(name, ExpressionToStringVisitor.convert(value, context.parameters()));
        }
    }

    @Override
    public AnalyzedStatement newAnalysis(ParameterContext parameterContext) {
        return new CreateAnalyzerAnalyzedStatement(fulltextAnalyzerResolver, parameterContext);
    }
}
