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
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.ImmutableSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CreateAnalyzerStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateAnalyzerAnalysis> {

    private final ExpressionVisitor expressionVisitor = new ExpressionVisitor();

    @Override
    public Void visitCreateAnalyzer(CreateAnalyzer node, CreateAnalyzerAnalysis context) {
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
    public Void visitTokenizer(Tokenizer tokenizer, CreateAnalyzerAnalysis context) {
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
            for (Map.Entry<String, List<Expression>> tokenizerProperty : properties.get().properties().entrySet()) {
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
    public Void visitGenericProperty(GenericProperty property, CreateAnalyzerAnalysis context) {
        genericPropertyToSetting(context.genericAnalyzerSettingsBuilder(),
                context.getSettingsKey("index.analysis.analyzer.%s.%s", context.ident(), property.key()),
                property.value(),
                context
        );
        return null;
    }

    @Override
    public Void visitTokenFilters(TokenFilters tokenFilters, CreateAnalyzerAnalysis context) {
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
                for (Map.Entry<String, List<Expression>> tokenFilterProperty : properties.get().properties().entrySet()) {
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
    public Void visitCharFilters(CharFilters charFilters, CreateAnalyzerAnalysis context) {
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
                for (Map.Entry<String, List<Expression>> charFilterProperty: properties.get().properties().entrySet()) {
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
    private String extractType(GenericProperties properties, CreateAnalyzerAnalysis context) {
        List<Expression> expressions = properties.get("type");
        if (expressions == null) {
            throw new IllegalArgumentException("'type' property missing");
        }
        if (expressions.size() != 1) {
            throw new IllegalArgumentException("'type' property is invalid");
        }

        return expressionVisitor.process(expressions.get(0), context.parameters());
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
                                          List<Expression> value,
                                          CreateAnalyzerAnalysis context) {
        if (value.size() == 1) {
            builder.put(name, expressionVisitor.process(value.get(0), context.parameters()));
        } else if (value.size() > 1) {
            List<String> values = new ArrayList<>(value.size());
            for (Expression expression : value) {
                values.add(expressionVisitor.process(expression, context.parameters()));
            }
            builder.putArray(name, values.toArray(new String[values.size()]));
        }
    }

    private class ExpressionVisitor extends AstVisitor<String, Object[]> {

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Object[] parameters) {
            return node.getName().getSuffix();
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Object[] parameters) {
            return node.getValue();
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Object[] parameters) {
            return Boolean.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Object[] parameters) {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Object[] parameters) {
            return Long.toString(node.getValue());
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Object[] parameters) {
            return parameters[node.index()].toString();
        }

        @Override
        protected String visitNegativeExpression(NegativeExpression node, Object[] context) {
            return "-" + process(node.getValue(), context);
        }

        @Override
        protected String visitNode(Node node, Object[] context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
        }
    }

}
