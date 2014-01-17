/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.cratedb.action.parser.visitors;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.service.SQLParseService;
import org.cratedb.service.SQLService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Parsing CREATE ANALYZER Statements.
 */
public class AnalyzerVisitor extends BaseVisitor {
    private String analyzerName = null;
    private String sql_stmt;
    private String extendedAnalyzerName = null;
    private Settings genericAnalyzerSettings = null;
    private Settings extendedCustomAnalyzer = null;

    private Tuple<String, Settings> tokenizerDefinition = null;
    private Map<String,Settings> charFilters = new HashMap<>();
    private Map<String, Settings> tokenFilters = new HashMap<>();
    private final AnalyzerService analyzerService;

    // used for saving the creation statement
    public static final String SQL_STATEMENT_KEY = "_sql_stmt";

    public AnalyzerVisitor(NodeExecutionContext context, ParsedStatement parsedStatement, Object[] args) {
        super(context, parsedStatement, args);
        analyzerService = context.analyzerService();
    }

    public boolean extendsCustomAnalyzer() {
        return extendedAnalyzerName != null
            && extendedCustomAnalyzer != null
            && extendedCustomAnalyzer.get(String.format("index.analysis.analyzer.%s.type",
                extendedAnalyzerName)).equals("custom");
    }

    public boolean extendsBuiltInAnalyzer() {
        return extendedAnalyzerName != null && (extendedCustomAnalyzer == null ||
                !extendedCustomAnalyzer.get(String.format("index.analysis.analyzer.%s.type",
                        extendedAnalyzerName)).equals("custom"));
    }

    @Override
    public void visit(CreateAnalyzerNode node) throws StandardException {
        sql_stmt = new SQLParseService(context).unparse(stmt, args);
        analyzerName = node.getObjectName().getTableName(); // extend to use schema here
        if (analyzerName.equalsIgnoreCase("default")) {
            throw new SQLParseException("Overriding the default analyzer is forbidden");
        }
        if (analyzerService.hasBuiltInAnalyzer(analyzerName)) {
            throw new SQLParseException(String.format("Cannot override builtin analyzer '%s'", analyzerName));
        }
        if (node.getExtendsName() != null) {
            visit(node.getExtendsName());
        }
        visit(node.getElements());

        stmt.type(ParsedStatement.ActionType.CREATE_ANALYZER_ACTION);
    }

    public void visit(TableName extendsName) throws StandardException {
        String extended = extendsName.getTableName();
        if (!analyzerService.hasAnalyzer(extended)) {
            throw new SQLParseException(String.format("Extended Analyzer '%s' does not exist", extended));
        }
        extendedAnalyzerName = extended;
        // resolve custom Analyzer, if any
        extendedCustomAnalyzer = analyzerService.getCustomAnalyzer(extendedAnalyzerName);
    }

    public void visit(AnalyzerElements elements) throws StandardException {
        if (elements.getTokenizer() != null) {
            visit(elements.getTokenizer());
        }
        if (extendsBuiltInAnalyzer()) {
            visit(elements.getProperties());
        } else {
            visit(elements.getTokenFilters());
            visit(elements.getCharFilters());
        }
    }

    /**
     * evaluate tokenizer-definition and check for correctness
     * @param tokenizer
     * @return
     * @throws StandardException
     */
    public void visit(NamedNodeWithOptionalProperties tokenizer) throws StandardException {

        String name = tokenizer.getName();
        GenericProperties properties = tokenizer.getProperties();


        if (properties == null) {
            // use a builtin tokenizer without parameters

            // validate
            if (!analyzerService.hasBuiltInTokenizer(name)) {
                throw new SQLParseException(String.format("Non-existing tokenizer '%s'", name));
            }
            // build
            tokenizerDefinition = new Tuple<>(name, ImmutableSettings.EMPTY);
        } else {
            // validate
            if (!analyzerService.hasBuiltInTokenizer(name)) {
                // type mandatory
                String evaluatedType = extractType(properties);
                if (!analyzerService.hasBuiltInTokenizer(evaluatedType)) {
                    // only builtin tokenizers can be extended, for now
                    throw new SQLParseException(String.format("Non-existing built-in tokenizer " +
                            "type '%s'", evaluatedType));
                }
            } else {
                throw new SQLParseException(String.format("tokenizer name '%s' is reserved", name));
            }
            // build
            // transform name as tokenizer is not publicly available
            name = String.format("%s_%s", analyzerName, name);

            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            for (Map.Entry<String, QueryTreeNode> tokenizerProperty : properties.iterator()) {
                genericPropertyToSetting(builder,
                        getSettingsKey("index.analysis.tokenizer.%s.%s", name, tokenizerProperty.getKey()),
                        tokenizerProperty.getValue());
            }
            tokenizerDefinition = new Tuple<>(name, builder.build());
        }
    }

    public void visit(GenericProperties properties) throws StandardException {
        if (properties.hasProperties()) {
            ImmutableSettings.Builder builder = ImmutableSettings.builder();
            for (Map.Entry<String, QueryTreeNode> prop : properties.iterator()) {
                genericPropertyToSetting(builder,
                        getSettingsKey("index.analysis.analyzer.%s.%s", analyzerName, prop.getKey()),
                        prop.getValue()
                );
            }
            genericAnalyzerSettings = builder.build();
        }
    }

    public void visit(TokenFilterList tokenFilterList) throws StandardException {
        for (NamedNodeWithOptionalProperties tokenFilterNode : tokenFilterList) {

            String name = tokenFilterNode.getName();
            GenericProperties properties = tokenFilterNode.getProperties();

            // use a builtin tokenfilter without parameters
            if (properties == null) {
                // validate
                if (!analyzerService.hasBuiltInTokenFilter(name)) {
                    throw new SQLParseException(String.format("Non-existing built-in token-filter '%s'", name));
                }
                // build
                tokenFilters.put(name, ImmutableSettings.EMPTY);
            } else {
                // validate
                if (!analyzerService.hasBuiltInTokenFilter(name)) {
                    // type mandatory when name is not a builtin filter
                    String evaluatedType = extractType(properties);
                    if (!analyzerService.hasBuiltInTokenFilter(evaluatedType)) {
                        // only builtin token-filters can be extended, for now
                        throw new SQLParseException(String.format("Non-existing " +
                                "built-in token-filter type '%s'", evaluatedType));
                    }
                } else {
                    if (properties.get("type") != null) {
                        throw new SQLParseException(String.format("token-filter name '%s' is reserved, 'type' property forbidden here", name));
                    }
                }

                // build
                // transform name as token-filter is not publicly available
                name = String.format("%s_%s", analyzerName, name);
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, QueryTreeNode> tokenFilterProperty : properties.iterator()) {
                    genericPropertyToSetting(builder,
                            getSettingsKey("index.analysis.filter.%s.%s", name, tokenFilterProperty.getKey()),
                            tokenFilterProperty.getValue());
                }
                tokenFilters.put(name, builder.build());
            }
        }
    }


    public void visit(CharFilterList charFilterList) throws StandardException {
        for (NamedNodeWithOptionalProperties charFilterNode : charFilterList) {

            String name = charFilterNode.getName();
            GenericProperties properties = charFilterNode.getProperties();

            // use a builtin tokenfilter without parameters
            if (properties == null) {
                // validate
                if (!analyzerService.hasBuiltInCharFilter(name)) {
                    throw new SQLParseException(String.format("Non-existing built-in char-filter '%s'", name));
                }
                // build
                charFilters.put(name, ImmutableSettings.EMPTY);
            } else {
                String type = extractType(properties);
                if (!analyzerService.hasBuiltInCharFilter(type)) {
                    // only builtin char-filters can be extended, for now
                    throw new SQLParseException(String.format("Non-existing built-in char-filter" +
                            " type '%s'", type));
                }

                // build
                // transform name as char-filter is not publicly available
                name = String.format("%s_%s", analyzerName, name);
                ImmutableSettings.Builder builder = ImmutableSettings.builder();
                for (Map.Entry<String, QueryTreeNode> charFilterProperty: properties.iterator()) {
                    genericPropertyToSetting(builder,
                            getSettingsKey("index.analysis.char_filter.%s.%s", name, charFilterProperty.getKey()),
                            charFilterProperty.getValue());
                }
                charFilters.put(name, builder.build());
            }
        }
    }


    @Override
    protected void afterVisit() throws SQLParseException {
        super.afterVisit();
        try {
            stmt.createAnalyzerSettings = buildSettings();
        } catch (IOException ioe) {
            throw new SQLParseException("Could not build analyzer Settings", ioe);
        }
    }


    // HELPER METHODS
    /**
     * validate Type Property
     */
    private String extractType(GenericProperties properties) throws StandardException {
        // validate
        QueryTreeNode typeNode = properties.get("type");
        if (typeNode == null) {
            throw new SQLParseException("'type' property missing");
        }

        if (!(typeNode instanceof ValueNode)) {
            throw new SQLParseException("'type' property invalid");
        }
        return (String)valueFromNode((ValueNode) typeNode);
    }

    /**
     * put a genericProperty into a settings-structure, hide the details of handling valuenodes and lists thereof
     * @param builder
     * @param name
     * @param value
     * @throws StandardException
     */
    private void genericPropertyToSetting(ImmutableSettings.Builder builder, String name, QueryTreeNode value) throws StandardException {
        if (value instanceof ValueNode) {
            builder.put(name, valueFromNode((ValueNode) value));
        } else if (value instanceof ValueNodeList) {
            ValueNodeList valueNodeList = (ValueNodeList)value;
            List<String> values = new ArrayList<>(valueNodeList.size());
            for (ValueNode node : valueNodeList) {
                values.add(valueFromNode(node).toString());
            }
            builder.putArray(name, values.toArray(new String[values.size()]));
        }
    }

    public static String getSettingsKey(String suffix, Object ... formatArgs) {
        if (formatArgs != null) {
            suffix = String.format(suffix, formatArgs);
        }
        return suffix;
    }

    public static String getPrefixedSettingsKey(String suffix, Object ... formatArgs) {
        if (formatArgs != null) {
            suffix = String.format(suffix, formatArgs);
        }
        return String.format("%s.%s", SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, suffix);
    }

    /**
     * create analyzer settings - possibly referencing charFilters, tokenFilters, tokenizers defined here
     * @return Settings describing a custom or extended builtin-analyzer
     */
    private Settings analyzerSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        if (extendsCustomAnalyzer()) {
            // use analyzer-settings from extended analyzer only
            Settings stripped = extendedCustomAnalyzer.getByPrefix(String.format("index.analysis.analyzer.%s", extendedAnalyzerName));
            for (Map.Entry<String, String> entry : stripped.getAsMap().entrySet()) {
                builder.put(String.format("index.analysis.analyzer.%s%s", analyzerName, entry.getKey()), entry.getValue());
            }

            if (tokenizerDefinition == null) {
                // set tokenizer if not defined in extending analyzer
                String extendedTokenizerName = extendedCustomAnalyzer.get(String.format("index.analysis.analyzer.%s.tokenizer", extendedAnalyzerName));
                if (extendedTokenizerName != null) {
                    Settings extendedTokenizerSettings = analyzerService.getCustomTokenizer(extendedTokenizerName);
                    if (extendedTokenizerSettings != null) {
                        tokenizerDefinition = new Tuple<>(extendedTokenizerName, extendedTokenizerSettings);
                    } else {
                        tokenizerDefinition = new Tuple<>(extendedTokenizerName, ImmutableSettings.EMPTY);
                    }
                }
            }

            if (tokenFilters.isEmpty()) {
                // only use inherited tokenfilters if none are defined in extending analyzer
                String[] extendedTokenFilterNames = extendedCustomAnalyzer.getAsArray(String.format("index.analysis.analyzer.%s.filter", extendedAnalyzerName));
                for (int i=0;i<extendedTokenFilterNames.length;i++) {
                    Settings extendedTokenFilterSettings = analyzerService.getCustomTokenFilter(extendedTokenFilterNames[i]);
                    if (extendedTokenFilterSettings != null) {
                        tokenFilters.put(extendedTokenFilterNames[i], extendedTokenFilterSettings);
                    } else {
                        tokenFilters.put(extendedTokenFilterNames[i], ImmutableSettings.EMPTY);
                    }
                }
            }

            if (charFilters.isEmpty()) {
                // only use inherited charfilters if none are defined in extending analyzer
                String[] extendedCustomCharFilterNames = extendedCustomAnalyzer.getAsArray(String.format("index.analysis.analyzer.%s.char_filter", extendedAnalyzerName));
                for (int i=0; i<extendedCustomCharFilterNames.length; i++) {
                    Settings extendedCustomCharFilterSettings = analyzerService.getCustomCharFilter(extendedCustomCharFilterNames[i]);
                    if (extendedCustomCharFilterSettings != null) {
                        charFilters.put(extendedCustomCharFilterNames[i], extendedCustomCharFilterSettings);
                    } else {
                        charFilters.put(extendedCustomCharFilterNames[i], ImmutableSettings.EMPTY);
                    }
                }
            }

        } else if(extendsBuiltInAnalyzer()) {
            // generic properties for extending builtin analyzers
            if (genericAnalyzerSettings != null) {
                builder.put(genericAnalyzerSettings);
            }
        }

        // analyzer type
        String analyzerType = "custom";
        if (extendsBuiltInAnalyzer()){
            if (extendedCustomAnalyzer != null) {
                analyzerType = extendedCustomAnalyzer.get(
                        String.format("index.analysis.analyzer.%s.type", extendedAnalyzerName)
                );
            } else {
                // direct extending builtin analyzer, use name as type
                analyzerType = extendedAnalyzerName;
            }
        }
        builder.put(
                getSettingsKey("index.analysis.analyzer.%s.type", analyzerName),
                analyzerType
        );

        if (tokenizerDefinition != null) {
            builder.put(
                    getSettingsKey("index.analysis.analyzer.%s.tokenizer", analyzerName),
                    tokenizerDefinition.v1()
            );
        }
        if (charFilters.size() > 0) {
            String[] charFilterNames = charFilters.keySet().toArray(new String[charFilters.size()]);
            builder.putArray(
                    getSettingsKey("index.analysis.analyzer.%s.char_filter", analyzerName),
                    charFilterNames
            );
        }
        if (tokenFilters.size() > 0) {
            String[] tokenFilterNames = tokenFilters.keySet().toArray(new String[tokenFilters.size()]);
            builder.putArray(
                    getSettingsKey("index.analysis.analyzer.%s.filter", analyzerName),
                    tokenFilterNames
            );
        }
        return builder.build();
    }

    /**
     * build settings ready for putting into clusterstate
     * @return the analyzer settings corresponding to the analyzed <tt>CREATE ANALYZER</tt> statement
     * @throws SettingsException in case we can't build the settings yet
     */
    private Settings buildSettings() throws IOException {

        ImmutableSettings.Builder builder = ImmutableSettings.builder();

        String encodedAnalyzerSettings = AnalyzerService.encodeSettings(analyzerSettings()).toUtf8();
        builder.put(
                String.format("%s.analyzer.%s", SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, analyzerName),
                encodedAnalyzerSettings
        );
        // set source
        builder.put(
                String.format("%s.analyzer.%s.%s",
                        SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, analyzerName,
                        AnalyzerVisitor.SQL_STATEMENT_KEY),
                sql_stmt
        );

        if (tokenizerDefinition != null && !tokenizerDefinition.v2().getAsMap().isEmpty()) {
            builder.put(
                    String.format("%s.tokenizer.%s", SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, tokenizerDefinition.v1()),
                    AnalyzerService.encodeSettings(tokenizerDefinition.v2()).toUtf8()
            );
        }
        for (Map.Entry<String, Settings> tokenFilterDefinition: tokenFilters.entrySet()) {
            if (!tokenFilterDefinition.getValue().getAsMap().isEmpty()) {
                builder.put(
                        String.format("%s.filter.%s", SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, tokenFilterDefinition.getKey()),
                        AnalyzerService.encodeSettings(tokenFilterDefinition.getValue()).toUtf8()
                );
            }
        }
        for (Map.Entry<String, Settings> charFilterDefinition : charFilters.entrySet()) {
            if (!charFilterDefinition.getValue().getAsMap().isEmpty()) {
                builder.put(
                        String.format("%s.char_filter.%s", SQLService.CUSTOM_ANALYSIS_SETTINGS_PREFIX, charFilterDefinition.getKey()),
                        AnalyzerService.encodeSettings(charFilterDefinition.getValue()).toUtf8()
                );
            }
        }
        return builder.build();
    }
}
