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

import io.crate.metadata.FulltextAnalyzerResolver;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static io.crate.metadata.settings.AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX;

public class CreateAnalyzerAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    private String ident;
    private String extendedAnalyzerName = null;
    private Settings extendedCustomAnalyzer = null;
    private Settings genericAnalyzerSettings = null;
    private Settings.Builder genericAnalyzerSettingsBuilder = Settings.builder();
    private Tuple<String, Settings> tokenizerDefinition = null;
    private Map<String, Settings> charFilters = new HashMap<>();
    private Map<String, Settings> tokenFilters = new HashMap<>();

    public CreateAnalyzerAnalyzedStatement(FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateAnalyzerStatement(this, context);
    }

    public FulltextAnalyzerResolver analyzerService() {
        return fulltextAnalyzerResolver;
    }


    public void ident(String ident) {
        if (ident.equalsIgnoreCase("default")) {
            throw new IllegalArgumentException("Overriding the default analyzer is forbidden");
        }
        if (fulltextAnalyzerResolver.hasBuiltInAnalyzer(ident)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Cannot override builtin analyzer '%s'", ident));
        }
        this.ident = ident;
    }

    public String ident() {
        return ident;
    }

    public void extendedAnalyzer(String name) {
        if (!fulltextAnalyzerResolver.hasAnalyzer(name)) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                "Extended Analyzer '%s' does not exist", name));
        }
        extendedAnalyzerName = name;
        // resolve custom Analyzer, if any
        extendedCustomAnalyzer = fulltextAnalyzerResolver.getCustomAnalyzer(name);
    }

    @Nullable
    public String extendedAnalyzerName() {
        return extendedAnalyzerName;
    }

    public void tokenDefinition(String name, Settings settings) {
        tokenizerDefinition = new Tuple<>(name, settings);
    }

    @Nullable
    public Tuple<String, Settings> tokenizerDefinition() {
        return tokenizerDefinition;
    }

    public Settings genericAnalyzerSettings() {
        if (genericAnalyzerSettings == null) {
            genericAnalyzerSettings = genericAnalyzerSettingsBuilder.build();
        }
        return genericAnalyzerSettings;
    }

    public Settings.Builder genericAnalyzerSettingsBuilder() {
        return genericAnalyzerSettingsBuilder;
    }

    public void addTokenFilter(String name, Settings settings) {
        tokenFilters.put(name, settings);
    }

    @Nullable
    public Map<String, Settings> tokenFilters() {
        return tokenFilters;
    }

    public void addCharFilter(String name, Settings settings) {
        charFilters.put(name, settings);
    }

    @Nullable
    public Map<String, Settings> charFilters() {
        return charFilters;
    }

    public boolean extendsCustomAnalyzer() {
        return extendedAnalyzerName != null
               && extendedCustomAnalyzer != null
               && extendedCustomAnalyzer.get(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.type",
            extendedAnalyzerName)).equals("custom");
    }

    public boolean extendsBuiltInAnalyzer() {
        return extendedAnalyzerName != null && (extendedCustomAnalyzer == null ||
                                                !extendedCustomAnalyzer.get(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.type",
                                                    extendedAnalyzerName)).equals("custom"));
    }

    /**
     * create analyzer settings - possibly referencing charFilters, tokenFilters, tokenizers defined here
     *
     * @return Settings describing a custom or extended builtin-analyzer
     */
    private Settings analyzerSettings() {
        Settings.Builder builder = Settings.builder();

        if (extendsCustomAnalyzer()) {
            // use analyzer-settings from extended analyzer only
            Settings stripped = extendedCustomAnalyzer.getByPrefix(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s", extendedAnalyzerName));

            for (String settingName : stripped.keySet()) {
                builder.put("index.analysis.analyzer." + ident + settingName, stripped.get(settingName));
            }

            if (tokenizerDefinition == null) {
                // set tokenizer if not defined in extending analyzer
                String extendedTokenizerName = extendedCustomAnalyzer.get(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.tokenizer", extendedAnalyzerName));
                if (extendedTokenizerName != null) {
                    Settings extendedTokenizerSettings = fulltextAnalyzerResolver.getCustomTokenizer(extendedTokenizerName);
                    if (extendedTokenizerSettings != null) {
                        tokenizerDefinition = new Tuple<>(extendedTokenizerName, extendedTokenizerSettings);
                    } else {
                        tokenizerDefinition = new Tuple<>(extendedTokenizerName, Settings.EMPTY);
                    }
                }
            }

            if (tokenFilters.isEmpty()) {
                // only use inherited tokenfilters if none are defined in extending analyzer
                String[] extendedTokenFilterNames = extendedCustomAnalyzer.getAsArray(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.filter", extendedAnalyzerName));
                for (int i = 0; i < extendedTokenFilterNames.length; i++) {
                    Settings extendedTokenFilterSettings = fulltextAnalyzerResolver.getCustomTokenFilter(extendedTokenFilterNames[i]);
                    if (extendedTokenFilterSettings != null) {
                        tokenFilters.put(extendedTokenFilterNames[i], extendedTokenFilterSettings);
                    } else {
                        tokenFilters.put(extendedTokenFilterNames[i], Settings.EMPTY);
                    }
                }
            }

            if (charFilters.isEmpty()) {
                // only use inherited charfilters if none are defined in extending analyzer
                String[] extendedCustomCharFilterNames = extendedCustomAnalyzer.getAsArray(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.char_filter", extendedAnalyzerName));
                for (int i = 0; i < extendedCustomCharFilterNames.length; i++) {
                    Settings extendedCustomCharFilterSettings = fulltextAnalyzerResolver.getCustomCharFilter(extendedCustomCharFilterNames[i]);
                    if (extendedCustomCharFilterSettings != null) {
                        charFilters.put(extendedCustomCharFilterNames[i], extendedCustomCharFilterSettings);
                    } else {
                        charFilters.put(extendedCustomCharFilterNames[i], Settings.EMPTY);
                    }
                }
            }

        } else if (extendsBuiltInAnalyzer()) {
            // generic properties for extending builtin analyzers
            if (genericAnalyzerSettings() != null) {
                builder.put(genericAnalyzerSettings());
            }
        }

        // analyzer type
        String analyzerType = "custom";
        if (extendsBuiltInAnalyzer()) {
            if (extendedCustomAnalyzer != null) {
                analyzerType = extendedCustomAnalyzer.get(
                    String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.type", extendedAnalyzerName)
                );
            } else {
                // direct extending builtin analyzer, use name as type
                analyzerType = extendedAnalyzerName;
            }
        }
        builder.put(
            getSettingsKey("index.analysis.analyzer.%s.type", ident),
            analyzerType
        );

        if (tokenizerDefinition != null) {
            builder.put(
                getSettingsKey("index.analysis.analyzer.%s.tokenizer", ident),
                tokenizerDefinition.v1()
            );
        } else if (!extendsBuiltInAnalyzer()) {
            throw new UnsupportedOperationException("Tokenizer missing from non-extended analyzer");
        }
        if (charFilters.size() > 0) {
            String[] charFilterNames = charFilters.keySet().toArray(new String[charFilters.size()]);
            builder.putArray(
                getSettingsKey("index.analysis.analyzer.%s.char_filter", ident),
                charFilterNames
            );
        }
        if (tokenFilters.size() > 0) {
            String[] tokenFilterNames = tokenFilters.keySet().toArray(new String[tokenFilters.size()]);
            builder.putArray(
                getSettingsKey("index.analysis.analyzer.%s.filter", ident),
                tokenFilterNames
            );
        }
        return builder.build();
    }

    /**
     * build settings ready for putting into clusterstate
     *
     * @return the analyzer settings corresponding to the analyzed <tt>CREATE ANALYZER</tt> statement
     * @throws org.elasticsearch.common.settings.SettingsException in case we can't build the settings yet
     */
    public Settings buildSettings() throws IOException {

        Settings.Builder builder = Settings.builder();

        String encodedAnalyzerSettings = FulltextAnalyzerResolver.encodeSettings(analyzerSettings()).utf8ToString();
        builder.put(
            CUSTOM_ANALYSIS_SETTINGS_PREFIX + "analyzer." + ident,
            encodedAnalyzerSettings
        );

        // TODO: save original SQL statement, so it can be displayed at information_schema.routines
        // set source
        /*
        builder.put(
                String.format(Locale.ENGLISH, "%s.analyzer.%s.%s",
                        Constants.CUSTOM_ANALYSIS_SETTINGS_PREFIX, ident,
                        AnalyzerVisitor.SQL_STATEMENT_KEY),
                sql_stmt
        );
        */

        if (tokenizerDefinition != null && !tokenizerDefinition.v2().isEmpty()) {
            builder.put(
                CUSTOM_ANALYSIS_SETTINGS_PREFIX + "tokenizer." + tokenizerDefinition.v1(),
                FulltextAnalyzerResolver.encodeSettings(tokenizerDefinition.v2()).utf8ToString()
            );
        }
        for (Map.Entry<String, Settings> tokenFilterDefinition : tokenFilters.entrySet()) {
            if (!tokenFilterDefinition.getValue().isEmpty()) {
                builder.put(
                    CUSTOM_ANALYSIS_SETTINGS_PREFIX + "filter." + tokenFilterDefinition.getKey(),
                    FulltextAnalyzerResolver.encodeSettings(tokenFilterDefinition.getValue()).utf8ToString()
                );
            }
        }
        for (Map.Entry<String, Settings> charFilterDefinition : charFilters.entrySet()) {
            if (!charFilterDefinition.getValue().isEmpty()) {
                builder.put(
                    CUSTOM_ANALYSIS_SETTINGS_PREFIX + "char_filter." + charFilterDefinition.getKey(),
                    FulltextAnalyzerResolver.encodeSettings(charFilterDefinition.getValue()).utf8ToString()
                );
            }
        }
        return builder.build();
    }

    public static String getSettingsKey(String suffix, Object... formatArgs) {
        if (formatArgs != null) {
            suffix = String.format(Locale.ENGLISH, suffix, formatArgs);
        }
        return suffix;
    }
}
