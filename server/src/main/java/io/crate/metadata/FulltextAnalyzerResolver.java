/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.metadata;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.CHAR_FILTER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKENIZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKEN_FILTER;
import static io.crate.metadata.settings.AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import io.crate.exceptions.AnalyzerInvalidException;
import io.crate.exceptions.AnalyzerUnknownException;

/**
 * Service to get builtin and custom analyzers, tokenizers, token_filters, char_filters
 */
public class FulltextAnalyzerResolver {

    private final ClusterService clusterService;
    private final AnalysisRegistry analysisRegistry;

    // redefined list of extended analyzers not available outside of
    // a concrete index (see AnalyzerModule.ExtendedProcessor)
    // stripped Prebuilt<Thingy> (e.g. PreBuiltTokenFilters)
    private static final Set<String> EXTENDED_BUILTIN_TOKEN_FILTERS = Set.of(
        "limit", "synonym",
        "keep", "pattern_capture", "pattern_replace",
        "dictionary_decompounder", "hyphenation_decompounder",
        "keyword_marker", "stemmer_override",
        "hunspell", "cjk_bigram", "cjk_width");
    private static final Set<String> EXTENDED_BUILTIN_CHAR_FILTERS =
        Set.of("mapping", "pattern_replace");

    // used for saving the creation statement
    public static final String SQL_STATEMENT_KEY = "_sql_stmt";


    public enum CustomType {
        ANALYZER("analyzer"),
        TOKENIZER("tokenizer"),
        TOKEN_FILTER("filter"),
        CHAR_FILTER("char_filter");

        private static final String INDEX_ANALYSIS_PREFIX = "index.analysis.";

        private final String name;
        private final String settingNamePrefix;
        private final String settingChildNamePrefix;

        CustomType(String name) {
            this.name = name;
            settingNamePrefix = CUSTOM_ANALYSIS_SETTINGS_PREFIX + name + ".";
            settingChildNamePrefix = INDEX_ANALYSIS_PREFIX + name + ".";
        }

        public String getName() {
            return this.name;
        }

        /**
         * Build the setting name this custom type definition will be stored under.
         */
        public String buildSettingName(String customName) {
            return settingNamePrefix + customName;
        }

        /**
         * Build the child setting name for a custom type definition.
         */
        public String buildSettingChildName(String customName, String settingName) {
            return buildSettingChildNamePrefix(customName) + "." + settingName;
        }

        /**
         * Build the child setting prefix for a custom type definition.
         */
        public String buildSettingChildNamePrefix(String customName) {
            return settingChildNamePrefix + customName;
        }
    }

    @Inject
    public FulltextAnalyzerResolver(ClusterService clusterService,
                                    AnalysisRegistry analysisRegistry) {
        this.clusterService = clusterService;
        this.analysisRegistry = analysisRegistry;
    }

    public boolean hasAnalyzer(String name) {
        return hasBuiltInAnalyzer(name) || hasCustomAnalyzer(name);
    }

    public boolean hasBuiltInAnalyzer(String name) {
        try {
            return analysisRegistry.getAnalyzer(name) != null;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * get all the builtin Analyzers defined in Crate
     *
     * @return an Iterable of Strings
     */
    public Set<String> getBuiltInAnalyzers() {
        return Set.copyOf(analysisRegistry.getAnalyzers().keySet());
    }

    /**
     * get the custom analyzer created by the CREATE ANALYZER command.
     * This does not include definitions for custom tokenizers, token-filters or char-filters
     *
     * @param name the name of the analyzer
     * @return Settings defining a custom Analyzer
     */
    public Settings getCustomAnalyzer(String name) {
        return getCustomThingy(name, ANALYZER);
    }


    public Map<String, Settings> getCustomAnalyzers() throws IOException {
        Map<String, Settings> result = new HashMap<>();
        Settings analyzer = getCustomThingies(ANALYZER);
        for (String settingName : analyzer.keySet()) {
            if (!settingName.endsWith("." + SQL_STATEMENT_KEY)) {
                result.put(settingName, decodeSettings(analyzer.get(settingName)));
            }
        }
        return result;
    }

    public boolean hasCustomAnalyzer(String name) {
        return hasCustomThingy(name, ANALYZER);
    }


    public boolean hasBuiltInTokenizer(String name) {
        return analysisRegistry.getTokenizerProvider(name) != null;
    }

    public Set<String> getBuiltInTokenizers() {
        return Set.copyOf(analysisRegistry.getTokenizers().keySet());
    }

    public Map<String, Settings> getCustomTokenizers() throws IOException {
        return getDecodedSettings(getCustomThingies(CustomType.TOKENIZER));
    }

    public boolean hasBuiltInCharFilter(String name) {
        return EXTENDED_BUILTIN_CHAR_FILTERS.contains(name) || analysisRegistry.getCharFilterProvider(name) != null;
    }

    public Set<String> getBuiltInCharFilters() {
        var charFilters = new HashSet<String>();
        charFilters.addAll(EXTENDED_BUILTIN_CHAR_FILTERS);
        charFilters.addAll(analysisRegistry.getCharFilters().keySet());
        return Collections.unmodifiableSet(charFilters);
    }

    public Map<String, Settings> getCustomCharFilters() throws IOException {
        return getDecodedSettings(getCustomThingies(CustomType.CHAR_FILTER));
    }

    public boolean hasBuiltInTokenFilter(String name) {
        return EXTENDED_BUILTIN_TOKEN_FILTERS.contains(name) || analysisRegistry.getTokenFilterProvider(name) != null;
    }

    public Set<String> getBuiltInTokenFilters() {
        var tokenFilters = new HashSet<String>();
        tokenFilters.addAll(EXTENDED_BUILTIN_TOKEN_FILTERS);
        tokenFilters.addAll(analysisRegistry.getTokenFilters().keySet());
        return Collections.unmodifiableSet(tokenFilters);
    }

    public Map<String, Settings> getCustomTokenFilters() throws IOException {
        return getDecodedSettings(getCustomThingies(CustomType.TOKEN_FILTER));
    }

    private static Map<String, Settings> getDecodedSettings(Settings settings) throws IOException {
        Map<String, Settings> result = new HashMap<>();
        for (String name : settings.keySet()) {
            result.put(name, decodeSettings(settings.get(name)));
        }
        return result;
    }

    public static BytesReference encodeSettings(Settings settings) {
        try {
            XContentBuilder builder = JsonXContent.builder();
            builder.startObject();
            settings.toXContent(builder, false);
            builder.endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            // this is a memory stream so no real I/O happens and a IOException can't really happen at runtime
            throw new RuntimeException(e);
        }
    }

    public static Settings decodeSettings(String encodedSettings) throws IOException {
        return Settings.builder().loadFromSource(encodedSettings, XContentType.JSON).build();
    }

    /**
     * used to get custom analyzers, tokenizers, token-filters or char-filters with name ``name``
     * from crate-cluster-settings
     *
     * @param name
     * @param type
     * @return a full settings instance for the thingy with given name and type or null if it does not exists
     */
    private Settings getCustomThingy(String name, CustomType type) {
        if (name == null) {
            return null;
        }
        String encodedSettings = clusterService.state().metadata().persistentSettings().get(type.buildSettingName(name));
        if (encodedSettings == null) {
            return null;
        }
        try {
            return decodeSettings(encodedSettings);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Settings getCustomThingies(CustomType type) {
        Map<String, Settings> settingsMap = clusterService.state().metadata().persistentSettings()
            .getGroups(CUSTOM_ANALYSIS_SETTINGS_PREFIX);
        Settings result = settingsMap.get(type.getName());
        return result != null ? result : Settings.EMPTY;
    }

    /**
     * used to check if custom analyzer, tokenizer, token-filter or char-filter with name ``name`` exists
     *
     * @param name
     * @param type
     * @return true if exists, false otherwise
     */
    public boolean hasCustomThingy(String name, CustomType type) {
        return clusterService.state().metadata().persistentSettings().hasValue(type.buildSettingName(name));
    }

    /**
     * resolve the full settings necessary for the custom analyzer with name ``name``
     * to be included in index-settings to get applied on an index.
     * <p>
     * Resolves all custom tokenizer, token-filter and char-filter settings and includes them
     *
     * @param name the name of the analyzer to resolve
     * @return Settings ready for inclusion into a CreateIndexRequest
     * @throws AnalyzerInvalidException if no custom analyzer with name ``name`` could be found
     */
    public Settings resolveFullCustomAnalyzerSettings(String name) throws AnalyzerInvalidException {
        Settings.Builder builder = Settings.builder();
        Settings analyzerSettings = getCustomAnalyzer(name);
        if (analyzerSettings != null) {

            builder.put(analyzerSettings);

            String tokenizerName = analyzerSettings.get(ANALYZER.buildSettingChildName(name, TOKENIZER.getName()));
            if (tokenizerName != null) {
                Settings customTokenizerSettings = getCustomTokenizer(tokenizerName);
                if (customTokenizerSettings != null) {
                    builder.put(customTokenizerSettings);
                } else if (!hasBuiltInTokenizer(tokenizerName)) {
                    throw new AnalyzerInvalidException(
                        String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve tokenizer '%s'", tokenizerName));
                }
            }

            List<String> tokenFilterNames = analyzerSettings.getAsList(ANALYZER.buildSettingChildName(name, TOKEN_FILTER.getName()));
            for (String tokenFilterName : tokenFilterNames) {
                Settings customTokenFilterSettings = getCustomTokenFilter(tokenFilterName);
                if (customTokenFilterSettings != null) {
                    builder.put(customTokenFilterSettings);
                } else if (!hasBuiltInTokenFilter(tokenFilterName)) {
                    throw new AnalyzerInvalidException(
                        String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve token-filter '%s'", tokenFilterName));
                }
            }

            List<String> charFilterNames = analyzerSettings.getAsList(ANALYZER.buildSettingChildName(name, CHAR_FILTER.getName()));
            for (String charFilterName : charFilterNames) {
                Settings customCharFilterSettings = getCustomCharFilter(charFilterName);
                if (customCharFilterSettings != null) {
                    builder.put(customCharFilterSettings);
                } else if (!hasBuiltInCharFilter(charFilterName)) {
                    throw new AnalyzerInvalidException(
                        String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve char-filter '%s'", charFilterName));
                }
            }
        } else {
            throw new AnalyzerUnknownException(name);
        }
        return builder.build();
    }

    public Settings getCustomTokenizer(String name) {
        return getCustomThingy(name, CustomType.TOKENIZER);
    }

    public Settings getCustomTokenFilter(String name) {
        return getCustomThingy(name, CustomType.TOKEN_FILTER);
    }

    public Settings getCustomCharFilter(String name) {
        return getCustomThingy(name, CustomType.CHAR_FILTER);
    }
}
