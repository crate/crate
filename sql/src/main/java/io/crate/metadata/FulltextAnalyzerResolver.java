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

package io.crate.metadata;

import com.google.common.collect.ImmutableSet;
import io.crate.exceptions.AnalyzerInvalidException;
import io.crate.exceptions.AnalyzerUnknownException;
import io.crate.metadata.settings.AnalyzerSettings;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.loader.JsonSettingsLoader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Service to get builtin and custom analyzers, tokenizers, token_filters, char_filters
 */
public class FulltextAnalyzerResolver {

    private final ClusterService clusterService;
    private final AnalysisRegistry analysisRegistry;

    // redefined list of extended analyzers not available outside of
    // a concrete index (see AnalyzerModule.ExtendedProcessor)
    // stripped Prebuilt<Thingy> (e.g. PreBuiltTokenFilters)
    private static final ImmutableSet<String> EXTENDED_BUILTIN_TOKEN_FILTERS = ImmutableSet.of(
        "limit", "delimited_payload_filter", "synonym",
        "keep", "pattern_capture", "pattern_replace",
        "dictionary_decompounder", "hyphenation_decompounder",
        "keyword_marker", "stemmer_override",
        "hunspell", "cjk_bigram", "cjk_width");
    private static final ImmutableSet<String> EXTENDED_BUILTIN_CHAR_FILTERS = ImmutableSet
        .of("mapping", "pattern_replace");

    // used for saving the creation statement
    public static final String SQL_STATEMENT_KEY = "_sql_stmt";

    private static final Logger logger = Loggers.getLogger(FulltextAnalyzerResolver.class);


    public enum CustomType {
        ANALYZER("analyzer"),
        TOKENIZER("tokenizer"),
        TOKEN_FILTER("filter"),
        CHAR_FILTER("char_filter");

        private String name;

        CustomType(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
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
        return new ImmutableSet.Builder<String>()
            .addAll(analysisRegistry.getAnalyzers().keySet()).build();
    }

    /**
     * get the custom analyzer created by the CREATE ANALYZER command.
     * This does not include definitions for custom tokenizers, token-filters or char-filters
     *
     * @param name the name of the analyzer
     * @return Settings defining a custom Analyzer
     */
    public Settings getCustomAnalyzer(String name) {
        return getCustomThingy(name, CustomType.ANALYZER);
    }


    public Map<String, Settings> getCustomAnalyzers() throws IOException {
        Map<String, Settings> result = new HashMap<>();
        for (Map.Entry<String, String> entry : getCustomThingies(CustomType.ANALYZER)
            .getAsMap().entrySet()) {
            if (!entry.getKey().endsWith("." + SQL_STATEMENT_KEY)) {
                result.put(entry.getKey(), decodeSettings(entry.getValue()));
            }
        }
        return result;
    }

    public boolean hasCustomAnalyzer(String name) {
        return hasCustomThingy(name, CustomType.ANALYZER);
    }


    public boolean hasBuiltInTokenizer(String name) {
        return analysisRegistry.getTokenizerProvider(name) != null;
    }

    public Set<String> getBuiltInTokenizers() {
        return new ImmutableSet.Builder<String>()
            .addAll(analysisRegistry.getTokenizers().keySet())
            .build();
    }

    public Map<String, Settings> getCustomTokenizers() throws IOException {
        Map<String, Settings> result = new HashMap<>();
        for (Map.Entry<String, String> entry : getCustomThingies(CustomType.TOKENIZER).getAsMap
            ().entrySet()) {
            result.put(entry.getKey(), decodeSettings(entry.getValue()));
        }
        return result;
    }

    public boolean hasBuiltInCharFilter(String name) {
        return EXTENDED_BUILTIN_CHAR_FILTERS.contains(name) || analysisRegistry.getCharFilterProvider(name) != null;
    }

    public Set<String> getBuiltInCharFilters() {
        return new ImmutableSet.Builder<String>().addAll(EXTENDED_BUILTIN_CHAR_FILTERS)
            .addAll(analysisRegistry.getCharFilters().keySet())
            .build();
    }

    public Map<String, Settings> getCustomCharFilters() throws IOException {
        Map<String, Settings> result = new HashMap<>();
        for (Map.Entry<String, String> entry : getCustomThingies(CustomType.CHAR_FILTER).getAsMap
            ().entrySet()) {
            result.put(entry.getKey(), decodeSettings(entry.getValue()));
        }
        return result;
    }

    public boolean hasBuiltInTokenFilter(String name) {
        return EXTENDED_BUILTIN_TOKEN_FILTERS.contains(name) || analysisRegistry.getTokenFilterProvider(name) != null;
    }

    public Set<String> getBuiltInTokenFilters() {
        return new ImmutableSet.Builder<String>()
            .addAll(EXTENDED_BUILTIN_TOKEN_FILTERS)
            .addAll(analysisRegistry.getTokenFilters().keySet())
            .build();
    }

    public Map<String, Settings> getCustomTokenFilters() throws IOException {
        Map<String, Settings> result = new HashMap<>();
        for (Map.Entry<String, String> entry : getCustomThingies(CustomType.TOKEN_FILTER).getAsMap
            ().entrySet()) {
            result.put(entry.getKey(), decodeSettings(entry.getValue()));
        }
        return result;
    }

    public static BytesReference encodeSettings(Settings settings) {
        try {
            BytesStreamOutput bso = new BytesStreamOutput();
            XContentBuilder builder = XContentFactory.jsonBuilder(bso);
            builder.startObject();
            for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
            builder.flush();
            return bso.bytes();
        } catch (IOException e) {
            // this is a memory stream so no real I/O happens and a IOException can't really happen at runtime
            throw new RuntimeException(e);
        }
    }

    public static Settings decodeSettings(String encodedSettings) throws IOException {
        Map<String, String> loaded = new JsonSettingsLoader(false).load(encodedSettings);
        return Settings.builder().put(loaded).build();


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
        String encodedSettings = clusterService.state().metaData().persistentSettings().get(
            String.format(Locale.ENGLISH, "%s%s.%s", AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX, type.getName(), name)
        );
        Settings decoded = null;
        if (encodedSettings != null) {
            try {
                decoded = decodeSettings(encodedSettings);
            } catch (IOException e) {
                logger.warn("Could not decode settings for {} '{}'.", e, type.getName(), name);
            }
        }
        return decoded;
    }

    private Settings getCustomThingies(CustomType type) {
        Map<String, Settings> settingsMap = clusterService.state().metaData().persistentSettings
            ().getGroups(AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX);
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
    private boolean hasCustomThingy(String name, CustomType type) {
        return clusterService.state().metaData().persistentSettings().getAsMap().containsKey(
            String.format(Locale.ROOT, "%s%s.%s", AnalyzerSettings.CUSTOM_ANALYSIS_SETTINGS_PREFIX, type.getName(), name));
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

            String tokenizerName = analyzerSettings.get(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.tokenizer", name));
            if (tokenizerName != null) {
                Settings customTokenizerSettings = getCustomTokenizer(tokenizerName);
                if (customTokenizerSettings != null) {
                    builder.put(customTokenizerSettings);
                } else if (!hasBuiltInTokenizer(tokenizerName)) {
                    throw new AnalyzerInvalidException(String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve tokenizer '%s'", tokenizerName));
                }
            }

            String[] tokenFilterNames = analyzerSettings.getAsArray(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.filter", name));
            for (int i = 0; i < tokenFilterNames.length; i++) {
                Settings customTokenFilterSettings = getCustomTokenFilter(tokenFilterNames[i]);
                if (customTokenFilterSettings != null) {
                    builder.put(customTokenFilterSettings);
                } else if (!hasBuiltInTokenFilter(tokenFilterNames[i])) {
                    throw new AnalyzerInvalidException(String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve token-filter '%s'", tokenFilterNames[i]));
                }
            }

            String[] charFilterNames = analyzerSettings.getAsArray(String.format(Locale.ENGLISH, "index.analysis.analyzer.%s.char_filter", name));
            for (int i = 0; i < charFilterNames.length; i++) {
                Settings customCharFilterSettings = getCustomCharFilter(charFilterNames[i]);
                if (customCharFilterSettings != null) {
                    builder.put(customCharFilterSettings);
                } else if (!hasBuiltInCharFilter(charFilterNames[i])) {
                    throw new AnalyzerInvalidException(String.format(Locale.ENGLISH, "Invalid Analyzer: could not resolve char-filter '%s'", charFilterNames[i]));
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
