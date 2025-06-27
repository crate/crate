/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.analysis;

import static org.elasticsearch.plugins.AnalysisPlugin.requiresAnalysisSettings;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.hunspell.Dictionary;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.HunspellTokenFilterFactory;
import org.elasticsearch.index.analysis.KeywordAnalyzerProvider;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.ShingleTokenFilterFactory;
import org.elasticsearch.index.analysis.SimpleAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardAnalyzerProvider;
import org.elasticsearch.index.analysis.StandardTokenizerFactory;
import org.elasticsearch.index.analysis.StopAnalyzerProvider;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.analysis.WhitespaceAnalyzerProvider;
import org.elasticsearch.plugins.AnalysisPlugin;

import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;

/**
 * Sets up {@link AnalysisRegistry}.
 */
public final class AnalysisModule {

    static {
        IndexMetadata metadata = IndexMetadata.builder(UUIDs.randomBase64UUID())
            .indexName("_na_")
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata .SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .build())
            .build();
        NA_INDEX_SETTINGS = new IndexSettings(metadata, Settings.EMPTY);
    }

    private static final IndexSettings NA_INDEX_SETTINGS;

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(LogManager.getLogger(AnalysisModule.class));

    private final AnalysisRegistry analysisRegistry;

    public AnalysisModule(Environment environment, List<AnalysisPlugin> plugins) throws IOException {
        var hunspellDictionaries = MapBuilder.<String, Dictionary>newMapBuilder()
            .putUnique("dictionary", Lists.mapLazy(plugins, AnalysisPlugin::getHunspellDictionaries))
            .immutableMap();
        var hunspellService = new HunspellService(environment.settings(), environment, hunspellDictionaries);

        var tokenFilters = MapBuilder.<String, AnalysisProvider<TokenFilterFactory>>newMapBuilder()
            .putUnique("token_filter", "stop", StopTokenFilterFactory::new)
            .putUnique("token_filter", "standard", (indexSettings, _, name, settings) -> {
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("standard_deprecation",
                    "The [standard] token filter name is deprecated and will be removed in a future version.");
                return new AbstractTokenFilterFactory(indexSettings, name, settings) {
                    @Override
                    public TokenStream create(TokenStream tokenStream) {
                        return tokenStream;
                    }
                };
            })
            .putUnique("token_filter", "shingle", ShingleTokenFilterFactory::new)
            .putUnique("token_filter", "hunspell", requiresAnalysisSettings((indexSettings, _, name, settings) ->
                new HunspellTokenFilterFactory(
                    indexSettings,
                    name,
                    settings,
                    hunspellService
                )
            ))
            .putUnique("token_filter", Lists.mapLazy(plugins, AnalysisPlugin::getTokenFilters))
            .immutableMap();

        var tokenizers = MapBuilder.<String, AnalysisProvider<TokenizerFactory>>newMapBuilder()
            .putUnique("tokenizer", "standard", StandardTokenizerFactory::new)
            .putUnique("tokenizer", Lists.mapLazy(plugins, AnalysisPlugin::getTokenizers))
            .immutableMap();

        var analyzers = MapBuilder.<String, AnalysisProvider<AnalyzerProvider<?>>>newMapBuilder()
            .putUnique("analyzers", "default", StandardAnalyzerProvider::new)
            .putUnique("analyzers", "standard", StandardAnalyzerProvider::new)
            .putUnique("analyzers", "simple", SimpleAnalyzerProvider::new)
            .putUnique("analyzers", "stop", StopAnalyzerProvider::new)
            .putUnique("analyzers", "whitespace", WhitespaceAnalyzerProvider::new)
            .putUnique("analyzers", "keyword", KeywordAnalyzerProvider::new)
            .putUnique("analyzers", Lists.mapLazy(plugins, AnalysisPlugin::getAnalyzers))
            .immutableMap();

        Map<String, AnalysisProvider<AnalyzerProvider<?>>> normalizers = Map.of();

        var preConfiguredCharFilters = preConfig(
            "pre-configured char_filter",
            plugins,
            AnalysisPlugin::getPreConfiguredCharFilters,
            PreConfiguredCharFilter::getName
        );
        var preConfiguredAnalyzers = preConfig(
            "pre-built analyzer",
            plugins,
            AnalysisPlugin::getPreBuiltAnalyzerProviderFactories,
            PreBuiltAnalyzerProviderFactory::getName
        );

        analysisRegistry = new AnalysisRegistry(
            environment,
            MapBuilder.<String, AnalysisProvider<CharFilterFactory>>newMapBuilder()
                .putUnique("char_filter", Lists.mapLazy(plugins, AnalysisPlugin::getCharFilters))
                .immutableMap(),
            tokenFilters,
            tokenizers,
            analyzers,
            normalizers,
            preConfiguredCharFilters,
            setupPreConfiguredTokenFilters(plugins),
            setupPreConfiguredTokenizers(plugins),
            preConfiguredAnalyzers
        );
    }

    private static <T> Map<String, T> preConfig(String mapName,
                                                List<AnalysisPlugin> plugins,
                                                Function<AnalysisPlugin,
                                                List<T>> getItems, Function<T, String> getKey) {
        var map = MapBuilder.<String, T>newMapBuilder();
        for (AnalysisPlugin plugin : plugins) {
            List<T> items = getItems.apply(plugin);
            for (T item : items) {
                map.putUnique(mapName, getKey.apply(item), item);
            }
        }
        return map.immutableMap();
    }

    public AnalysisRegistry getAnalysisRegistry() {
        return analysisRegistry;
    }


    static Map<String, PreConfiguredTokenFilter> setupPreConfiguredTokenFilters(List<AnalysisPlugin> plugins) {
        String name = "pre-configured token_filter";
        var preConfiguredTokenFilters = MapBuilder.<String, PreConfiguredTokenFilter>newMapBuilder()
            // Add filters available in lucene-core
            .putUnique(name, "lowercase", PreConfiguredTokenFilter.singleton("lowercase", true, LowerCaseFilter::new))
            .putUnique(name, "standard", PreConfiguredTokenFilter.singletonWithVersion("standard", false, (reader, _) -> {
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("standard_deprecation",
                    "The [standard] token filter is deprecated and will be removed in a future version.");
                return reader;
            }));

        /* Note that "stop" is available in lucene-core but it's pre-built
         * version uses a set of English stop words that are in
         * lucene-analysis-common so "stop" is defined in the analysis-common
         * module. */
        for (AnalysisPlugin plugin: plugins) {
            for (PreConfiguredTokenFilter filter : plugin.getPreConfiguredTokenFilters()) {
                preConfiguredTokenFilters.putUnique(name, filter.getName(), filter);
            }
        }
        return preConfiguredTokenFilters.immutableMap();
    }

    static Map<String, PreConfiguredTokenizer> setupPreConfiguredTokenizers(List<AnalysisPlugin> plugins) {
        String mapName = "pre-configured tokenizer";
        var preConfiguredTokenizers = MapBuilder.<String, PreConfiguredTokenizer>newMapBuilder();

        // Temporary shim to register old style pre-configured tokenizers
        for (PreBuiltTokenizers tokenizer : PreBuiltTokenizers.values()) {
            String name = tokenizer.name().toLowerCase(Locale.ROOT);
            PreConfiguredTokenizer preConfigured;
            switch (tokenizer.getCachingStrategy()) {
                case ONE:
                    preConfigured = PreConfiguredTokenizer.singleton(
                        name,
                        () -> tokenizer.create(Version.CURRENT),
                        null
                    );
                    break;
                default:
                    throw new UnsupportedOperationException(
                        "Caching strategy unsupported by temporary shim [" + tokenizer + "]");
            }
            preConfiguredTokenizers.putUnique(mapName, name, preConfigured);
        }
        for (AnalysisPlugin plugin: plugins) {
            for (PreConfiguredTokenizer tokenizer : plugin.getPreConfiguredTokenizers()) {
                preConfiguredTokenizers.putUnique(mapName, tokenizer.getName(), tokenizer);
            }
        }

        return preConfiguredTokenizers.immutableMap();
    }

    /**
     * The basic factory interface for analysis components.
     */
    public interface AnalysisProvider<T> {

        /**
         * Creates a new analysis provider.
         *
         * @param indexSettings the index settings for the index this provider is created for
         * @param environment   the nodes environment to load resources from persistent storage
         * @param name          the name of the analysis component
         * @param settings      the component specific settings without context prefixes
         * @return a new provider instance
         * @throws IOException if an {@link IOException} occurs
         */
        T get(IndexSettings indexSettings, Environment environment, String name, Settings settings) throws IOException;

        /**
         * Creates a new global scope analysis provider without index specific settings not settings for the provider itself.
         * This can be used to get a default instance of an analysis factory without binding to an index.
         *
         * @param environment the nodes environment to load resources from persistent storage
         * @param name        the name of the analysis component
         * @return a new provider instance
         * @throws IOException              if an {@link IOException} occurs
         * @throws IllegalArgumentException if the provider requires analysis settings ie. if {@link #requiresAnalysisSettings()} returns
         *                                  <code>true</code>
         */
        default T get(Environment environment, String name) throws IOException {
            if (requiresAnalysisSettings()) {
                throw new IllegalArgumentException("Analysis settings required - can't instantiate analysis factory");
            }
            return get(NA_INDEX_SETTINGS, environment, name, NA_INDEX_SETTINGS.getSettings());
        }

        /**
         * If <code>true</code> the analysis component created by this provider requires certain settings to be instantiated.
         * it can't be created with defaults. The default is <code>false</code>.
         */
        default boolean requiresAnalysisSettings() {
            return false;
        }
    }
}
