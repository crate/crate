package org.cratedb.action.sql.analyzer;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.Analyzer;
import org.cratedb.service.SQLService;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.io.IOException;

/**
 * Service to get builtin and custom analyzers, tokenizers, token_filters, char_filters
 */
public class AnalyzerService {

    private final ClusterService clusterService;
    private final IndicesAnalysisService analysisService;

    // redefined list of extended analyzers not available outside of
    // a concrete index (see AnalyzerModule.ExtendedProcessor)
    // TODO: maybe extract them from ExtendedProcessor
    private static final ImmutableSet<String> EXTENDED_BUILTIN_TOKEN_FILTERS = ImmutableSet.of("snowball",
            "stemmer", "word_delimiter", "synonym",
            "elision", "keep", "pattern_capture", "pattern_replace",
            "dictionary_decompounder", "hyphenation_decompounder",
            "arabic_stem", "brazilian_stem", "czech_stem", "dutch_stem", "french_stem",
            "german_stem", "russian_stem", "keyword_marker", "stemmer_override",
            "arabic_normalization", "persian_normalization",
            "hunspell", "cjk_bigram", "cjk_width");
    private static final ImmutableSet<String> EXTENDED_BUILTIN_TOKENIZERS = ImmutableSet.of("pattern");
    private static final ImmutableSet<String> EXTENDED_BUILTIN_CHAR_FILTERS = ImmutableSet.of("mapping",
            "html_strip", "pattern_replace");
    private static final ImmutableSet<String> EXTENDED_BUILTIN_ANALYZERS = ImmutableSet.of("pattern",
            "snowball", "arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan",
            "chinese", "cjk", "czech", "danish", "dutch",
            "english", "finnish", "french", "galician", "german", "greek", "hindi", "hungarian",
            "indonesian", "italian", "latvian", "norwegian", "persian", "portuguese",
            "romanian", "russian", "spanish", "swedish", "turkish", "thai");

    public enum CustomType {
        ANALYZER("analyzer"),
        TOKENIZER("tokenizer"),
        TOKEN_FILTER("filter"),
        CHAR_FILTER("char_filter");

        private String name;

        private CustomType(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    @Inject
    public AnalyzerService(ClusterService clusterService, IndicesAnalysisService analysisService) {
        this.clusterService = clusterService;
        this.analysisService = analysisService;
    }

    public boolean hasAnalyzer(String name) {
        return hasBuiltInAnalyzer(name) || hasCustomAnalyzer(name);
    }

    public boolean hasBuiltInAnalyzer(String name) {
        return EXTENDED_BUILTIN_ANALYZERS.contains(name) || analysisService.hasAnalyzer(name);
    }

    public Analyzer getBuiltInAnalyzer(String name) {
        return analysisService.analyzer(name);
    }

    public boolean hasCustomAnalyzer(String name) {
        return hasCustomThingy(name, CustomType.ANALYZER);
    }


    public boolean hasTokenizer(String name) {
        return hasBuiltInTokenizer(name) || hasCustomTokenizer(name);
    }

    public boolean hasBuiltInTokenizer(String name) {
        return EXTENDED_BUILTIN_TOKENIZERS.contains(name) || analysisService.hasTokenizer(name);
    }

    public boolean hasCustomTokenizer(String name) {
        return hasCustomThingy(name, CustomType.TOKENIZER);
    }


    public boolean hasCharFilter(String name) {
        return hasBuiltInCharFilter(name) || hasCustomCharFilter(name);
    }

    public boolean hasBuiltInCharFilter(String name) {
        return EXTENDED_BUILTIN_CHAR_FILTERS.contains(name) || analysisService.hasCharFilter(name);
    }

    public boolean hasCustomCharFilter(String name) {
        return hasCustomThingy(name, CustomType.CHAR_FILTER);
    }


    public boolean hasTokenFilter(String name) {
        return hasBuiltInTokenFilter(name) || hasCustomTokenFilter(name);
    }

    public boolean hasBuiltInTokenFilter(String name) {
        return EXTENDED_BUILTIN_TOKEN_FILTERS.contains(name) || analysisService.hasTokenFilter(name);
    }

    public boolean hasCustomTokenFilter(String name) {
        return hasCustomThingy(name, CustomType.TOKEN_FILTER);
    }

    public static BytesReference encodeSettings(Settings settings) throws IOException {
        BytesStreamOutput bso = new BytesStreamOutput();
        ImmutableSettings.writeSettingsToStream(settings, bso);
        return bso.bytes();
    }

    public static Settings decodeSettings(String encodedSettings) throws IOException {
        BytesStreamInput bsi = new BytesStreamInput(encodedSettings.getBytes(), 0, encodedSettings.getBytes().length, false);
        return ImmutableSettings.readSettingsFromStream(bsi);

    }

    private Settings getCustomThingy(String name, CustomType type) {
        String encodedSettings = clusterService.state().metaData().persistentSettings().get(
                String.format("%s.%s.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, type.getName(), name)
        );
        Settings decoded = null;
        if (encodedSettings != null) {
            try {
                decoded = decodeSettings(encodedSettings);
            } catch (IOException e) {
                // ignore
            }
        }
        return decoded;
    }

    private boolean hasCustomThingy(String name, CustomType type) {
        return clusterService.state().metaData().persistentSettings().getAsMap().containsKey(
                String.format("%s.%s.%s", SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX, type.getName(), name));
    }


    /**
     * get the custom analyzer created by the CREATE ANALYZER command.
     * This does not include definitions for custom tokenizers, token-filters or char-filters
     *
     * @param name the name of the analyzer
     * @return Settings defining a custom Analyzer
     */
    public Settings getCustomAnalyzer(String name) {
        Settings analyzerSettings = getCustomThingy(name, CustomType.ANALYZER);
        return analyzerSettings;
    }

    /**
     * resolve the full settings necessary for the custom analyzer with name ``name``
     * to be included in index-settings to get applied on an index.
     *
     * Resolves all custom tokenizer, token-filter and char-filter settings and includes them
     *
     * @param name the name of the analyzer to resolve
     * @return Settings ready for inclusion into a CreateIndexRequest
     * @throws StandardException if no custom analyzer with name ``name`` could be found
     */
    public Settings resolveFullCustomAnalyzerSettings(String name) throws StandardException {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        Settings analyzerSettings = getCustomAnalyzer(name);
        if (analyzerSettings != null) {

            builder.put(analyzerSettings);

            String tokenizerName = analyzerSettings.get(String.format("index.analysis.analyzer.%s.tokenizer", name));
            if (tokenizerName != null) {
                Settings customTokenizerSettings = getCustomTokenizer(tokenizerName);
                if (customTokenizerSettings != null) {
                    builder.put(customTokenizerSettings);
                }
            }

            String[] tokenFilterNames = analyzerSettings.getAsArray(String.format("index.analysis.analyzer.%s.filter", name));
            for (int i=0; i<tokenFilterNames.length; i++) {
                Settings customTokenFilterSettings = getCustomTokenFilter(tokenFilterNames[i]);
                if (customTokenFilterSettings != null) {
                    builder.put(customTokenFilterSettings);
                }
            }

            String[] charFilterNames = analyzerSettings.getAsArray(String.format("index.analysis.analyzer.%s.char_filter", name));
            for (int i=0; i<tokenFilterNames.length; i++) {
                Settings customCharFilterSettings = getCustomCharFilter(charFilterNames[i]);
                if (customCharFilterSettings != null) {
                    builder.put(customCharFilterSettings);
                }
            }
        } else {
            throw new StandardException(String.format("No custom analyzer with name '%s'", name));
        }
        return builder.build();
    }

    public TokenizerFactory getBuiltinTokenizer(String name) {
        return analysisService.tokenizerFactoryFactory(name).create(null, null); // arguments do not matter here
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
