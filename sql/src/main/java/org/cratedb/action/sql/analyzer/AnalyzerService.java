package org.cratedb.action.sql.analyzer;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.analysis.Analyzer;
import org.cratedb.service.SQLService;
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
        return hasBuiltInAnalyzer(name) || getCustomAnalyzer(name) != null;
    }

    public boolean hasTokenizer(String name) {
        return hasBuiltInTokenizer(name) || getCustomTokenizer(name) != null;
    }

    public boolean hasBuiltInTokenizer(String name) {
        return EXTENDED_BUILTIN_TOKENIZERS.contains(name) || analysisService.hasTokenizer(name);
    }

    public boolean hasCharFilter(String name) {
        return hasBuiltInCharFilter(name) || getCustomCharFilter(name) != null;
    }

    public boolean hasBuiltInCharFilter(String name) {
        return EXTENDED_BUILTIN_CHAR_FILTERS.contains(name) || analysisService.hasCharFilter(name);
    }

    public boolean hasTokenFilter(String name) {
        return hasBuiltInTokenFilter(name) || getCustomTokenFilter(name) != null;
    }

    public boolean hasBuiltInTokenFilter(String name) {
        return EXTENDED_BUILTIN_TOKEN_FILTERS.contains(name) || analysisService.hasTokenFilter(name);
    }

    public boolean hasBuiltInAnalyzer(String name) {
        return EXTENDED_BUILTIN_ANALYZERS.contains(name) || analysisService.hasAnalyzer(name);
    }

    public Analyzer getBuiltInAnalyzer(String name) {
        return analysisService.analyzer(name);
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

    /**
     * get the custom analyzer created by the CREATE ANALYZER command
     * @param name the name of the analyzer
     * @return Settings defining a custom Analyzer
     */
    public Settings getCustomAnalyzer(String name) {
        Settings analyzerSettings = getCustomThingy(name, CustomType.ANALYZER);
        return analyzerSettings;
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
