package org.cratedb.action;

import org.apache.lucene.analysis.Analyzer;
import org.cratedb.service.SQLService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.util.Arrays;
import java.util.Map;

/**
 * Service to get builtin and custom analyzers, tokenizers, token_filters, char_filters
 * and for setting custom analyzers, tokenizers, token_filters, char_filters
 */
public class AnalyzerService {

    private final ClusterService clusterService;
    private final IndicesAnalysisService analysisService;

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
        return Arrays.asList("pattern").contains(name) || analysisService.hasTokenizer(name);
    }

    public boolean hasCharFilter(String name) {
        return hasBuiltInCharFilter(name) || getCustomCharFilter(name) != null;
    }

    public boolean hasBuiltInCharFilter(String name) {
        return Arrays.asList("mapping").contains(name) || analysisService.hasCharFilter(name);
    }

    public boolean hasTokenFilter(String name) {
        return hasBuiltInTokenFilter(name) || getCustomTokenFilter(name) != null;
    }

    public boolean hasBuiltInTokenFilter(String name) {
        return Arrays.asList("snowball", "stemmer", "word_delimiter", "synonym",
                "elision", "keep", "pattern_capture", "pattern_replace",
                "dictionary_decompounder", "hyphenation_decompounder",
                "arabic_stem", "brazilian_stem", "czech_stem", "dutch_stem", "french_stem", "german_stem", "russian_stem",
                "keyword_marker", "stemmer_override",
                "arabic_normalization", "persian_normalization",
                "hunspell", "cjk_bigram", "cjk_width").contains(name) || analysisService.hasTokenFilter(name);
    }

    public boolean hasBuiltInAnalyzer(String name) {
        return Arrays.asList("pattern", "snowball", "arabic", "armenian", "basque", "brazilian", "bulgarian", "catalan", "chinese", "cjk", "czech", "danish", "dutch",
                "english", "finnish", "french", "galician", "german", "greek", "hindi", "hungarian",
                "indonesian", "italian", "latvian", "norwegian", "persian", "portuguese",
                "romanian", "russian", "spanish", "swedish", "turkish", "thai").contains(name) || analysisService.hasAnalyzer(name);
    }

    public Analyzer getBuiltInAnalyzer(String name) {
        return analysisService.analyzer(name);
    }

    private Settings getSettings() throws SettingsException {
        try {
            String source = clusterService.state().metaData().persistentSettings().get(SQLService.CUSTOM_ANALYZER_SETTINGS_PREFIX);
            return ImmutableSettings.builder().loadFromSource(source).build();
        } catch(SettingsException | NullPointerException e) {
            return ImmutableSettings.EMPTY;
        }
    }

    private Settings getCustomThingy(String name, CustomType type) {
        Settings settings = getSettings();
        Map<String, Settings> thingyMap = settings.getGroups(type.getName());
        return thingyMap.get(name);
    }

    /**
     * get the custom analyzer created by the CREATE ANALYZER command
     * @param name the name of the analyzer
     * @return Settings defining a custom Analyzer
     */
    public Settings getCustomAnalyzer(String name) {
        return getCustomThingy(name, CustomType.ANALYZER);
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
