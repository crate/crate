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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.bn.BengaliNormalizationFilter;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.cjk.CJKWidthFilter;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniNormalizationFilter;
import org.apache.lucene.analysis.commongrams.CommonGramsFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.UpperCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.de.GermanNormalizationFilter;
import org.apache.lucene.analysis.de.GermanStemFilter;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fa.PersianNormalizationFilter;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hi.HindiNormalizationFilter;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.miscellaneous.KeywordRepeatFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.ScandinavianNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.TrimFilter;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.path.PathHierarchyTokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.apache.lucene.analysis.payloads.DelimitedPayloadTokenFilter;
import org.apache.lucene.analysis.payloads.TypeAsPayloadTokenFilter;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.th.ThaiTokenizer;
import org.apache.lucene.analysis.tr.ApostropheFilter;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.elasticsearch.index.analysis.PreConfiguredCharFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.PreConfiguredTokenizer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltCacheFactory.CachingStrategy;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.plugins.AnalysisPlugin.requiresAnalysisSettings;

public class CommonAnalysisPlugin extends Plugin implements AnalysisPlugin, ScriptPlugin {

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(CommonAnalysisPlugin.class));

    private final SetOnce<ScriptService> scriptService = new SetOnce<>();

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        this.scriptService.set(scriptService);
        return Collections.emptyList();
    }

    @Override
    public List<ScriptContext<?>> getContexts() {
        return Collections.singletonList(AnalysisPredicateScript.CONTEXT);
    }

    @Override
    public Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        Map<String, AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> analyzers = new TreeMap<>();
        analyzers.put("fingerprint", FingerprintAnalyzerProvider::new);
        analyzers.put("standard_html_strip", StandardHtmlStripAnalyzerProvider::new);
        analyzers.put("pattern", PatternAnalyzerProvider::new);
        analyzers.put("snowball", SnowballAnalyzerProvider::new);

        // Language analyzers:
        analyzers.put("arabic", ArabicAnalyzerProvider::new);
        analyzers.put("armenian", ArmenianAnalyzerProvider::new);
        analyzers.put("basque", BasqueAnalyzerProvider::new);
        analyzers.put("bengali", BengaliAnalyzerProvider::new);
        analyzers.put("brazilian", BrazilianAnalyzerProvider::new);
        analyzers.put("bulgarian", BulgarianAnalyzerProvider::new);
        analyzers.put("catalan", CatalanAnalyzerProvider::new);
        analyzers.put("chinese", ChineseAnalyzerProvider::new);
        analyzers.put("cjk", CjkAnalyzerProvider::new);
        analyzers.put("czech", CzechAnalyzerProvider::new);
        analyzers.put("danish", DanishAnalyzerProvider::new);
        analyzers.put("dutch", DutchAnalyzerProvider::new);
        analyzers.put("english", EnglishAnalyzerProvider::new);
        analyzers.put("finnish", FinnishAnalyzerProvider::new);
        analyzers.put("french", FrenchAnalyzerProvider::new);
        analyzers.put("galician", GalicianAnalyzerProvider::new);
        analyzers.put("german", GermanAnalyzerProvider::new);
        analyzers.put("greek", GreekAnalyzerProvider::new);
        analyzers.put("hindi", HindiAnalyzerProvider::new);
        analyzers.put("hungarian", HungarianAnalyzerProvider::new);
        analyzers.put("indonesian", IndonesianAnalyzerProvider::new);
        analyzers.put("irish", IrishAnalyzerProvider::new);
        analyzers.put("italian", ItalianAnalyzerProvider::new);
        analyzers.put("latvian", LatvianAnalyzerProvider::new);
        analyzers.put("lithuanian", LithuanianAnalyzerProvider::new);
        analyzers.put("norwegian", NorwegianAnalyzerProvider::new);
        analyzers.put("persian", PersianAnalyzerProvider::new);
        analyzers.put("portuguese", PortugueseAnalyzerProvider::new);
        analyzers.put("romanian", RomanianAnalyzerProvider::new);
        analyzers.put("russian", RussianAnalyzerProvider::new);
        analyzers.put("sorani", SoraniAnalyzerProvider::new);
        analyzers.put("spanish", SpanishAnalyzerProvider::new);
        analyzers.put("swedish", SwedishAnalyzerProvider::new);
        analyzers.put("turkish", TurkishAnalyzerProvider::new);
        analyzers.put("thai", ThaiAnalyzerProvider::new);
        return analyzers;
    }

    @Override
    public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
        Map<String, AnalysisProvider<TokenFilterFactory>> filters = new TreeMap<>();
        filters.put("apostrophe", ApostropheFilterFactory::new);
        filters.put("arabic_normalization", ArabicNormalizationFilterFactory::new);
        filters.put("arabic_stem", ArabicStemTokenFilterFactory::new);
        filters.put("asciifolding", ASCIIFoldingTokenFilterFactory::new);
        filters.put("bengali_normalization", BengaliNormalizationFilterFactory::new);
        filters.put("brazilian_stem", BrazilianStemTokenFilterFactory::new);
        filters.put("cjk_bigram", CJKBigramFilterFactory::new);
        filters.put("cjk_width", CJKWidthFilterFactory::new);
        filters.put("classic", ClassicFilterFactory::new);
        filters.put("czech_stem", CzechStemTokenFilterFactory::new);
        filters.put("common_grams", requiresAnalysisSettings(CommonGramsTokenFilterFactory::new));
        filters.put("condition",
            requiresAnalysisSettings((i, e, n, s) -> new ScriptedConditionTokenFilterFactory(i, n, s, scriptService.get())));
        filters.put("decimal_digit", DecimalDigitFilterFactory::new);
        filters.put("delimited_payload_filter", LegacyDelimitedPayloadTokenFilterFactory::new);
        filters.put("delimited_payload", DelimitedPayloadTokenFilterFactory::new);
        filters.put("dictionary_decompounder", requiresAnalysisSettings(DictionaryCompoundWordTokenFilterFactory::new));
        filters.put("dutch_stem", DutchStemTokenFilterFactory::new);
        filters.put("edge_ngram", EdgeNGramTokenFilterFactory::new);
        filters.put("edgeNGram", EdgeNGramTokenFilterFactory::new);
        filters.put("elision", ElisionTokenFilterFactory::new);
        filters.put("fingerprint", FingerprintTokenFilterFactory::new);
        filters.put("flatten_graph", FlattenGraphTokenFilterFactory::new);
        filters.put("french_stem", FrenchStemTokenFilterFactory::new);
        filters.put("german_normalization", GermanNormalizationFilterFactory::new);
        filters.put("german_stem", GermanStemTokenFilterFactory::new);
        filters.put("hindi_normalization", HindiNormalizationFilterFactory::new);
        filters.put("hyphenation_decompounder", requiresAnalysisSettings(HyphenationCompoundWordTokenFilterFactory::new));
        filters.put("indic_normalization", IndicNormalizationFilterFactory::new);
        filters.put("keep", requiresAnalysisSettings(KeepWordFilterFactory::new));
        filters.put("keep_types", requiresAnalysisSettings(KeepTypesFilterFactory::new));
        filters.put("keyword_marker", requiresAnalysisSettings(KeywordMarkerTokenFilterFactory::new));
        filters.put("kstem", KStemTokenFilterFactory::new);
        filters.put("length", LengthTokenFilterFactory::new);
        filters.put("limit", LimitTokenCountFilterFactory::new);
        filters.put("lowercase", LowerCaseTokenFilterFactory::new);
        filters.put("min_hash", MinHashTokenFilterFactory::new);
        filters.put("multiplexer", MultiplexerTokenFilterFactory::new);
        filters.put("ngram", NGramTokenFilterFactory::new);
        filters.put("nGram", NGramTokenFilterFactory::new);
        filters.put("pattern_capture", requiresAnalysisSettings(PatternCaptureGroupTokenFilterFactory::new));
        filters.put("pattern_replace", requiresAnalysisSettings(PatternReplaceTokenFilterFactory::new));
        filters.put("persian_normalization", PersianNormalizationFilterFactory::new);
        filters.put("porter_stem", PorterStemTokenFilterFactory::new);
        filters.put("predicate_token_filter",
            requiresAnalysisSettings((i, e, n, s) -> new PredicateTokenFilterScriptFactory(i, n, s, scriptService.get())));
        filters.put("remove_duplicates", RemoveDuplicatesTokenFilterFactory::new);
        filters.put("reverse", ReverseTokenFilterFactory::new);
        filters.put("russian_stem", RussianStemTokenFilterFactory::new);
        filters.put("scandinavian_folding", ScandinavianFoldingFilterFactory::new);
        filters.put("scandinavian_normalization", ScandinavianNormalizationFilterFactory::new);
        filters.put("serbian_normalization", SerbianNormalizationFilterFactory::new);
        filters.put("snowball", SnowballTokenFilterFactory::new);
        filters.put("sorani_normalization", SoraniNormalizationFilterFactory::new);
        filters.put("stemmer_override", requiresAnalysisSettings(StemmerOverrideTokenFilterFactory::new));
        filters.put("stemmer", StemmerTokenFilterFactory::new);
        filters.put("trim", TrimTokenFilterFactory::new);
        filters.put("truncate", requiresAnalysisSettings(TruncateTokenFilterFactory::new));
        filters.put("unique", UniqueTokenFilterFactory::new);
        filters.put("uppercase", UpperCaseTokenFilterFactory::new);
        filters.put("word_delimiter_graph", WordDelimiterGraphTokenFilterFactory::new);
        filters.put("word_delimiter", WordDelimiterTokenFilterFactory::new);
        return filters;
    }

    @Override
    public Map<String, AnalysisProvider<CharFilterFactory>> getCharFilters() {
        Map<String, AnalysisProvider<CharFilterFactory>> filters = new TreeMap<>();
        filters.put("html_strip", HtmlStripCharFilterFactory::new);
        filters.put("pattern_replace", requiresAnalysisSettings(PatternReplaceCharFilterFactory::new));
        filters.put("mapping", requiresAnalysisSettings(MappingCharFilterFactory::new));
        return filters;
    }

    @Override
    public Map<String, AnalysisProvider<TokenizerFactory>> getTokenizers() {
        Map<String, AnalysisProvider<TokenizerFactory>> tokenizers = new TreeMap<>();
        tokenizers.put("simple_pattern", SimplePatternTokenizerFactory::new);
        tokenizers.put("simple_pattern_split", SimplePatternSplitTokenizerFactory::new);
        tokenizers.put("thai", ThaiTokenizerFactory::new);
        tokenizers.put("nGram", NGramTokenizerFactory::new);
        tokenizers.put("ngram", NGramTokenizerFactory::new);
        tokenizers.put("edgeNGram", EdgeNGramTokenizerFactory::new);
        tokenizers.put("edge_ngram", EdgeNGramTokenizerFactory::new);
        tokenizers.put("char_group", CharGroupTokenizerFactory::new);
        tokenizers.put("classic", ClassicTokenizerFactory::new);
        tokenizers.put("letter", LetterTokenizerFactory::new);
        tokenizers.put("lowercase", LowerCaseTokenizerFactory::new);
        tokenizers.put("path_hierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.put("PathHierarchy", PathHierarchyTokenizerFactory::new);
        tokenizers.put("pattern", PatternTokenizerFactory::new);
        tokenizers.put("uax_url_email", UAX29URLEmailTokenizerFactory::new);
        tokenizers.put("whitespace", WhitespaceTokenizerFactory::new);
        tokenizers.put("keyword", KeywordTokenizerFactory::new);
        return tokenizers;
    }

    @Override
    public List<PreBuiltAnalyzerProviderFactory> getPreBuiltAnalyzerProviderFactories() {
        List<PreBuiltAnalyzerProviderFactory> analyzers = new ArrayList<>();
        analyzers.add(new PreBuiltAnalyzerProviderFactory("standard_html_strip", CachingStrategy.ELASTICSEARCH,
            () -> new StandardHtmlStripAnalyzer(CharArraySet.EMPTY_SET)));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("pattern", CachingStrategy.ELASTICSEARCH,
            () -> new PatternAnalyzer(Regex.compile("\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/, null), true,
            CharArraySet.EMPTY_SET)));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("snowball", CachingStrategy.LUCENE,
            () -> new SnowballAnalyzer("English", StopAnalyzer.ENGLISH_STOP_WORDS_SET)));

        // Language analyzers:
        analyzers.add(new PreBuiltAnalyzerProviderFactory("arabic", CachingStrategy.LUCENE, ArabicAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("armenian", CachingStrategy.LUCENE, ArmenianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("basque", CachingStrategy.LUCENE, BasqueAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("bengali", CachingStrategy.LUCENE, BengaliAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("brazilian", CachingStrategy.LUCENE, BrazilianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("bulgarian", CachingStrategy.LUCENE, BulgarianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("catalan", CachingStrategy.LUCENE, CatalanAnalyzer::new));
        // chinese analyzer: only for old indices, best effort
        analyzers.add(new PreBuiltAnalyzerProviderFactory("chinese", CachingStrategy.ONE, StandardAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("cjk", CachingStrategy.LUCENE, CJKAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("czech", CachingStrategy.LUCENE, CzechAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("danish", CachingStrategy.LUCENE, DanishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("dutch", CachingStrategy.LUCENE, DutchAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("english", CachingStrategy.LUCENE, EnglishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("finnish", CachingStrategy.LUCENE, FinnishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("french", CachingStrategy.LUCENE, FrenchAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("galician", CachingStrategy.LUCENE, GalicianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("german", CachingStrategy.LUCENE, GermanAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("greek", CachingStrategy.LUCENE, GreekAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("hindi", CachingStrategy.LUCENE, HindiAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("hungarian", CachingStrategy.LUCENE, HungarianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("indonesian", CachingStrategy.LUCENE, IndonesianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("irish", CachingStrategy.LUCENE, IrishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("italian", CachingStrategy.LUCENE, ItalianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("latvian", CachingStrategy.LUCENE, LatvianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("lithuanian", CachingStrategy.LUCENE, LithuanianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("norwegian", CachingStrategy.LUCENE, NorwegianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("persian", CachingStrategy.LUCENE, PersianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("portuguese", CachingStrategy.LUCENE, PortugueseAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("romanian", CachingStrategy.LUCENE, RomanianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("russian", CachingStrategy.LUCENE, RussianAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("sorani", CachingStrategy.LUCENE, SoraniAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("spanish", CachingStrategy.LUCENE, SpanishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("swedish", CachingStrategy.LUCENE, SwedishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("turkish", CachingStrategy.LUCENE, TurkishAnalyzer::new));
        analyzers.add(new PreBuiltAnalyzerProviderFactory("thai", CachingStrategy.LUCENE, ThaiAnalyzer::new));
        return analyzers;
    }

    @Override
    public List<PreConfiguredCharFilter> getPreConfiguredCharFilters() {
        List<PreConfiguredCharFilter> filters = new ArrayList<>();
        filters.add(PreConfiguredCharFilter.singleton("html_strip", false, HTMLStripCharFilter::new));
        filters.add(PreConfiguredCharFilter.singletonWithVersion("htmlStrip", false, (reader, version) -> {
            if (version.onOrAfter(org.elasticsearch.Version.V_6_3_0)) {
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("htmlStrip_deprecation",
                        "The [htmpStrip] char filter name is deprecated and will be removed in a future version. "
                                + "Please change the filter name to [html_strip] instead.");
            }
            return new HTMLStripCharFilter(reader);
        }));
        return filters;
    }

    @Override
    public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
        List<PreConfiguredTokenFilter> filters = new ArrayList<>();
        filters.add(PreConfiguredTokenFilter.singleton("apostrophe", false, ApostropheFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("arabic_normalization", true, ArabicNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("arabic_stem", false, ArabicStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("asciifolding", true, ASCIIFoldingFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("bengali_normalization", true, BengaliNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("brazilian_stem", false, BrazilianStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("cjk_bigram", false, CJKBigramFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("cjk_width", true, CJKWidthFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("classic", false, ClassicFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("common_grams", false,
                input -> new CommonGramsFilter(input, CharArraySet.EMPTY_SET)));
        filters.add(PreConfiguredTokenFilter.singleton("czech_stem", false, CzechStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("decimal_digit", true, DecimalDigitFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("delimited_payload_filter", false, input ->
                new DelimitedPayloadTokenFilter(input,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_DELIMITER,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_ENCODER)));
        filters.add(PreConfiguredTokenFilter.singleton("delimited_payload", false, input ->
                new DelimitedPayloadTokenFilter(input,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_DELIMITER,
                        DelimitedPayloadTokenFilterFactory.DEFAULT_ENCODER)));
        filters.add(PreConfiguredTokenFilter.singleton("dutch_stem", false, input -> new SnowballFilter(input, new DutchStemmer())));
        filters.add(PreConfiguredTokenFilter.singleton("edge_ngram", false, input ->
                new EdgeNGramTokenFilter(input, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE)));
        filters.add(PreConfiguredTokenFilter.singletonWithVersion("edgeNGram", false, (reader, version) -> {
            if (version.onOrAfter(org.elasticsearch.Version.V_6_4_0)) {
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("edgeNGram_deprecation",
                        "The [edgeNGram] token filter name is deprecated and will be removed in a future version. "
                                + "Please change the filter name to [edge_ngram] instead.");
            }
            return new EdgeNGramTokenFilter(reader, EdgeNGramTokenFilter.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenFilter.DEFAULT_MAX_GRAM_SIZE);
            }));
        filters.add(PreConfiguredTokenFilter.singleton("elision", true,
                input -> new ElisionFilter(input, FrenchAnalyzer.DEFAULT_ARTICLES)));
        filters.add(PreConfiguredTokenFilter.singleton("french_stem", false, input -> new SnowballFilter(input, new FrenchStemmer())));
        filters.add(PreConfiguredTokenFilter.singleton("german_normalization", true, GermanNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("german_stem", false, GermanStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("hindi_normalization", true, HindiNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("indic_normalization", true, IndicNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("keyword_repeat", false, KeywordRepeatFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("kstem", false, KStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("length", false, input ->
                new LengthFilter(input, 0, Integer.MAX_VALUE)));  // TODO this one seems useless
        filters.add(PreConfiguredTokenFilter.singleton("limit", false, input ->
                new LimitTokenCountFilter(input,
                        LimitTokenCountFilterFactory.DEFAULT_MAX_TOKEN_COUNT,
                        LimitTokenCountFilterFactory.DEFAULT_CONSUME_ALL_TOKENS)));
        filters.add(PreConfiguredTokenFilter.singleton("ngram", false, NGramTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singletonWithVersion("nGram", false, (reader, version) -> {
            if (version.onOrAfter(org.elasticsearch.Version.V_6_4_0)) {
                DEPRECATION_LOGGER.deprecatedAndMaybeLog("nGram_deprecation",
                        "The [nGram] token filter name is deprecated and will be removed in a future version. "
                                + "Please change the filter name to [ngram] instead.");
            }
            return new NGramTokenFilter(reader);
        }));
        filters.add(PreConfiguredTokenFilter.singleton("persian_normalization", true, PersianNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("porter_stem", false, PorterStemFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("reverse", false, ReverseStringFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("russian_stem", false, input -> new SnowballFilter(input, "Russian")));
        filters.add(PreConfiguredTokenFilter.singleton("scandinavian_folding", true, ScandinavianFoldingFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("scandinavian_normalization", true, ScandinavianNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("shingle", false, input -> {
            TokenStream ts = new ShingleFilter(input);
            /**
             * We disable the graph analysis on this token stream
             * because it produces shingles of different size.
             * Graph analysis on such token stream is useless and dangerous as it may create too many paths
             * since shingles of different size are not aligned in terms of positions.
             */
            ts.addAttribute(DisableGraphAttribute.class);
            return ts;
        }));
        filters.add(PreConfiguredTokenFilter.singleton("snowball", false, input -> new SnowballFilter(input, "English")));
        filters.add(PreConfiguredTokenFilter.singleton("sorani_normalization", true, SoraniNormalizationFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("stemmer", false, PorterStemFilter::new));
        // The stop filter is in lucene-core but the English stop words set is in lucene-analyzers-common
        filters.add(PreConfiguredTokenFilter.singleton("stop", false, input -> new StopFilter(input, StopAnalyzer.ENGLISH_STOP_WORDS_SET)));
        filters.add(PreConfiguredTokenFilter.singleton("trim", true, TrimFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("truncate", false, input -> new TruncateTokenFilter(input, 10)));
        filters.add(PreConfiguredTokenFilter.singleton("type_as_payload", false, TypeAsPayloadTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("unique", false, UniqueTokenFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("uppercase", true, UpperCaseFilter::new));
        filters.add(PreConfiguredTokenFilter.singleton("word_delimiter", false, input ->
                new WordDelimiterFilter(input,
                        WordDelimiterFilter.GENERATE_WORD_PARTS
                      | WordDelimiterFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE, null)));
        filters.add(PreConfiguredTokenFilter.singleton("word_delimiter_graph", false, input ->
                new WordDelimiterGraphFilter(input,
                        WordDelimiterGraphFilter.GENERATE_WORD_PARTS
                      | WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS
                      | WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE
                      | WordDelimiterGraphFilter.SPLIT_ON_NUMERICS
                      | WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE, null)));
        return filters;
    }

    @Override
    public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        List<PreConfiguredTokenizer> tokenizers = new ArrayList<>();
        tokenizers.add(PreConfiguredTokenizer.singleton("keyword", KeywordTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("classic", ClassicTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("uax_url_email", UAX29URLEmailTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("path_hierarchy", PathHierarchyTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("letter", LetterTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("whitespace", WhitespaceTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("ngram", NGramTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("edge_ngram",
            () -> new EdgeNGramTokenizer(EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE), null));
        tokenizers.add(PreConfiguredTokenizer.singleton("pattern", () -> new PatternTokenizer(Regex.compile("\\W+", null), -1), null));
        tokenizers.add(PreConfiguredTokenizer.singleton("thai", ThaiTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("lowercase", LowerCaseTokenizer::new, () -> new TokenFilterFactory() {
            @Override
            public String name() {
                return "lowercase";
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return new LowerCaseFilter(tokenStream);
            }
        }));

        // Temporary shim for aliases. TODO deprecate after they are moved
        tokenizers.add(PreConfiguredTokenizer.singleton("nGram", NGramTokenizer::new, null));
        tokenizers.add(PreConfiguredTokenizer.singleton("edgeNGram",
            () -> new EdgeNGramTokenizer(EdgeNGramTokenizer.DEFAULT_MIN_GRAM_SIZE, EdgeNGramTokenizer.DEFAULT_MAX_GRAM_SIZE), null));
        tokenizers.add(PreConfiguredTokenizer.singleton("PathHierarchy", PathHierarchyTokenizer::new, null));

        return tokenizers;
    }
}
