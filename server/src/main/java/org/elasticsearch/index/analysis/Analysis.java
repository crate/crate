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

package org.elasticsearch.index.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

public class Analysis {

    public static Version parseAnalysisVersion(Settings indexSettings, Settings settings, Logger logger) {
        // check for explicit version on the specific analyzer component
        String sVersion = settings.get("version");
        if (sVersion != null) {
            return Lucene.parseVersion(sVersion, Version.LATEST, logger);
        }
        // check for explicit version on the index itself as default for all analysis components
        sVersion = indexSettings.get("index.analysis.version");
        if (sVersion != null) {
            return Lucene.parseVersion(sVersion, Version.LATEST, logger);
        }
        // resolve the analysis version based on the version the index was created with
        return org.elasticsearch.Version.indexCreated(indexSettings).luceneVersion;
    }

    public static CharArraySet parseStemExclusion(Settings settings, CharArraySet defaultStemExclusion) {
        String value = settings.get("stem_exclusion");
        if ("_none_".equals(value)) {
            return CharArraySet.EMPTY_SET;
        }
        List<String> stemExclusion = settings.getAsList("stem_exclusion", null);
        if (stemExclusion != null) {
            // LUCENE 4 UPGRADE: Should be settings.getAsBoolean("stem_exclusion_case", false)?
            return new CharArraySet(stemExclusion, false);
        } else {
            return defaultStemExclusion;
        }
    }

    public static final Map<String, Set<?>> NAMED_STOP_WORDS = Map.ofEntries(
        Map.entry("_arabic_", ArabicAnalyzer.getDefaultStopSet()),
        Map.entry("_armenian_", ArmenianAnalyzer.getDefaultStopSet()),
        Map.entry("_basque_", BasqueAnalyzer.getDefaultStopSet()),
        Map.entry("_bengali_", BengaliAnalyzer.getDefaultStopSet()),
        Map.entry("_brazilian_", BrazilianAnalyzer.getDefaultStopSet()),
        Map.entry("_bulgarian_", BulgarianAnalyzer.getDefaultStopSet()),
        Map.entry("_catalan_", CatalanAnalyzer.getDefaultStopSet()),
        Map.entry("_czech_", CzechAnalyzer.getDefaultStopSet()),
        Map.entry("_danish_", DanishAnalyzer.getDefaultStopSet()),
        Map.entry("_dutch_", DutchAnalyzer.getDefaultStopSet()),
        Map.entry("_english_", EnglishAnalyzer.getDefaultStopSet()),
        Map.entry("_finnish_", FinnishAnalyzer.getDefaultStopSet()),
        Map.entry("_french_", FrenchAnalyzer.getDefaultStopSet()),
        Map.entry("_galician_", GalicianAnalyzer.getDefaultStopSet()),
        Map.entry("_german_", GermanAnalyzer.getDefaultStopSet()),
        Map.entry("_greek_", GreekAnalyzer.getDefaultStopSet()),
        Map.entry("_hindi_", HindiAnalyzer.getDefaultStopSet()),
        Map.entry("_hungarian_", HungarianAnalyzer.getDefaultStopSet()),
        Map.entry("_indonesian_", IndonesianAnalyzer.getDefaultStopSet()),
        Map.entry("_irish_", IrishAnalyzer.getDefaultStopSet()),
        Map.entry("_italian_", ItalianAnalyzer.getDefaultStopSet()),
        Map.entry("_latvian_", LatvianAnalyzer.getDefaultStopSet()),
        Map.entry("_lithuanian_", LithuanianAnalyzer.getDefaultStopSet()),
        Map.entry("_norwegian_", NorwegianAnalyzer.getDefaultStopSet()),
        Map.entry("_persian_", PersianAnalyzer.getDefaultStopSet()),
        Map.entry("_portuguese_", PortugueseAnalyzer.getDefaultStopSet()),
        Map.entry("_romanian_", RomanianAnalyzer.getDefaultStopSet()),
        Map.entry("_russian_", RussianAnalyzer.getDefaultStopSet()),
        Map.entry("_sorani_", SoraniAnalyzer.getDefaultStopSet()),
        Map.entry("_spanish_", SpanishAnalyzer.getDefaultStopSet()),
        Map.entry("_swedish_", SwedishAnalyzer.getDefaultStopSet()),
        Map.entry("_thai_", ThaiAnalyzer.getDefaultStopSet()),
        Map.entry("_turkish_", TurkishAnalyzer.getDefaultStopSet())
    );

    public static CharArraySet parseWords(Environment env, Settings settings, String name, CharArraySet defaultWords,
                                          Map<String, Set<?>> namedWords, boolean ignoreCase) {
        String value = settings.get(name);
        if (value != null) {
            if ("_none_".equals(value)) {
                return CharArraySet.EMPTY_SET;
            } else {
                return resolveNamedWords(settings.getAsList(name), namedWords, ignoreCase);
            }
        }
        List<String> pathLoadedWords = getWordList(env, settings, name);
        if (pathLoadedWords != null) {
            return resolveNamedWords(pathLoadedWords, namedWords, ignoreCase);
        }
        return defaultWords;
    }

    public static CharArraySet parseCommonWords(Environment env, Settings settings, CharArraySet defaultCommonWords, boolean ignoreCase) {
        return parseWords(env, settings, "common_words", defaultCommonWords, NAMED_STOP_WORDS, ignoreCase);
    }

    public static CharArraySet parseArticles(Environment env, Settings settings) {
        boolean articlesCase = settings.getAsBoolean("articles_case", false);
        return parseWords(env, settings, "articles", null, null, articlesCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords) {
        boolean stopwordsCase = settings.getAsBoolean("stopwords_case", false);
        return parseStopWords(env, settings, defaultStopWords, stopwordsCase);
    }

    public static CharArraySet parseStopWords(Environment env, Settings settings, CharArraySet defaultStopWords, boolean ignoreCase) {
        return parseWords(env, settings, "stopwords", defaultStopWords, NAMED_STOP_WORDS, ignoreCase);
    }

    private static CharArraySet resolveNamedWords(Collection<String> words, Map<String, Set<?>> namedWords, boolean ignoreCase) {
        if (namedWords == null) {
            return new CharArraySet(words, ignoreCase);
        }
        CharArraySet setWords = new CharArraySet(words.size(), ignoreCase);
        for (String word : words) {
            if (namedWords.containsKey(word)) {
                setWords.addAll(namedWords.get(word));
            } else {
                setWords.add(word);
            }
        }
        return setWords;
    }

    public static CharArraySet getWordSet(Environment env, Settings settings, String settingsPrefix) {
        List<String> wordList = getWordList(env, settings, settingsPrefix);
        if (wordList == null) {
            return null;
        }
        boolean ignoreCase =
            settings.getAsBoolean(settingsPrefix + "_case", false);
        return new CharArraySet(wordList, ignoreCase);
    }

    /**
     * Fetches a list of words from the specified settings file. The list should either be available at the key
     * specified by settingsPrefix or in a file specified by settingsPrefix + _path.
     *
     * @throws IllegalArgumentException
     *          If the word list cannot be found at either key.
     */
    public static List<String> getWordList(Environment env, Settings settings, String settingPrefix) {
        String wordListPath = settings.get(settingPrefix + "_path", null);

        if (wordListPath == null) {
            List<String> explicitWordList = settings.getAsList(settingPrefix, null);
            if (explicitWordList == null) {
                return null;
            } else {
                return explicitWordList;
            }
        }

        final Path path = env.configFile().resolve(wordListPath);

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            return loadWordList(reader, "#");
        } catch (CharacterCodingException ex) {
            String message = String.format(Locale.ROOT,
                "Unsupported character encoding detected while reading %s_path: %s - files must be UTF-8 encoded",
                settingPrefix, path.toString());
            throw new IllegalArgumentException(message, ex);
        } catch (IOException ioe) {
            String message = String.format(Locale.ROOT, "IOException while reading %s_path: %s", settingPrefix, path.toString());
            throw new IllegalArgumentException(message, ioe);
        }
    }

    public static List<String> loadWordList(Reader reader, String comment) throws IOException {
        final List<String> result = new ArrayList<>();
        BufferedReader br = null;
        try {
            if (reader instanceof BufferedReader) {
                br = (BufferedReader) reader;
            } else {
                br = new BufferedReader(reader);
            }
            String word;
            while ((word = br.readLine()) != null) {
                if (!Strings.hasText(word)) {
                    continue;
                }
                if (!word.startsWith(comment)) {
                    result.add(word.trim());
                }
            }
        } finally {
            if (br != null)
                br.close();
        }
        return result;
    }

    /**
     * @return null If no settings set for "settingsPrefix" then return <code>null</code>.
     * @throws IllegalArgumentException
     *          If the Reader can not be instantiated.
     */
    public static Reader getReaderFromFile(Environment env, Settings settings, String settingPrefix) {
        String filePath = settings.get(settingPrefix, null);

        if (filePath == null) {
            return null;
        }
        final Path path = env.configFile().resolve(filePath);
        try {
            return Files.newBufferedReader(path, StandardCharsets.UTF_8);
        } catch (CharacterCodingException ex) {
            String message = String.format(Locale.ROOT,
                "Unsupported character encoding detected while reading %s_path: %s files must be UTF-8 encoded",
                settingPrefix, path.toString());
            throw new IllegalArgumentException(message, ex);
        } catch (IOException ioe) {
            String message = String.format(Locale.ROOT, "IOException while reading %s_path: %s", settingPrefix, path.toString());
            throw new IllegalArgumentException(message, ioe);
        }
    }

}
