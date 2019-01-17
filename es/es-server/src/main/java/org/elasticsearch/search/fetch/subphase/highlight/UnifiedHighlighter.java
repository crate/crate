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
package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.uhighlight.BoundedBreakIteratorScanner;
import org.apache.lucene.search.uhighlight.CustomPassageFormatter;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.apache.lucene.search.uhighlight.PassageFormatter;
import org.apache.lucene.search.uhighlight.Snippet;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;

public class UnifiedHighlighter implements Highlighter {
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(Loggers.getLogger(UnifiedHighlighter.class));

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }
    
    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {
        MappedFieldType fieldType = highlighterContext.fieldType;
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;
        Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;
        List<Snippet> snippets = new ArrayList<>();
        int numberOfFragments;
        try {

            final Analyzer analyzer = getAnalyzer(context.mapperService().documentMapper(hitContext.hit().getType()), fieldType);
            List<Object> fieldValues = loadFieldValues(fieldType, field, context, hitContext);
            if (fieldValues.size() == 0) {
                return null;
            }
            final PassageFormatter passageFormatter = getPassageFormatter(field, encoder);
            final IndexSearcher searcher = new IndexSearcher(hitContext.reader());
            final CustomUnifiedHighlighter highlighter;
            final String fieldValue = mergeFieldValues(fieldValues, MULTIVAL_SEP_CHAR);
            final OffsetSource offsetSource = getOffsetSource(fieldType);

            final int maxAnalyzedOffset = context.indexShard().indexSettings().getHighlightMaxAnalyzedOffset();
            // Issue a deprecation warning if maxAnalyzedOffset is not set, and field length > default setting for 7.0
            final int maxAnalyzedOffset7 = 1000000;
            if ((offsetSource == OffsetSource.ANALYSIS) &&  (maxAnalyzedOffset == -1) && (fieldValue.length() > maxAnalyzedOffset7)) {
                deprecationLogger.deprecated(
                    "The length [" + fieldValue.length() + "] of [" + highlighterContext.fieldName + "] field of [" +
                        hitContext.hit().getId() + "] doc of [" + context.indexShard().shardId().getIndexName() + "] index has " +
                        "exceeded the allowed maximum of [" + maxAnalyzedOffset7 + "] set for the next major Elastic version. " +
                        "This maximum can be set by changing the [" + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() +
                        "] index level setting. " + "For large texts, indexing with offsets or term vectors is recommended!");
            }
            // Throw an error if maxAnalyzedOffset is explicitly set by the user, and field length > maxAnalyzedOffset
            if ((offsetSource == OffsetSource.ANALYSIS) &&  (maxAnalyzedOffset > 0) && (fieldValue.length() > maxAnalyzedOffset)) {
                throw new IllegalArgumentException(
                    "The length [" + fieldValue.length() + "] of [" + highlighterContext.fieldName + "] field of [" +
                        hitContext.hit().getId() + "] doc of [" + context.indexShard().shardId().getIndexName() + "] index " +
                        "has exceeded [" + maxAnalyzedOffset + "] - maximum allowed to be analyzed for highlighting. " +
                        "This maximum can be set by changing the [" + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() +
                        "] index level setting. " + "For large texts, indexing with offsets or term vectors is recommended!");
            }

            if (field.fieldOptions().numberOfFragments() == 0) {
                // we use a control char to separate values, which is the only char that the custom break iterator
                // breaks the text on, so we don't lose the distinction between the different values of a field and we
                // get back a snippet per value
                CustomSeparatorBreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
                highlighter = new CustomUnifiedHighlighter(searcher, analyzer, offsetSource, passageFormatter,
                    field.fieldOptions().boundaryScannerLocale(), breakIterator, fieldValue, field.fieldOptions().noMatchSize());
                numberOfFragments = fieldValues.size(); // we are highlighting the whole content, one snippet per value
            } else {
                //using paragraph separator we make sure that each field value holds a discrete passage for highlighting
                BreakIterator bi = getBreakIterator(field);
                highlighter = new CustomUnifiedHighlighter(searcher, analyzer, offsetSource, passageFormatter,
                    field.fieldOptions().boundaryScannerLocale(), bi, fieldValue, field.fieldOptions().noMatchSize());
                numberOfFragments = field.fieldOptions().numberOfFragments();
            }

            if (field.fieldOptions().requireFieldMatch()) {
                final String fieldName = highlighterContext.fieldName;
                highlighter.setFieldMatcher((name) -> fieldName.equals(name));
            } else {
                highlighter.setFieldMatcher((name) -> true);
            }

            Snippet[] fieldSnippets = highlighter.highlightField(highlighterContext.fieldName,
                highlighterContext.query, hitContext.docId(), numberOfFragments);
            for (Snippet fieldSnippet : fieldSnippets) {
                if (Strings.hasText(fieldSnippet.getText())) {
                    snippets.add(fieldSnippet);
                }
            }
        } catch (IOException e) {
            throw new FetchPhaseExecutionException(context,
                "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }

        snippets = filterSnippets(snippets, field.fieldOptions().numberOfFragments());

        if (field.fieldOptions().scoreOrdered()) {
            //let's sort the snippets by score if needed
            CollectionUtil.introSort(snippets, (o1, o2) -> Double.compare(o2.getScore(), o1.getScore()));
        }

        String[] fragments = new String[snippets.size()];
        for (int i = 0; i < fragments.length; i++) {
            fragments[i] = snippets.get(i).getText();
        }

        if (fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, Text.convertFromStringArray(fragments));
        }
        return null;
    }

    protected PassageFormatter getPassageFormatter(SearchContextHighlight.Field field, Encoder encoder) {
        CustomPassageFormatter passageFormatter = new CustomPassageFormatter(field.fieldOptions().preTags()[0],
            field.fieldOptions().postTags()[0], encoder);
        return passageFormatter;
    }

    
    protected Analyzer getAnalyzer(DocumentMapper docMapper, MappedFieldType type) {
        return HighlightUtils.getAnalyzer(docMapper, type);
    }
    
    protected List<Object> loadFieldValues(MappedFieldType fieldType, SearchContextHighlight.Field field, SearchContext context,
            FetchSubPhase.HitContext hitContext) throws IOException {
        List<Object> fieldValues = HighlightUtils.loadFieldValues(field, fieldType, context, hitContext);
        fieldValues = fieldValues.stream()
            .map((s) -> convertFieldValue(fieldType, s))
            .collect(Collectors.toList());
        return fieldValues;
    }

    protected BreakIterator getBreakIterator(SearchContextHighlight.Field field) {
        final SearchContextHighlight.FieldOptions fieldOptions = field.fieldOptions();
        final Locale locale =
            fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() :
                Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type =
            fieldOptions.boundaryScannerType()  != null ? fieldOptions.boundaryScannerType() :
                HighlightBuilder.BoundaryScannerType.SENTENCE;
        int maxLen = fieldOptions.fragmentCharSize();
        switch (type) {
            case SENTENCE:
                if (maxLen > 0) {
                    return BoundedBreakIteratorScanner.getSentence(locale, maxLen);
                }
                return BreakIterator.getSentenceInstance(locale);
            case WORD:
                // ignore maxLen
                return BreakIterator.getWordInstance(locale);
            default:
                throw new IllegalArgumentException("Invalid boundary scanner type: " + type.toString());
        }
    }

    protected static List<Snippet> filterSnippets(List<Snippet> snippets, int numberOfFragments) {

        //We need to filter the snippets as due to no_match_size we could have
        //either highlighted snippets or non highlighted ones and we don't want to mix those up
        List<Snippet> filteredSnippets = new ArrayList<>(snippets.size());
        for (Snippet snippet : snippets) {
            if (snippet.isHighlighted()) {
                filteredSnippets.add(snippet);
            }
        }

        //if there's at least one highlighted snippet, we return all the highlighted ones
        //otherwise we return the first non highlighted one if available
        if (filteredSnippets.size() == 0) {
            if (snippets.size() > 0) {
                Snippet snippet = snippets.get(0);
                //if we tried highlighting the whole content using whole break iterator (as number_of_fragments was 0)
                //we need to return the first sentence of the content rather than the whole content
                if (numberOfFragments == 0) {
                    BreakIterator bi = BreakIterator.getSentenceInstance(Locale.ROOT);
                    String text = snippet.getText();
                    bi.setText(text);
                    int next = bi.next();
                    if (next != BreakIterator.DONE) {
                        String newText = text.substring(0, next).trim();
                        snippet = new Snippet(newText, snippet.getScore(), snippet.isHighlighted());
                    }
                }
                filteredSnippets.add(snippet);
            }
        }

        return filteredSnippets;
    }

    protected static String convertFieldValue(MappedFieldType type, Object value) {
        if (value instanceof BytesRef) {
            return type.valueForDisplay(value).toString();
        } else {
            return value.toString();
        }
    }

    protected static String mergeFieldValues(List<Object> fieldValues, char valuesSeparator) {
        //postings highlighter accepts all values in a single string, as offsets etc. need to match with content
        //loaded from stored fields, we merge all values using a proper separator
        String rawValue = Strings.collectionToDelimitedString(fieldValues, String.valueOf(valuesSeparator));
        return rawValue.substring(0, Math.min(rawValue.length(), Integer.MAX_VALUE - 1));
    }

    protected OffsetSource getOffsetSource(MappedFieldType fieldType) {
        if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
            return fieldType.storeTermVectors() ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
        }
        if (fieldType.storeTermVectorOffsets()) {
            return OffsetSource.TERM_VECTORS;
        }
        return OffsetSource.ANALYSIS;
    }
}
