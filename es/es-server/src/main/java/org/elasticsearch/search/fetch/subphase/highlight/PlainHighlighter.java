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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.NullFragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.fetch.subphase.highlight.UnifiedHighlighter.convertFieldValue;

public class PlainHighlighter implements Highlighter {
    private static final String CACHE_KEY = "highlight-plain";
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(Loggers.getLogger(PlainHighlighter.class));

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;
        MappedFieldType fieldType = highlighterContext.fieldType;

        Encoder encoder = field.fieldOptions().encoder().equals("html") ? HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HashMap<>());
        }
        @SuppressWarnings("unchecked")
        Map<MappedFieldType, org.apache.lucene.search.highlight.Highlighter> cache =
            (Map<MappedFieldType, org.apache.lucene.search.highlight.Highlighter>) hitContext.cache().get(CACHE_KEY);

        org.apache.lucene.search.highlight.Highlighter entry = cache.get(fieldType);
        if (entry == null) {
            QueryScorer queryScorer = new CustomQueryScorer(highlighterContext.query,
                    field.fieldOptions().requireFieldMatch() ? fieldType.name() : null);
            queryScorer.setExpandMultiTermQuery(true);
            Fragmenter fragmenter;
            if (field.fieldOptions().numberOfFragments() == 0) {
                fragmenter = new NullFragmenter();
            } else if (field.fieldOptions().fragmenter() == null) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else if ("simple".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleFragmenter(field.fieldOptions().fragmentCharSize());
            } else if ("span".equals(field.fieldOptions().fragmenter())) {
                fragmenter = new SimpleSpanFragmenter(queryScorer, field.fieldOptions().fragmentCharSize());
            } else {
                throw new IllegalArgumentException("unknown fragmenter option [" + field.fieldOptions().fragmenter()
                        + "] for the field [" + highlighterContext.fieldName + "]");
            }
            Formatter formatter = new SimpleHTMLFormatter(field.fieldOptions().preTags()[0], field.fieldOptions().postTags()[0]);

            entry = new org.apache.lucene.search.highlight.Highlighter(formatter, encoder, queryScorer);
            entry.setTextFragmenter(fragmenter);
            // always highlight across all data
            entry.setMaxDocCharsToAnalyze(Integer.MAX_VALUE);

            cache.put(fieldType, entry);
        }

        // a HACK to make highlighter do highlighting, even though its using the single frag list builder
        int numberOfFragments = field.fieldOptions().numberOfFragments() == 0 ? 1 : field.fieldOptions().numberOfFragments();
        ArrayList<TextFragment> fragsList = new ArrayList<>();
        List<Object> textsToHighlight;
        Analyzer analyzer = HighlightUtils.getAnalyzer(context.mapperService().documentMapper(hitContext.hit().getType()), fieldType);
        final int maxAnalyzedOffset = context.indexShard().indexSettings().getHighlightMaxAnalyzedOffset();

        try {
            textsToHighlight = HighlightUtils.loadFieldValues(field, fieldType, context, hitContext);
            final int maxAnalyzedOffset7 = 1000000;
            for (Object textToHighlight : textsToHighlight) {
                String text = convertFieldValue(fieldType, textToHighlight);

                // Issue deprecation warning if maxAnalyzedOffset is not set, and text length > default setting for 7.0
                if ((maxAnalyzedOffset == -1) && (text.length() > maxAnalyzedOffset7)) {
                    deprecationLogger.deprecated(
                        "The length [" + text.length()+ "] of [" + highlighterContext.fieldName + "] field of [" +
                            hitContext.hit().getId() + "] doc of [" + context.indexShard().shardId().getIndexName() + "] index has " +
                            "exceeded the allowed maximum of ["+ maxAnalyzedOffset7 + "] set for the next major Elastic version. " +
                            "This maximum can be set by changing the [" + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() +
                            "] index level setting. For large texts, indexing with offsets or term vectors is recommended!");
                }
                // Throw an error if maxAnalyzedOffset is explicitly set by the user, and text length > maxAnalyzedOffset
                if ((maxAnalyzedOffset > 0) && (text.length() > maxAnalyzedOffset)) {
                    // maxAnalyzedOffset is not set by user
                    throw new IllegalArgumentException(
                        "The length [" + text.length()+ "] of [" + highlighterContext.fieldName + "] field of [" +
                            hitContext.hit().getId() + "] doc of [" + context.indexShard().shardId().getIndexName() + "] index " +
                            "has exceeded [" + maxAnalyzedOffset + "] - maximum allowed to be analyzed for highlighting. " +
                            "This maximum can be set by changing the [" + IndexSettings.MAX_ANALYZED_OFFSET_SETTING.getKey() +
                            "] index level setting. " + "For large texts, indexing with offsets or term vectors, and highlighting " +
                            "with unified or fvh highlighter is recommended!");
                }

                try (TokenStream tokenStream = analyzer.tokenStream(fieldType.name(), text)) {
                    if (!tokenStream.hasAttribute(CharTermAttribute.class) || !tokenStream.hasAttribute(OffsetAttribute.class)) {
                        // can't perform highlighting if the stream has no terms (binary token stream) or no offsets
                        continue;
                    }
                    TextFragment[] bestTextFragments = entry.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                    for (TextFragment bestTextFragment : bestTextFragments) {
                        if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                            fragsList.add(bestTextFragment);
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (ExceptionsHelper.unwrap(e, BytesRefHash.MaxBytesLengthExceededException.class) != null) {
                // this can happen if for example a field is not_analyzed and ignore_above option is set.
                // the field will be ignored when indexing but the huge term is still in the source and
                // the plain highlighter will parse the source and try to analyze it.
                return null;
            } else {
                throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
            }
        }
        if (field.fieldOptions().scoreOrdered()) {
            CollectionUtil.introSort(fragsList, new Comparator<TextFragment>() {
                @Override
                public int compare(TextFragment o1, TextFragment o2) {
                    return Math.round(o2.getScore() - o1.getScore());
                }
            });
        }
        String[] fragments;
        // number_of_fragments is set to 0 but we have a multivalued field
        if (field.fieldOptions().numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
            fragments = new String[fragsList.size()];
            for (int i = 0; i < fragsList.size(); i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        } else {
            // refine numberOfFragments if needed
            numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size() : numberOfFragments;
            fragments = new String[numberOfFragments];
            for (int i = 0; i < fragments.length; i++) {
                fragments[i] = fragsList.get(i).toString();
            }
        }

        if (fragments.length > 0) {
            return new HighlightField(highlighterContext.fieldName, Text.convertFromStringArray(fragments));
        }

        int noMatchSize = highlighterContext.field.fieldOptions().noMatchSize();
        if (noMatchSize > 0 && textsToHighlight.size() > 0) {
            // Pull an excerpt from the beginning of the string but make sure to split the string on a term boundary.
            String fieldContents = textsToHighlight.get(0).toString();
            int end;
            try {
                end = findGoodEndForNoHighlightExcerpt(noMatchSize, analyzer, fieldType.name(), fieldContents);
            } catch (Exception e) {
                throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
            }
            if (end > 0) {
                return new HighlightField(highlighterContext.fieldName, new Text[] { new Text(fieldContents.substring(0, end)) });
            }
        }
        return null;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    private static int findGoodEndForNoHighlightExcerpt(int noMatchSize, Analyzer analyzer, String fieldName, String contents)
            throws IOException {
        try (TokenStream tokenStream = analyzer.tokenStream(fieldName, contents)) {
            if (!tokenStream.hasAttribute(OffsetAttribute.class)) {
                // Can't split on term boundaries without offsets
                return -1;
            }
            int end = -1;
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                OffsetAttribute attr = tokenStream.getAttribute(OffsetAttribute.class);
                if (attr.endOffset() >= noMatchSize) {
                    // Jump to the end of this token if it wouldn't put us past the boundary
                    if (attr.endOffset() == noMatchSize) {
                        end = noMatchSize;
                    }
                    return end;
                }
                end = attr.endOffset();
            }
            tokenStream.end();
            // We've exhausted the token stream so we should just highlight everything.
            return end;
        }
    }
}
