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

import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.BreakIteratorBoundaryScanner;
import org.apache.lucene.search.vectorhighlight.CustomFieldQuery;
import org.apache.lucene.search.vectorhighlight.FieldFragList;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList.WeightedPhraseInfo;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.apache.lucene.search.vectorhighlight.FragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.SimpleBoundaryScanner;
import org.apache.lucene.search.vectorhighlight.SimpleFieldFragList;
import org.apache.lucene.search.vectorhighlight.SimpleFragListBuilder;
import org.apache.lucene.search.vectorhighlight.SingleFragListBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight.Field;
import org.elasticsearch.search.fetch.subphase.highlight.SearchContextHighlight.FieldOptions;
import org.elasticsearch.search.internal.SearchContext;

import java.text.BreakIterator;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class FastVectorHighlighter implements Highlighter {
    private static final BoundaryScanner DEFAULT_SIMPLE_BOUNDARY_SCANNER = new SimpleBoundaryScanner();
    private static final BoundaryScanner DEFAULT_SENTENCE_BOUNDARY_SCANNER =
        new BreakIteratorBoundaryScanner(BreakIterator.getSentenceInstance(Locale.ROOT));
    private static final BoundaryScanner DEFAULT_WORD_BOUNDARY_SCANNER =
        new BreakIteratorBoundaryScanner(BreakIterator.getWordInstance(Locale.ROOT));

    public static final Setting<Boolean> SETTING_TV_HIGHLIGHT_MULTI_VALUE =
        Setting.boolSetting("search.highlight.term_vector_multi_value", true, Setting.Property.NodeScope);

    private static final String CACHE_KEY = "highlight-fsv";
    private final Boolean termVectorMultiValue;

    public FastVectorHighlighter(Settings settings) {
        this.termVectorMultiValue = SETTING_TV_HIGHLIGHT_MULTI_VALUE.get(settings);
    }

    @Override
    public HighlightField highlight(HighlighterContext highlighterContext) {
        SearchContextHighlight.Field field = highlighterContext.field;
        SearchContext context = highlighterContext.context;
        FetchSubPhase.HitContext hitContext = highlighterContext.hitContext;
        MappedFieldType fieldType = highlighterContext.fieldType;

        if (canHighlight(fieldType) == false) {
            throw new IllegalArgumentException("the field [" + highlighterContext.fieldName +
                "] should be indexed with term vector with position offsets to be used with fast vector highlighter");
        }

        Encoder encoder = field.fieldOptions().encoder().equals("html") ?
            HighlightUtils.Encoders.HTML : HighlightUtils.Encoders.DEFAULT;

        if (!hitContext.cache().containsKey(CACHE_KEY)) {
            hitContext.cache().put(CACHE_KEY, new HighlighterEntry());
        }
        HighlighterEntry cache = (HighlighterEntry) hitContext.cache().get(CACHE_KEY);

        try {
            FieldHighlightEntry entry = cache.fields.get(fieldType);
            if (entry == null) {
                FragListBuilder fragListBuilder;
                BaseFragmentsBuilder fragmentsBuilder;

                final BoundaryScanner boundaryScanner = getBoundaryScanner(field);
                boolean forceSource = context.highlight().forceSource(field);
                if (field.fieldOptions().numberOfFragments() == 0) {
                    fragListBuilder = new SingleFragListBuilder();

                    if (!forceSource && fieldType.stored()) {
                        fragmentsBuilder = new SimpleFragmentsBuilder(fieldType, field.fieldOptions().preTags(),
                                field.fieldOptions().postTags(), boundaryScanner);
                    } else {
                        fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldType, context,
                                field.fieldOptions().preTags(), field.fieldOptions().postTags(), boundaryScanner);
                    }
                } else {
                    fragListBuilder = field.fieldOptions().fragmentOffset() == -1 ?
                        new SimpleFragListBuilder() : new SimpleFragListBuilder(field.fieldOptions().fragmentOffset());
                    if (field.fieldOptions().scoreOrdered()) {
                        if (!forceSource && fieldType.stored()) {
                            fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.fieldOptions().preTags(),
                                    field.fieldOptions().postTags(), boundaryScanner);
                        } else {
                            fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(fieldType, context,
                                    field.fieldOptions().preTags(), field.fieldOptions().postTags(), boundaryScanner);
                        }
                    } else {
                        if (!forceSource && fieldType.stored()) {
                            fragmentsBuilder = new SimpleFragmentsBuilder(fieldType, field.fieldOptions().preTags(),
                                    field.fieldOptions().postTags(), boundaryScanner);
                        } else {
                            fragmentsBuilder =
                                new SourceSimpleFragmentsBuilder(fieldType, context, field.fieldOptions().preTags(),
                                    field.fieldOptions().postTags(), boundaryScanner);
                        }
                    }
                }
                fragmentsBuilder.setDiscreteMultiValueHighlighting(termVectorMultiValue);
                entry = new FieldHighlightEntry();
                if (field.fieldOptions().requireFieldMatch()) {
                    /**
                     * we use top level reader to rewrite the query against all readers,
                     * with use caching it across hits (and across readers...)
                     */
                    entry.fieldMatchFieldQuery = new CustomFieldQuery(highlighterContext.query,
                        hitContext.topLevelReader(), true, field.fieldOptions().requireFieldMatch());
                } else {
                    /**
                     * we use top level reader to rewrite the query against all readers,
                     * with use caching it across hits (and across readers...)
                     */
                    entry.noFieldMatchFieldQuery = new CustomFieldQuery(highlighterContext.query,
                        hitContext.topLevelReader(), true, field.fieldOptions().requireFieldMatch());
                }
                entry.fragListBuilder = fragListBuilder;
                entry.fragmentsBuilder = fragmentsBuilder;
                if (cache.fvh == null) {
                    // parameters to FVH are not requires since:
                    // first two booleans are not relevant since they are set on the CustomFieldQuery
                    // (phrase and fieldMatch) fragment builders are used explicitly
                    cache.fvh = new org.apache.lucene.search.vectorhighlight.FastVectorHighlighter();
                }
                CustomFieldQuery.highlightFilters.set(field.fieldOptions().highlightFilter());
                cache.fields.put(fieldType, entry);
            }
            final FieldQuery fieldQuery;
            if (field.fieldOptions().requireFieldMatch()) {
                fieldQuery = entry.fieldMatchFieldQuery;
            } else {
                fieldQuery = entry.noFieldMatchFieldQuery;
            }
            cache.fvh.setPhraseLimit(field.fieldOptions().phraseLimit());

            String[] fragments;

            // a HACK to make highlighter do highlighting, even though its using the single frag list builder
            int numberOfFragments = field.fieldOptions().numberOfFragments() == 0 ?
                    Integer.MAX_VALUE : field.fieldOptions().numberOfFragments();
            int fragmentCharSize = field.fieldOptions().numberOfFragments() == 0 ?
                    Integer.MAX_VALUE : field.fieldOptions().fragmentCharSize();
            // we highlight against the low level reader and docId, because if we load source, we want to reuse it if possible
            // Only send matched fields if they were requested to save time.
            if (field.fieldOptions().matchedFields() != null && !field.fieldOptions().matchedFields().isEmpty()) {
                fragments = cache.fvh.getBestFragments(fieldQuery, hitContext.reader(), hitContext.docId(),
                    fieldType.name(), field.fieldOptions().matchedFields(), fragmentCharSize,
                    numberOfFragments, entry.fragListBuilder, entry.fragmentsBuilder, field.fieldOptions().preTags(),
                    field.fieldOptions().postTags(), encoder);
            } else {
                fragments = cache.fvh.getBestFragments(fieldQuery, hitContext.reader(), hitContext.docId(),
                    fieldType.name(), fragmentCharSize, numberOfFragments, entry.fragListBuilder,
                    entry.fragmentsBuilder, field.fieldOptions().preTags(), field.fieldOptions().postTags(), encoder);
            }

            if (fragments != null && fragments.length > 0) {
                return new HighlightField(highlighterContext.fieldName, Text.convertFromStringArray(fragments));
            }

            int noMatchSize = highlighterContext.field.fieldOptions().noMatchSize();
            if (noMatchSize > 0) {
                // Essentially we just request that a fragment is built from 0 to noMatchSize using
                // the normal fragmentsBuilder
                FieldFragList fieldFragList = new SimpleFieldFragList(-1 /*ignored*/);
                fieldFragList.add(0, noMatchSize, Collections.<WeightedPhraseInfo>emptyList());
                fragments = entry.fragmentsBuilder.createFragments(hitContext.reader(), hitContext.docId(),
                    fieldType.name(), fieldFragList, 1, field.fieldOptions().preTags(),
                    field.fieldOptions().postTags(), encoder);
                if (fragments != null && fragments.length > 0) {
                    return new HighlightField(highlighterContext.fieldName, Text.convertFromStringArray(fragments));
                }
            }

            return null;

        } catch (Exception e) {
            throw new FetchPhaseExecutionException(context,
                "Failed to highlight field [" + highlighterContext.fieldName + "]", e);
        }
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return fieldType.storeTermVectors()
            && fieldType.storeTermVectorOffsets()
            && fieldType.storeTermVectorPositions();
    }

    private static BoundaryScanner getBoundaryScanner(Field field) {
        final FieldOptions fieldOptions = field.fieldOptions();
        final Locale boundaryScannerLocale =
            fieldOptions.boundaryScannerLocale() != null ? fieldOptions.boundaryScannerLocale() :
                Locale.ROOT;
        final HighlightBuilder.BoundaryScannerType type =
            fieldOptions.boundaryScannerType()  != null ? fieldOptions.boundaryScannerType() :
                HighlightBuilder.BoundaryScannerType.CHARS;
        switch(type) {
            case SENTENCE:
                if (boundaryScannerLocale != null) {
                    return new BreakIteratorBoundaryScanner(BreakIterator.getSentenceInstance(boundaryScannerLocale));
                }
                return DEFAULT_SENTENCE_BOUNDARY_SCANNER;
            case WORD:
                if (boundaryScannerLocale != null) {
                    return new BreakIteratorBoundaryScanner(BreakIterator.getWordInstance(boundaryScannerLocale));
                }
                return DEFAULT_WORD_BOUNDARY_SCANNER;
            case CHARS:
                if (fieldOptions.boundaryMaxScan() != SimpleBoundaryScanner.DEFAULT_MAX_SCAN
                    || fieldOptions.boundaryChars() != SimpleBoundaryScanner.DEFAULT_BOUNDARY_CHARS) {
                    return new SimpleBoundaryScanner(fieldOptions.boundaryMaxScan(), fieldOptions.boundaryChars());
                }
                return DEFAULT_SIMPLE_BOUNDARY_SCANNER;
            default:
                throw new IllegalArgumentException("Invalid boundary scanner type: " + type.toString());
        }
    }

    private class FieldHighlightEntry {
        public FragListBuilder fragListBuilder;
        public FragmentsBuilder fragmentsBuilder;
        public FieldQuery noFieldMatchFieldQuery;
        public FieldQuery fieldMatchFieldQuery;
    }

    private class HighlighterEntry {
        public org.apache.lucene.search.vectorhighlight.FastVectorHighlighter fvh;
        public Map<MappedFieldType, FieldHighlightEntry> fields = new HashMap<>();
    }
}
