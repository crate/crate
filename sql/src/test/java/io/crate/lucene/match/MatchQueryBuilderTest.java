/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.lucene.match;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class MatchQueryBuilderTest extends CrateUnitTest {

    private IndexCache cache;

    @Before
    public void prepare() throws Exception {
        cache = mock(IndexCache.class, Answers.RETURNS_MOCKS.get());
    }

    @Test
    public void testUnknownMatchType() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(
                "Unknown matchType \"foo\". Possible matchTypes are: best_fields, most_fields, cross_fields, phrase, phrase_prefix");
        new MatchQueryBuilder(null, cache, new BytesRef("foo"), Collections.emptyMap());
    }

    @Test
    public void testDefaultMatchType() throws Exception {
        MatchQueryBuilder builder = new MatchQueryBuilder(null, cache, null, null);
        assertThat(builder.matchType, equalTo(MultiMatchQueryBuilder.Type.BEST_FIELDS));
    }

    @Test
    public void testUnknownOption() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("match predicate doesn't support any of the given options: foobar, wrong");

        Map options = newMapBuilder().put("foobar", "foo").put("wrong", "option").map();
        new MatchQueryBuilder(null, cache, null, options);
    }

    @Test
    public void testIllegalTypeOptionCombination() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("match predicate option(s) \"slop\" cannot be used with matchType \"best_fields\"");

        Map options = newMapBuilder().put("slop", 4).map();
        new MatchQueryBuilder(null, cache, null, options);
    }

    @Test
    public void testSimpleSingleMatchSingleTerm() throws Exception {
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder().put("col1", null).map();
        MatchQueryBuilder builder = new MatchQueryBuilder(mockMapperService(), cache, null, Collections.emptyMap());
        Query query = builder.query(fields, new BytesRef("foo"));
        assertThat(query, instanceOf(TermQuery.class));
    }

    @Test
    public void testSimpleSingleMatchTwoTerms() throws Exception {
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder().put("col1", null).map();
        MatchQueryBuilder builder = new MatchQueryBuilder(mockMapperService(), cache, null, Collections.emptyMap());
        Query query = builder.query(fields, new BytesRef("foo bar"));
        assertThat(query, instanceOf(BooleanQuery.class));
    }

    @Test
    public void testSingleFieldWithCutFrequency() throws Exception {
        MatchQueryBuilder builder = new MatchQueryBuilder(
                mockMapperService(), cache, null, newMapBuilder().put("cutoff_frequency", 3).map());

        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder().put("col1", null).map();
        Query query = builder.query(fields, new BytesRef("foo bar"));
        assertThat(query, instanceOf(ExtendedCommonTermsQuery.class));
    }

    @Test
    public void testTwoFieldsSingleTerm() throws Exception {
        MatchQueryBuilder builder = new io.crate.lucene.match.MultiMatchQueryBuilder(
                mockMapperService(), cache, null, Collections.emptyMap());
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder()
                .put("col1", null)
                .put("col2", null).map();
        Query query = builder.query(fields, new BytesRef("foo"));
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
    }

    @Test
    public void testTwoFieldsSingleTermMostFields() throws Exception {
        MatchQueryBuilder builder = new io.crate.lucene.match.MultiMatchQueryBuilder(
                mockMapperService(), cache, new BytesRef("most_fields"), Collections.emptyMap());
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder()
                .put("col1", null)
                .put("col2", null).map();
        Query query = builder.query(fields, new BytesRef("foo"));
        assertThat(query, instanceOf(BooleanQuery.class));
    }

    @Test
    public void testCrossFieldMatchType() throws Exception {
        Analyzer analyzer = new GermanAnalyzer();
        MapperService.SmartNameFieldMappers smartNameFieldMappers = mock(MapperService.SmartNameFieldMappers.class);
        when(smartNameFieldMappers.hasMapper()).thenReturn(true);
        FieldMapper fieldMapper = mock(FieldMapper.class, Answers.RETURNS_MOCKS.get());
        when(smartNameFieldMappers.mapper()).thenReturn(fieldMapper);
        when(fieldMapper.searchAnalyzer()).thenReturn(analyzer);

        MapperService mapperService = mock(MapperService.class);
        when(mapperService.smartName(anyString())).thenReturn(smartNameFieldMappers);
        when(mapperService.searchAnalyzer()).thenReturn(analyzer);

        MatchQueryBuilder builder = new io.crate.lucene.match.MultiMatchQueryBuilder(
                mapperService, cache, new BytesRef("cross_fields"), Collections.emptyMap());
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder()
                .put("col1", null)
                .put("col2", null)
                .map();
        Query query = builder.query(fields, new BytesRef("foo bar"));
        assertThat(query, instanceOf(BooleanQuery.class));

        Query innerQuery = ((BooleanQuery) query).clauses().get(0).getQuery();
        assertThat(innerQuery, instanceOf(BlendedTermQuery.class));
    }


    @Test
    public void testFuzzyQuery() throws Exception {
        MatchQueryBuilder builder = new io.crate.lucene.match.MultiMatchQueryBuilder(
                mockMapperService(), cache, null, newMapBuilder().put("fuzziness", 2).map());
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder().put("col1", null).map();

        Query query = builder.query(fields, new BytesRef("foo"));
        assertThat(query, instanceOf(FuzzyQuery.class));
    }

    @Test
    public void testPhraseQuery() throws Exception {
        MatchQueryBuilder builder = new MatchQueryBuilder(
                mockMapperService(), cache, new BytesRef("phrase"), Collections.emptyMap());

        Query query = builder.query(
                MapBuilder.<String, Object>newMapBuilder().put("col1", null).map(),
                new BytesRef("foo bar")
        );
        assertThat(query, instanceOf(PhraseQuery.class));
    }

    @Test
    public void testPhrasePrefix() throws Exception {
        MatchQueryBuilder builder = new MatchQueryBuilder(
                mockMapperService(), cache, new BytesRef("phrase_prefix"), Collections.emptyMap());
        Map<String, Object> fields = MapBuilder.<String, Object>newMapBuilder().put("col1", null).map();
        Query query = builder.query(fields, new BytesRef("foo"));
        assertThat(query, instanceOf(MultiPhrasePrefixQuery.class));
    }

    private MapperService mockMapperService() {
        Analyzer analyzer = new GermanAnalyzer();
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.searchAnalyzer()).thenReturn(analyzer);
        return mapperService;
    }
}