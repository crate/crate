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
package org.elasticsearch.test.hamcrest;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ElasticsearchAssertions {

    public static void assertAcked(AcknowledgedRequestBuilder<?, ?, ?> builder) {
        assertAcked(builder.get());
    }

    public static void assertNoTimeout(ClusterHealthRequestBuilder requestBuilder) {
        assertNoTimeout(requestBuilder.get());
    }

    public static void assertNoTimeout(ClusterHealthResponse response) {
        assertThat("ClusterHealthResponse has timed out - returned: [" + response + "]", response.isTimedOut(), is(false));
    }

    public static void assertAcked(AcknowledgedResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
    }

    public static void assertAcked(DeleteIndexRequestBuilder builder) {
        assertAcked(builder.get());
    }

    /**
     * Assert that an index creation was fully acknowledged, meaning that both the index creation cluster
     * state update was successful and that the requisite number of shard copies were started before returning.
     */
    public static void assertAcked(CreateIndexResponse response) {
        assertThat(response.getClass().getSimpleName() + " failed - not acked", response.isAcknowledged(), equalTo(true));
        assertTrue(response.getClass().getSimpleName() + " failed - index creation acked but not all shards were started",
            response.isShardsAcknowledged());
    }

    /**
     * Executes the request and fails if the request has not been blocked.
     *
     * @param builder the request builder
     */
    public static void assertBlocked(ActionRequestBuilder builder) {
        assertBlocked(builder, null);
    }

    /**
     * Checks that all shard requests of a replicated broadcast request failed due to a cluster block
     *
     * @param replicatedBroadcastResponse the response that should only contain failed shard responses
     *
     * */
    public static void assertBlocked(BroadcastResponse replicatedBroadcastResponse) {
        assertThat("all shard requests should have failed",
                replicatedBroadcastResponse.getFailedShards(), equalTo(replicatedBroadcastResponse.getTotalShards()));
        for (DefaultShardOperationFailedException exception : replicatedBroadcastResponse.getShardFailures()) {
            ClusterBlockException clusterBlockException =
                    (ClusterBlockException) ExceptionsHelper.unwrap(exception.getCause(), ClusterBlockException.class);
            assertNotNull("expected the cause of failure to be a ClusterBlockException but got " + exception.getCause().getMessage(),
                    clusterBlockException);
            assertThat(clusterBlockException.blocks().size(), greaterThan(0));
            assertThat(clusterBlockException.status(), CoreMatchers.equalTo(RestStatus.FORBIDDEN));
        }
    }

    /**
     * Executes the request and fails if the request has not been blocked by a specific {@link ClusterBlock}.
     *
     * @param builder the request builder
     * @param expectedBlock the expected block
     */
    public static void assertBlocked(ActionRequestBuilder builder, ClusterBlock expectedBlock) {
        try {
            builder.get();
            fail("Request executed with success but a ClusterBlockException was expected");
        } catch (ClusterBlockException e) {
            assertThat(e.blocks().size(), greaterThan(0));
            assertThat(e.status(), equalTo(RestStatus.FORBIDDEN));

            if (expectedBlock != null) {
                boolean found = false;
                for (ClusterBlock clusterBlock : e.blocks()) {
                    if (clusterBlock.id() == expectedBlock.id()) {
                        found = true;
                        break;
                    }
                }
                assertThat("Request should have been blocked by [" + expectedBlock + "] instead of " + e.blocks(), found, equalTo(true));
            }
        }
    }

    public static String formatShardStatus(BroadcastResponse response) {
        StringBuilder msg = new StringBuilder();
        msg.append(" Total shards: ").append(response.getTotalShards())
           .append(" Successful shards: ").append(response.getSuccessfulShards())
           .append(" & ").append(response.getFailedShards()).append(" shard failures:");
        for (DefaultShardOperationFailedException failure : response.getShardFailures()) {
            msg.append("\n ").append(failure);
        }
        return msg.toString();
    }

    public static String formatShardStatus(SearchResponse response) {
        StringBuilder msg = new StringBuilder();
        msg.append(" Total shards: ").append(response.getTotalShards())
           .append(" Successful shards: ").append(response.getSuccessfulShards())
           .append(" & ").append(response.getFailedShards()).append(" shard failures:");
        for (ShardSearchFailure failure : response.getShardFailures()) {
            msg.append("\n ").append(failure);
        }
        return msg.toString();
    }

    public static void assertNoSearchHits(SearchResponse searchResponse) {
        assertEquals(0, searchResponse.getHits().getHits().length);
    }

    public static void assertSearchHits(SearchResponse searchResponse, String... ids) {
        String shardStatus = formatShardStatus(searchResponse);

        Set<String> idsSet = new HashSet<>(Arrays.asList(ids));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(
                    "id [" + hit.getId() + "] was found in search results but wasn't expected (type [" + hit.getType()
                        + "], index [" + hit.getIndex() + "])" + shardStatus, idsSet.remove(hit.getId()),
                    equalTo(true));
        }
        assertThat("Some expected ids were not found in search results: " + Arrays.toString(idsSet.toArray(new String[idsSet.size()])) + "."
                + shardStatus, idsSet.size(), equalTo(0));
    }

    public static void assertSortValues(SearchResponse searchResponse, Object[]... sortValues) {
        assertSearchResponse(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertEquals(sortValues.length, hits.length);
        for (int i = 0; i < sortValues.length; ++i) {
            final Object[] hitsSortValues = hits[i].getSortValues();
            assertArrayEquals("Offset " + Integer.toString(i) + ", id " + hits[i].getId(), sortValues[i], hitsSortValues);
        }
    }

    public static void assertOrderedSearchHits(SearchResponse searchResponse, String... ids) {
        String shardStatus = formatShardStatus(searchResponse);
        assertThat("Expected different hit count. " + shardStatus, searchResponse.getHits().getHits().length, equalTo(ids.length));
        for (int i = 0; i < ids.length; i++) {
            SearchHit hit = searchResponse.getHits().getHits()[i];
            assertThat("Expected id: " + ids[i] + " at position " + i + " but wasn't." + shardStatus, hit.getId(), equalTo(ids[i]));
        }
    }

    public static void assertHitCount(SearchResponse countResponse, long expectedHitCount) {
        if (countResponse.getHits().getTotalHits() != expectedHitCount) {
            fail("Count is " + countResponse.getHits().getTotalHits() + " but " + expectedHitCount
                    + " was expected. " + formatShardStatus(countResponse));
        }
    }

    public static void assertExists(GetResponse response) {
        String message = String.format(Locale.ROOT, "Expected %s/%s/%s to exist, but does not",
                response.getIndex(), response.getType(), response.getId());
        assertThat(message, response.isExists(), is(true));
    }

    public static void assertFirstHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 1, matcher);
    }

    public static void assertSecondHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 2, matcher);
    }

    public static void assertThirdHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 3, matcher);
    }

    public static void assertFourthHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 4, matcher);
    }

    public static void assertFifthHit(SearchResponse searchResponse, Matcher<SearchHit> matcher) {
        assertSearchHit(searchResponse, 5, matcher);
    }

    public static void assertSearchHit(SearchResponse searchResponse, int number, Matcher<SearchHit> matcher) {
        assertThat(number, greaterThan(0));
        assertThat("SearchHit number must be greater than 0", number, greaterThan(0));
        assertThat(searchResponse.getHits().getTotalHits(), greaterThanOrEqualTo((long) number));
        assertThat(searchResponse.getHits().getAt(number - 1), matcher);
    }

    public static void assertNoFailures(SearchResponse searchResponse) {
        assertThat("Unexpected ShardFailures: " + Arrays.toString(searchResponse.getShardFailures()),
                searchResponse.getShardFailures().length, equalTo(0));
    }

    public static void assertFailures(SearchResponse searchResponse) {
        assertThat("Expected at least one shard failure, got none",
                searchResponse.getShardFailures().length, greaterThan(0));
    }

    public static void assertNoFailures(BulkResponse response) {
        assertThat("Unexpected ShardFailures: " + response.buildFailureMessage(),
                response.hasFailures(), is(false));
    }

    public static void assertFailures(SearchRequestBuilder searchRequestBuilder, RestStatus restStatus, Matcher<String> reasonMatcher) {
        //when the number for shards is randomized and we expect failures
        //we can either run into partial or total failures depending on the current number of shards
        try {
            SearchResponse searchResponse = searchRequestBuilder.get();
            assertThat("Expected shard failures, got none", searchResponse.getShardFailures().length, greaterThan(0));
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                assertThat(shardSearchFailure.status(), equalTo(restStatus));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.status(), equalTo(restStatus));
            assertThat(e.toString(), reasonMatcher);
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.status(), equalTo(restStatus));
                assertThat(shardSearchFailure.reason(), reasonMatcher);
            }
        } catch (Exception e) {
            fail("SearchPhaseExecutionException expected but got " + e.getClass());
        }
    }

    public static void assertNoFailures(BroadcastResponse response) {
        assertThat("Unexpected ShardFailures: " + Arrays.toString(response.getShardFailures()), response.getFailedShards(), equalTo(0));
    }

    public static void assertAllSuccessful(BroadcastResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful",
                response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }

    public static void assertAllSuccessful(SearchResponse response) {
        assertNoFailures(response);
        assertThat("Expected all shards successful",
                response.getSuccessfulShards(), equalTo(response.getTotalShards()));
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(resp, hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(SearchResponse resp, int hit, String field, int fragment,
            int totalFragments, Matcher<String> matcher) {
        assertHighlight(resp, hit, field, fragment, equalTo(totalFragments), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, greaterThan(fragment), matcher);
    }

    public static void assertHighlight(SearchHit hit, String field, int fragment, int totalFragments, Matcher<String> matcher) {
        assertHighlight(hit, field, fragment, equalTo(totalFragments), matcher);
    }

    private static void assertHighlight(SearchResponse resp, int hit, String field, int fragment,
            Matcher<Integer> fragmentsMatcher, Matcher<String> matcher) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().getHits().length, greaterThan(hit));
        assertHighlight(resp.getHits().getHits()[hit], field, fragment, fragmentsMatcher, matcher);
    }

    private static void assertHighlight(SearchHit hit, String field, int fragment,
            Matcher<Integer> fragmentsMatcher, Matcher<String> matcher) {
        assertThat(hit.getHighlightFields(), hasKey(field));
        assertThat(hit.getHighlightFields().get(field).fragments().length, fragmentsMatcher);
        assertThat(hit.getHighlightFields().get(field).fragments()[fragment].string(), matcher);
    }

    public static void assertNotHighlighted(SearchResponse resp, int hit, String field) {
        assertNoFailures(resp);
        assertThat("not enough hits", resp.getHits().getHits().length, greaterThan(hit));
        assertThat(resp.getHits().getHits()[hit].getHighlightFields(), not(hasKey(field)));
    }

    public static void assertSuggestionSize(Suggest searchSuggest, int entry, int size, String key) {
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), equalTo(size));
    }

    public static void assertSuggestionPhraseCollateMatchExists(Suggest searchSuggest, String key, int numberOfPhraseExists) {
        int counter = 0;
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));

        for (Suggest.Suggestion.Entry.Option option : searchSuggest.getSuggestion(key).getEntries().get(0).getOptions()) {
            if (option.collateMatch()) {
                counter++;
            }
        }

        assertThat(counter, equalTo(numberOfPhraseExists));
    }

    public static void assertSuggestion(Suggest searchSuggest, int entry, int ord, String key, String text) {
        assertThat(searchSuggest, notNullValue());
        String msg = "Suggest result: " + searchSuggest.toString();
        assertThat(msg, searchSuggest.size(), greaterThanOrEqualTo(1));
        assertThat(msg, searchSuggest.getSuggestion(key).getName(), equalTo(key));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().size(), greaterThanOrEqualTo(entry));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().size(), greaterThan(ord));
        assertThat(msg, searchSuggest.getSuggestion(key).getEntries().get(entry).getOptions().get(ord).getText().string(), equalTo(text));
    }

    /**
     * Assert suggestion returns exactly the provided text.
     */
    public static void assertSuggestion(Suggest searchSuggest, int entry, String key, String... text) {
        assertSuggestion(searchSuggest, entry, key, text.length, text);
    }

    /**
     * Assert suggestion returns size suggestions and the first are the provided
     * text.
     */
    public static void assertSuggestion(Suggest searchSuggest, int entry, String key, int size, String... text) {
        assertSuggestionSize(searchSuggest, entry, size, key);
        for (int i = 0; i < text.length; i++) {
            assertSuggestion(searchSuggest, entry, i, key, text[i]);
        }
    }

    /**
     * Assert that an index template is missing
     */
    public static void assertIndexTemplateMissing(GetIndexTemplatesResponse templatesResponse, String name) {
        List<String> templateNames = new ArrayList<>();
        for (IndexTemplateMetaData indexTemplateMetaData : templatesResponse.getIndexTemplates()) {
            templateNames.add(indexTemplateMetaData.name());
        }
        assertThat(templateNames, not(hasItem(name)));
    }

    /**
     * Assert that an index template exists
     */
    public static void assertIndexTemplateExists(GetIndexTemplatesResponse templatesResponse, String name) {
        List<String> templateNames = new ArrayList<>();
        for (IndexTemplateMetaData indexTemplateMetaData : templatesResponse.getIndexTemplates()) {
            templateNames.add(indexTemplateMetaData.name());
        }
        assertThat(templateNames, hasItem(name));
    }

    /**
     * Assert that aliases are missing
     */
    public static void assertAliasesMissing(AliasesExistResponse aliasesExistResponse) {
        assertFalse("Aliases shouldn't exist", aliasesExistResponse.exists());
    }

    /**
     * Assert that aliases exist
     */
    public static void assertAliasesExist(AliasesExistResponse aliasesExistResponse) {
        assertTrue("Aliases should exist", aliasesExistResponse.exists());
    }

    /*
     * matchers
     */
    public static Matcher<SearchHit> hasId(final String id) {
        return new ElasticsearchMatchers.SearchHitHasIdMatcher(id);
    }

    public static Matcher<SearchHit> hasType(final String type) {
        return new ElasticsearchMatchers.SearchHitHasTypeMatcher(type);
    }

    public static Matcher<SearchHit> hasIndex(final String index) {
        return new ElasticsearchMatchers.SearchHitHasIndexMatcher(index);
    }

    public static Matcher<SearchHit> hasScore(final float score) {
        return new ElasticsearchMatchers.SearchHitHasScoreMatcher(score);
    }

    public static <T extends Query> T assertBooleanSubQuery(Query query, Class<T> subqueryType, int i) {
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery q = (BooleanQuery) query;
        assertThat(q.clauses().size(), greaterThan(i));
        assertThat(q.clauses().get(i).getQuery(), instanceOf(subqueryType));
        return subqueryType.cast(q.clauses().get(i).getQuery());
    }

    public static <T extends Query> T assertDisjunctionSubQuery(Query query, Class<T> subqueryType, int i) {
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery q = (DisjunctionMaxQuery) query;
        assertThat(q.getDisjuncts().size(), greaterThan(i));
        assertThat(q.getDisjuncts().get(i), instanceOf(subqueryType));
        return subqueryType.cast(q.getDisjuncts().get(i));
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, Class<E> exceptionClass) {
        assertThrows(builder.execute(), exceptionClass);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type, with a given {@link RestStatus}
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder,
            Class<E> exceptionClass, RestStatus status) {
        assertThrows(builder.execute(), exceptionClass, status);
    }

    /**
     * Run the request from a given builder and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder,
            Class<E> exceptionClass, String extraInfo) {
        assertThrows(builder.execute(), exceptionClass, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass) {
        assertThrows(future, exceptionClass, null, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, with a given {@link RestStatus}
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, RestStatus status) {
        assertThrows(future, exceptionClass, status, null);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type
     *
     * @param extraInfo extra information to add to the failure message
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass, String extraInfo) {
        assertThrows(future, exceptionClass, null, extraInfo);
    }

    /**
     * Run future.actionGet() and check that it throws an exception of the right type, optionally checking the exception's rest status
     *
     * @param exceptionClass expected exception class
     * @param status         {@link org.elasticsearch.rest.RestStatus} to check for. Can be null to disable the check
     * @param extraInfo      extra information to add to the failure message. Can be null.
     */
    public static <E extends Throwable> void assertThrows(ActionFuture future, Class<E> exceptionClass,
            @Nullable RestStatus status, @Nullable String extraInfo) {
        boolean fail = false;
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + exceptionClass + " exception to be thrown";

        if (status != null) {
            extraInfo += " with status [" + status + "]";
        }

        try {
            future.actionGet();
            fail = true;

        } catch (ElasticsearchException esException) {
            assertThat(extraInfo, esException.unwrapCause(), instanceOf(exceptionClass));
            if (status != null) {
                assertThat(extraInfo, ExceptionsHelper.status(esException), equalTo(status));
            }
        } catch (Exception e) {
            assertThat(extraInfo, e, instanceOf(exceptionClass));
            if (status != null) {
                assertThat(extraInfo, ExceptionsHelper.status(e), equalTo(status));
            }
        }
        // has to be outside catch clause to get a proper message
        if (fail) {
            throw new AssertionError(extraInfo);
        }
    }

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, RestStatus status) {
        assertThrows(builder.execute(), status);
    }

    public static <E extends Throwable> void assertThrows(ActionRequestBuilder<?, ?, ?> builder, RestStatus status, String extraInfo) {
        assertThrows(builder.execute(), status, extraInfo);
    }

    public static <E extends Throwable> void assertThrows(ActionFuture future, RestStatus status) {
        assertThrows(future, status, null);
    }

    public static void assertThrows(ActionFuture future, RestStatus status, String extraInfo) {
        boolean fail = false;
        extraInfo = extraInfo == null || extraInfo.isEmpty() ? "" : extraInfo + ": ";
        extraInfo += "expected a " + status + " status exception to be thrown";

        try {
            future.actionGet();
            fail = true;
        } catch (Exception e) {
            assertThat(extraInfo, ExceptionsHelper.status(e), equalTo(status));
        }
        // has to be outside catch clause to get a proper message
        if (fail) {
            throw new AssertionError(extraInfo);
        }
    }

    /**
     * Applies basic assertions on the SearchResponse. This method checks if all shards were successful, if
     * any of the shards threw an exception and if the response is serializable.
     */
    public static SearchResponse assertSearchResponse(SearchRequestBuilder request) {
        return assertSearchResponse(request.get());
    }

    /**
     * Applies basic assertions on the SearchResponse. This method checks if all shards were successful, if
     * any of the shards threw an exception and if the response is serializable.
     */
    public static SearchResponse assertSearchResponse(SearchResponse response) {
        assertNoFailures(response);
        return response;
    }

    /**
     * Check if a file exists
     */
    public static void assertFileExists(Path file) {
        assertThat("file/dir [" + file + "] should exist.", Files.exists(file), is(true));
    }

    /**
     * Check if a file does not exist
     */
    public static void assertFileNotExists(Path file) {
        assertThat("file/dir [" + file + "] should not exist.", Files.exists(file), is(false));
    }

    /**
     * Check if a directory exists
     */
    public static void assertDirectoryExists(Path dir) {
        assertFileExists(dir);
        assertThat("file [" + dir + "] should be a directory.", Files.isDirectory(dir), is(true));
    }

    /**
     * Asserts that the provided {@link BytesReference}s created through
     * {@link org.elasticsearch.common.xcontent.ToXContent#toXContent(XContentBuilder, ToXContent.Params)} hold the same content.
     * The comparison is done by parsing both into a map and comparing those two, so that keys ordering doesn't matter.
     * Also binary values (byte[]) are properly compared through arrays comparisons.
     */
    public static void assertToXContentEquivalent(BytesReference expected, BytesReference actual, XContentType xContentType)
            throws IOException {
        //we tried comparing byte per byte, but that didn't fly for a couple of reasons:
        //1) whenever anything goes through a map while parsing, ordering is not preserved, which is perfectly ok
        //2) Jackson SMILE parser parses floats as double, which then get printed out as double (with double precision)
        //Note that byte[] holding binary values need special treatment as they need to be properly compared item per item.
        Map<String, Object> actualMap = null;
        Map<String, Object> expectedMap = null;
        try (XContentParser actualParser = xContentType.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, actual.streamInput())) {
            actualMap = actualParser.map();
            try (XContentParser expectedParser = xContentType.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, expected.streamInput())) {
                expectedMap = expectedParser.map();
                try {
                    assertMapEquals(expectedMap, actualMap);
                } catch (AssertionError error) {
                    NotEqualMessageBuilder message = new NotEqualMessageBuilder();
                    message.compareMaps(actualMap, expectedMap);
                    throw new AssertionError("Error when comparing xContent.\n" + message.toString(), error);
                }
            }
        }
    }

    /**
     * Compares two maps recursively, using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    private static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (Map.Entry<String, Object> expectedEntry : expected.entrySet()) {
            String expectedKey = expectedEntry.getKey();
            Object expectedValue = expectedEntry.getValue();
            if (expectedValue == null) {
                assertTrue(actual.get(expectedKey) == null && actual.containsKey(expectedKey));
            } else {
                Object actualValue = actual.get(expectedKey);
                assertObjectEquals(expectedValue, actualValue);
            }
        }
    }

    /**
     * Compares two lists recursively, but using arrays comparisons for byte[] through Arrays.equals(byte[], byte[])
     */
    @SuppressWarnings("unchecked")
    private static void assertListEquals(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size());
        Iterator<Object> actualIterator = actual.iterator();
        for (Object expectedValue : expected) {
            Object actualValue = actualIterator.next();
            assertObjectEquals(expectedValue, actualValue);
        }
    }

    /**
     * Compares two objects, recursively walking eventual maps and lists encountered, and using arrays comparisons
     * for byte[] through Arrays.equals(byte[], byte[])
     */
    @SuppressWarnings("unchecked")
    private static void assertObjectEquals(Object expected, Object actual) {
        if (expected instanceof Map) {
            assertThat(actual, instanceOf(Map.class));
            assertMapEquals((Map<String, Object>) expected, (Map<String, Object>) actual);
        } else if (expected instanceof List) {
            assertListEquals((List<Object>) expected, (List<Object>) actual);
        } else if (expected instanceof byte[]) {
            //byte[] is really a special case for binary values when comparing SMILE and CBOR, arrays of other types
            //don't need to be handled. Ordinary arrays get parsed as lists.
            assertArrayEquals((byte[]) expected, (byte[]) actual);
        } else {
            assertEquals(expected, actual);
        }
    }
}
