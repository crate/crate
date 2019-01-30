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

package org.elasticsearch.test;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;


public abstract class AbstractXContentTestCase<T extends ToXContent> extends ESTestCase {
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    public static <T> XContentTester<T> xContentTester(
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
            Supplier<T> instanceSupplier,
            CheckedBiConsumer<T, XContentBuilder, IOException> toXContent,
            CheckedFunction<XContentParser, T, IOException> fromXContent) {
        return new XContentTester<T>(
                createParser,
                instanceSupplier,
                (testInstance, xContentType) -> {
                    try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                        toXContent.accept(testInstance, builder);
                        return BytesReference.bytes(builder);
                    }
                },
                fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
            Supplier<T> instanceSupplier,
            CheckedFunction<XContentParser, T, IOException> fromXContent) {
        return xContentTester(createParser, instanceSupplier, ToXContent.EMPTY_PARAMS, fromXContent);
    }

    public static <T extends ToXContent> XContentTester<T> xContentTester(
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
            Supplier<T> instanceSupplier,
            ToXContent.Params toXContentParams,
            CheckedFunction<XContentParser, T, IOException> fromXContent) {
        return new XContentTester<T>(
                createParser,
                instanceSupplier,
                (testInstance, xContentType) ->
                        XContentHelper.toXContent(testInstance, xContentType, toXContentParams, false),
                fromXContent);
    }

    /**
     * Tests converting to and from xcontent.
     */
    public static class XContentTester<T> {
        private final CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser;
        private final Supplier<T> instanceSupplier;
        private final CheckedBiFunction<T, XContentType, BytesReference, IOException> toXContent;
        private final CheckedFunction<XContentParser, T, IOException> fromXContent;

        private int numberOfTestRuns = NUMBER_OF_TEST_RUNS;
        private boolean supportsUnknownFields = false;
        private String[] shuffleFieldsExceptions = Strings.EMPTY_ARRAY;
        private Predicate<String> randomFieldsExcludeFilter = field -> false;
        private BiConsumer<T, T> assertEqualsConsumer = (expectedInstance, newInstance) -> {
            assertNotSame(newInstance, expectedInstance);
            assertEquals(expectedInstance, newInstance);
            assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
        };
        private boolean assertToXContentEquivalence = true;

        private XContentTester(
                CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParser,
                Supplier<T> instanceSupplier,
                CheckedBiFunction<T, XContentType, BytesReference, IOException> toXContent,
                CheckedFunction<XContentParser, T, IOException> fromXContent) {
            this.createParser = createParser;
            this.instanceSupplier = instanceSupplier;
            this.toXContent = toXContent;
            this.fromXContent = fromXContent;
        }

        public void test() throws IOException {
            for (int runs = 0; runs < numberOfTestRuns; runs++) {
                T testInstance = instanceSupplier.get();
                XContentType xContentType = randomFrom(XContentType.values());
                BytesReference originalXContent = toXContent.apply(testInstance, xContentType);
                BytesReference shuffledContent = insertRandomFieldsAndShuffle(originalXContent, xContentType, supportsUnknownFields,
                        shuffleFieldsExceptions, randomFieldsExcludeFilter, createParser);
                XContentParser parser = createParser.apply(XContentFactory.xContent(xContentType), shuffledContent);
                T parsed = fromXContent.apply(parser);
                assertEqualsConsumer.accept(testInstance, parsed);
                if (assertToXContentEquivalence) {
                    assertToXContentEquivalent(
                            toXContent.apply(testInstance, xContentType),
                            toXContent.apply(parsed, xContentType),
                            xContentType);
                }
            }
        }

        public XContentTester<T> numberOfTestRuns(int numberOfTestRuns) {
            this.numberOfTestRuns = numberOfTestRuns;
            return this;
        }

        public XContentTester<T> supportsUnknownFields(boolean supportsUnknownFields) {
            this.supportsUnknownFields = supportsUnknownFields;
            return this;
        }

        public XContentTester<T> shuffleFieldsExceptions(String[] shuffleFieldsExceptions) {
            this.shuffleFieldsExceptions = shuffleFieldsExceptions;
            return this;
        }

        public XContentTester<T> randomFieldsExcludeFilter(Predicate<String> randomFieldsExcludeFilter) {
            this.randomFieldsExcludeFilter = randomFieldsExcludeFilter;
            return this;
        }

        public XContentTester<T> assertEqualsConsumer(BiConsumer<T, T> assertEqualsConsumer) {
            this.assertEqualsConsumer = assertEqualsConsumer;
            return this;
        }

        public XContentTester<T> assertToXContentEquivalence(boolean assertToXContentEquivalence) {
            this.assertToXContentEquivalence = assertToXContentEquivalence;
            return this;
        }
    }

    public static <T extends ToXContent> void testFromXContent(
            int numberOfTestRuns,
            Supplier<T> instanceSupplier,
            boolean supportsUnknownFields,
            String[] shuffleFieldsExceptions,
            Predicate<String> randomFieldsExcludeFilter,
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction,
            CheckedFunction<XContentParser, T, IOException> fromXContent,
            BiConsumer<T, T> assertEqualsConsumer,
            boolean assertToXContentEquivalence,
            ToXContent.Params toXContentParams) throws IOException {
        xContentTester(createParserFunction, instanceSupplier, toXContentParams, fromXContent)
                .numberOfTestRuns(numberOfTestRuns)
                .supportsUnknownFields(supportsUnknownFields)
                .shuffleFieldsExceptions(shuffleFieldsExceptions)
                .randomFieldsExcludeFilter(randomFieldsExcludeFilter)
                .assertEqualsConsumer(assertEqualsConsumer)
                .assertToXContentEquivalence(assertToXContentEquivalence)
                .test();
    }

    /**
     * Generic test that creates new instance from the test instance and checks
     * both for equality and asserts equality on the two queries.
     */
    public final void testFromXContent() throws IOException {
        testFromXContent(NUMBER_OF_TEST_RUNS, this::createTestInstance, supportsUnknownFields(), getShuffleFieldsExceptions(),
                getRandomFieldsExcludeFilter(), this::createParser, this::parseInstance, this::assertEqualInstances,
                assertToXContentEquivalence(), getToXContentParams());
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    protected abstract T createTestInstance();

    private T parseInstance(XContentParser parser) throws IOException {
        T parsedInstance = doParseInstance(parser);
        assertNull(parser.nextToken());
        return parsedInstance;
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     */
    protected abstract T doParseInstance(XContentParser parser) throws IOException;

    protected void assertEqualInstances(T expectedInstance, T newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
    }

    protected boolean assertToXContentEquivalence() {
        return true;
    }

    /**
     * Indicates whether the parser supports unknown fields or not. In case it does, such behaviour will be tested by
     * inserting random fields before parsing and checking that they don't make parsing fail.
     */
    protected abstract boolean supportsUnknownFields();

    /**
     * Returns a predicate that given the field name indicates whether the field has to be excluded from random fields insertion or not
     */
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> false;
    }

    /**
     * Fields that have to be ignored when shuffling as part of testFromXContent
     */
    protected String[] getShuffleFieldsExceptions() {
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Params that have to be provided when calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)}
     */
    protected ToXContent.Params getToXContentParams() {
        return ToXContent.EMPTY_PARAMS;
    }

    static BytesReference insertRandomFieldsAndShuffle(BytesReference xContent, XContentType xContentType,
            boolean supportsUnknownFields, String[] shuffleFieldsExceptions, Predicate<String> randomFieldsExcludeFilter,
            CheckedBiFunction<XContent, BytesReference, XContentParser, IOException> createParserFunction) throws IOException {
        BytesReference withRandomFields;
        if (supportsUnknownFields) {
            // add a few random fields to check that the parser is lenient on new fields
            withRandomFields = XContentTestUtils.insertRandomFields(xContentType, xContent, randomFieldsExcludeFilter, random());
        } else {
            withRandomFields = xContent;
        }
        XContentParser parserWithRandonFields = createParserFunction.apply(XContentFactory.xContent(xContentType), withRandomFields);
        return BytesReference.bytes(ESTestCase.shuffleXContent(parserWithRandonFields, false, shuffleFieldsExceptions));
    }

}
