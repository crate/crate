/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.FailureMarker;
import org.apache.lucene.tests.util.LuceneJUnit3MethodProvider;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.lucene.tests.util.RunListenerPrintReproduceInfo;
import org.apache.lucene.tests.util.TestRuleMarkFailure;
import org.apache.lucene.tests.util.TimeUnits;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup.Group;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import io.crate.lucene.CrateLuceneTestCase.CommonPoolFilter;

/**
 * Base class for all Lucene unit tests, Junit3 or Junit4 variant.
 *
 * <h2>Class and instance setup.</h2>
 *
 * <p>The preferred way to specify class (suite-level) setup/cleanup is to use static methods
 * annotated with {@link BeforeClass} and {@link AfterClass}. Any code in these methods is executed
 * within the test framework's control and ensure proper setup has been made. <b>Try not to use
 * static initializers (including complex final field initializers).</b> Static initializers are
 * executed before any setup rules are fired and may cause you (or somebody else) headaches.
 *
 * <p>For instance-level setup, use {@link Before} and {@link After} annotated methods. If you
 * override either {@link #setUp()} or {@link #tearDown()} in your subclass, make sure you call
 * <code>super.setUp()</code> and <code>super.tearDown()</code>. This is detected and enforced.
 *
 * <h2>Specifying test cases</h2>
 *
 * <p>Any test method with a <code>testXXX</code> prefix is considered a test case. Any test method
 * annotated with {@link Test} is considered a test case.
 *
 * <h2>Randomized execution and test facilities</h2>
 *
 * <p>{@link CrateLuceneTestCase} uses {@link RandomizedRunner} to execute test cases. {@link
 * RandomizedRunner} has built-in support for tests randomization including access to a repeatable
 * {@link Random} instance. See {@link #random()} method. Any test using {@link Random} acquired
 * from {@link #random()} should be fully reproducible (assuming no race conditions between threads
 * etc.). The initial seed for a test case is reported in many ways:
 *
 * <ul>
 *   <li>as part of any exception thrown from its body (inserted as a dummy stack trace entry),
 *   <li>as part of the main thread executing the test case (if your test hangs, just dump the stack
 *       trace of all threads and you'll see the seed),
 *   <li>the master seed can also be accessed manually by getting the current context ({@link
 *       RandomizedContext#current()}) and then calling {@link
 *       RandomizedContext#getRunnerSeedAsString()}.
 * </ul>
 *
 * Copied and slightly modified {@link org.apache.lucene.tests.util.LuceneTestCase}
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({LuceneJUnit3MethodProvider.class, JUnit4MethodProvider.class})
@Listeners({RunListenerPrintReproduceInfo.class, FailureMarker.class})
@SeedDecorators({MixWithSuiteName.class}) // See LUCENE-3995 for rationale.
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakGroup(Group.MAIN)
@ThreadLeakAction({Action.WARN, Action.INTERRUPT})
// Wait long for leaked threads to complete before failure. zk needs this.
@ThreadLeakLingering(linger = 20000)
@ThreadLeakZombies(Consequence.IGNORE_REMAINING_TESTS)
@TimeoutSuite(millis = 2 * TimeUnits.HOUR)
@ThreadLeakFilters(
    defaultFilters = true,
    filters = {QuickPatchThreadsFilter.class, CommonPoolFilter.class})
public abstract class CrateLuceneTestCase {

    public static class LocalLuceneTestCase extends LuceneTestCase {
        public static TestRuleMarkFailure getSuiteFailureMarker() {
            return suiteFailureMarker;
        }
    }

    /**
     * Ignores thread leaks from {@link ForkJoinPool#commonPool()} which is used by components like
     * {@link HttpClient} and cannot be shutdown manually. It closes on SystemExit.
     * {@link CrateLuceneTestCase#assertNoActiveCommonPoolThreads()} ensures there are no actual tasks leaking.
     **/
    public static class CommonPoolFilter implements ThreadFilter {

        @Override
        public boolean reject(Thread t) {
            return t instanceof ForkJoinWorkerThread wt && wt.getPool() == ForkJoinPool.commonPool();
        }
    }

    protected LocalLuceneTestCase luceneTestCase = new LocalLuceneTestCase();

    @ClassRule public static TestRule classRules = LocalLuceneTestCase.classRules;

    @Rule
    public final TestRule ruleChain = luceneTestCase.ruleChain;

    public int hashCode() {
        return luceneTestCase.hashCode();
    }

    public boolean equals(Object obj) {
        return luceneTestCase.equals(obj);
    }

    public String toString() {
        return luceneTestCase.toString();
    }

    @Before
    public void setUp() throws Exception {
        luceneTestCase.setUp();
    }

    @After
    public void tearDown() throws Exception {
        luceneTestCase.tearDown();
    }

    @After
    public void assertNoActiveCommonPoolThreads() throws Exception {
        assertThat(ForkJoinPool.commonPool().getActiveThreadCount()).isEqualTo(0);
    }

    public void setIndexWriterMaxDocs(int limit) {
        luceneTestCase.setIndexWriterMaxDocs(limit);
    }

    public void restoreIndexWriterMaxDocs() {
        luceneTestCase.restoreIndexWriterMaxDocs();
    }

    public <T extends Closeable> T closeAfterTest(T resource) {
        return luceneTestCase.closeAfterTest(resource);
    }

    public String getTestName() {
        return luceneTestCase.getTestName();
    }

    public void overrideTestDefaultQueryCache() {
        luceneTestCase.overrideTestDefaultQueryCache();
    }

    public void assertReaderEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        luceneTestCase.assertReaderEquals(info, leftReader, rightReader);
    }

    public void assertReaderStatisticsEquals(String info, IndexReader leftReader, IndexReader rightReader)
            throws IOException {
        luceneTestCase.assertReaderStatisticsEquals(info, leftReader, rightReader);
    }

    public void assertTermsEquals(String info, IndexReader leftReader, IndexReader rightReader, boolean deep)
            throws IOException {
        luceneTestCase.assertTermsEquals(info, leftReader, rightReader, deep);
    }

    public void assertTermsEquals(String info, IndexReader leftReader, Terms leftTerms, Terms rightTerms, boolean deep)
            throws IOException {
        luceneTestCase.assertTermsEquals(info, leftReader, leftTerms, rightTerms, deep);
    }

    public void assertTermsStatisticsEquals(String info, Terms leftTerms, Terms rightTerms) throws IOException {
        luceneTestCase.assertTermsStatisticsEquals(info, leftTerms, rightTerms);
    }

    public void assertTermsEnumEquals(String info, IndexReader leftReader, TermsEnum leftTermsEnum,
            TermsEnum rightTermsEnum, boolean deep) throws IOException {
        luceneTestCase.assertTermsEnumEquals(info, leftReader, leftTermsEnum, rightTermsEnum, deep);
    }

    public void assertDocsAndPositionsEnumEquals(String info, PostingsEnum leftDocs, PostingsEnum rightDocs)
            throws IOException {
        luceneTestCase.assertDocsAndPositionsEnumEquals(info, leftDocs, rightDocs);
    }

    public void assertDocsEnumEquals(String info, PostingsEnum leftDocs, PostingsEnum rightDocs, boolean hasFreqs)
            throws IOException {
        luceneTestCase.assertDocsEnumEquals(info, leftDocs, rightDocs, hasFreqs);
    }

    public void assertDocsSkippingEquals(String info, IndexReader leftReader, int docFreq, PostingsEnum leftDocs,
            PostingsEnum rightDocs, boolean hasFreqs) throws IOException {
        luceneTestCase.assertDocsSkippingEquals(info, leftReader, docFreq, leftDocs, rightDocs, hasFreqs);
    }

    public void assertPositionsSkippingEquals(String info, IndexReader leftReader, int docFreq, PostingsEnum leftDocs,
            PostingsEnum rightDocs) throws IOException {
        luceneTestCase.assertPositionsSkippingEquals(info, leftReader, docFreq, leftDocs, rightDocs);
    }

    public void assertTermStatsEquals(String info, TermsEnum leftTermsEnum, TermsEnum rightTermsEnum)
            throws IOException {
        luceneTestCase.assertTermStatsEquals(info, leftTermsEnum, rightTermsEnum);
    }

    public void assertNormsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        luceneTestCase.assertNormsEquals(info, leftReader, rightReader);
    }

    public void assertStoredFieldsEquals(String info, IndexReader leftReader, IndexReader rightReader)
            throws IOException {
        luceneTestCase.assertStoredFieldsEquals(info, leftReader, rightReader);
    }

    public void assertStoredFieldEquals(String info, IndexableField leftField, IndexableField rightField) {
        luceneTestCase.assertStoredFieldEquals(info, leftField, rightField);
    }

    public void assertTermVectorsEquals(String info, IndexReader leftReader, IndexReader rightReader)
            throws IOException {
        luceneTestCase.assertTermVectorsEquals(info, leftReader, rightReader);
    }

    public void assertDocValuesEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        luceneTestCase.assertDocValuesEquals(info, leftReader, rightReader);
    }

    public void assertDocValuesEquals(String info, int num, NumericDocValues leftDocValues,
            NumericDocValues rightDocValues) throws IOException {
        luceneTestCase.assertDocValuesEquals(info, num, leftDocValues, rightDocValues);
    }

    public void assertDeletedDocsEquals(String info, IndexReader leftReader, IndexReader rightReader)
            throws IOException {
        luceneTestCase.assertDeletedDocsEquals(info, leftReader, rightReader);
    }

    public void assertFieldInfosEquals(String info, IndexReader leftReader, IndexReader rightReader)
            throws IOException {
        luceneTestCase.assertFieldInfosEquals(info, leftReader, rightReader);
    }

    public void assertPointsEquals(String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        luceneTestCase.assertPointsEquals(info, leftReader, rightReader);
    }

    /**
     * Access to the current {@link RandomizedContext}'s Random instance. It is safe
     * to use this
     * method from multiple threads, etc., but it should be called while within a
     * runner's scope (so
     * no static initializers). The returned {@link Random} instance will be
     * <b>different</b> when
     * this method is called inside a {@link BeforeClass} hook (static suite scope)
     * and within {@link
     * Before}/ {@link After} hooks or test methods.
     *
     * <p>
     * The returned instance must not be shared with other threads or cross a single
     * scope's
     * boundary. For example, a {@link Random} acquired within a test method
     * shouldn't be reused for
     * another test case.
     *
     * <p>
     * There is an overhead connected with getting the {@link Random} for a
     * particular context and
     * thread. It is better to cache the {@link Random} locally if tight loops with
     * multiple
     * invocations are present or create a derivative local {@link Random} for
     * millions of calls like
     * this:
     *
     * <pre>
     * Random random = new Random(random().nextLong());
     * // tight loop with many invocations.
     * </pre>
     */
    public static Random random() {
        return RandomizedContext.current().getRandom();
    }

    /**
     * Return a random Locale from the available locales on the system.
     *
     * @see <a href=
     *      "http://issues.apache.org/jira/browse/LUCENE-4020">LUCENE-4020</a>
     */
    public static Locale randomLocale(Random random) {
        return LuceneTestCase.randomLocale(random);
    }

    /**
     * Return a random TimeZone from the available timezones on the system
     *
     * @see <a href=
     *      "http://issues.apache.org/jira/browse/LUCENE-4020">LUCENE-4020</a>
     */
    public static TimeZone randomTimeZone(Random random) {
        return LuceneTestCase.randomTimeZone(random);
    }

    /** return a Locale object equivalent to its programmatic name */
    public static Locale localeForLanguageTag(String languageTag) {
        return LuceneTestCase.localeForLanguageTag(languageTag);
    }

    /**
     * Returns a new Directory instance. Use this when the test does not care about
     * the specific
     * Directory implementation (most tests).
     *
     * <p>
     * The Directory is wrapped with {@link BaseDirectoryWrapper}. this means
     * usually it will be
     * picky, such as ensuring that you properly close it and all open files in your
     * test. It will
     * emulate some features of Windows, such as not allowing open files to be
     * overwritten.
     */
    public static BaseDirectoryWrapper newDirectory() {
        return newDirectory(random());
    }

    /**
     * Returns a new Directory instance, using the specified random. See
     * {@link #newDirectory()} for
     * more information.
     */
    public static BaseDirectoryWrapper newDirectory(Random r) {
        return LuceneTestCase.newDirectory(r);
    }

    /**
     * Returns a new FSDirectory instance over the given file, which must be a
     * folder.
     */
    public static BaseDirectoryWrapper newFSDirectory(Path f) {
        return LuceneTestCase.newFSDirectory(f);
    }

    public static MockDirectoryWrapper newMockDirectory() {
        return LuceneTestCase.newMockDirectory();
    }

    public static MockDirectoryWrapper newMockFSDirectory(Path f) {
        return LuceneTestCase.newMockFSDirectory(f);
    }

    public static LeafReader getOnlyLeafReader(IndexReader reader) {
        return LuceneTestCase.getOnlyLeafReader(reader);
    }

    /**
     * Creates an empty, temporary folder (when the name of the folder is of no
     * importance).
     *
     * @see #createTempDir(String)
     */
    public static Path createTempDir() {
        return LuceneTestCase.createTempDir();
    }

    /**
     * Creates an empty, temporary folder with the given name prefix.
     *
     * <p>
     * The folder will be automatically removed after the test class completes
     * successfully. The
     * test should close any file handles that would prevent the folder from being
     * removed.
     */
    public static Path createTempDir(String prefix) {
        return LuceneTestCase.createTempDir(prefix);
    }

    public static MergePolicy newMergePolicy() {
        return LuceneTestCase.newMergePolicy();
    }

    public static LogMergePolicy newLogMergePolicy() {
        return LuceneTestCase.newLogMergePolicy();
    }

    /**
     * create a new index writer config with random defaults using the specified
     * random
     */
    public static IndexWriterConfig newIndexWriterConfig(Random r, Analyzer a) {
        return LuceneTestCase.newIndexWriterConfig(r, a);
    }

    /** create a new index writer config with random defaults */
    public static IndexWriterConfig newIndexWriterConfig() {
        return LuceneTestCase.newIndexWriterConfig();
    }

    /**
     * Returns true if something should happen rarely,
     *
     * <p>
     * The actual number returned will be influenced by whether
     * {@link #TEST_NIGHTLY} is active and
     * {@link #RANDOM_MULTIPLIER}.
     */
    public static boolean rarely(Random random) {
        return LuceneTestCase.rarely(random);
    }

    /**
     * Returns a number of at least <code>i</code>
     *
     * <p>
     * The actual number returned will be influenced by whether
     * {@link #TEST_NIGHTLY} is active and
     * {@link #RANDOM_MULTIPLIER}, but also with some random fudge.
     */
    public static int atLeast(Random random, int i) {
        return LuceneTestCase.atLeast(random, i);
    }

    public static int atLeast(int i) {
        return atLeast(random(), i);
    }

    public static boolean rarely() {
        return rarely(random());
    }

    public static boolean usually(Random random) {
        return !rarely(random);
    }

    public static boolean usually() {
        return usually(random());
    }

    /**
     * Registers a {@link Closeable} resource that should be closed after the suite
     * completes.
     *
     * @return <code>resource</code> (for call chaining).
     */
    public static <T extends Closeable> T closeAfterSuite(T resource) {
        return LuceneTestCase.closeAfterSuite(resource);
    }

    /** Return the current class being tested. */
    public static Class<?> getTestClass() {
        return LuceneTestCase.getTestClass();
    }

    public static void assumeTrue(String msg, boolean condition) {
        RandomizedTest.assumeTrue(msg, condition);
    }

    /**
     * Creates an empty temporary file.
     *
     * @see #createTempFile(String, String)
     */
    public static Path createTempFile() throws IOException {
        return createTempFile("tempFile", ".tmp");
    }

    /**
     * Creates an empty file with the given prefix and suffix.
     *
     * <p>
     * The file will be automatically removed after the test class completes
     * successfully. The test
     * should close any file handles that would prevent the folder from being
     * removed.
     */
    public static Path createTempFile(String prefix, String suffix) throws IOException {
        return LuceneTestCase.createTempFile(prefix, suffix);
    }

    public static Field newField(String name, String value, FieldType type) {
        return LuceneTestCase.newField(name, value, type);
    }

    public static Field newTextField(String name, String value, Store stored) {
        return LuceneTestCase.newTextField(name, value, stored);
    }
}
