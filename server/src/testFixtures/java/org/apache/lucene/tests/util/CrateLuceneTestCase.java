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

package org.apache.lucene.tests.util;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsInt;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.StackWalker.StackFrame;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LiveIndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MultiBits;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ParallelCompositeReader;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SimpleMergedSegmentWarmer;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.internal.tests.IndexPackageAccess;
import org.apache.lucene.internal.tests.TestSecrets;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.AlcoholicMergePolicy;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.tests.index.AssertingLeafReader;
import org.apache.lucene.tests.index.FieldFilterLeafReader;
import org.apache.lucene.tests.index.MergingCodecReader;
import org.apache.lucene.tests.index.MergingDirectoryReaderWrapper;
import org.apache.lucene.tests.index.MismatchedDirectoryReader;
import org.apache.lucene.tests.index.MismatchedLeafReader;
import org.apache.lucene.tests.index.MockIndexWriterEventListener;
import org.apache.lucene.tests.index.MockRandomMergePolicy;
import org.apache.lucene.tests.mockfile.VirusCheckingFS;
import org.apache.lucene.tests.search.AssertingIndexSearcher;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.tests.store.RawDirectoryWrapper;
import org.apache.lucene.tests.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CommandLineUtil;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.JUnit4MethodProvider;
import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
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
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;

import junit.framework.AssertionFailedError;

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
    filters = {QuickPatchThreadsFilter.class})
public abstract class CrateLuceneTestCase {

    // --------------------------------------------------------------------
    // Test groups, system properties and other annotations modifying tests
    // --------------------------------------------------------------------

    public static final String SYSPROP_NIGHTLY = "tests.nightly";
    public static final String SYSPROP_WEEKLY = "tests.weekly";
    public static final String SYSPROP_MONSTER = "tests.monster";
    public static final String SYSPROP_AWAITSFIX = "tests.awaitsfix";
    public static final String SYSPROP_BADAPPLES = "tests.badapples";

    /** @see #ignoreAfterMaxFailures */
    public static final String SYSPROP_MAXFAILURES = "tests.maxfailures";

    /** @see #ignoreAfterMaxFailures */
    public static final String SYSPROP_FAILFAST = "tests.failfast";

    /** Annotation for tests that should only be run during nightly builds. */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_NIGHTLY)
    public @interface Nightly {}

    /** Annotation for tests that should only be run during weekly builds */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_WEEKLY)
    public @interface Weekly {}

    /** Annotation for monster tests that require special setup (e.g. use tons of disk and RAM) */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_MONSTER)
    public @interface Monster {
        String value();
    }

    /** Annotation for tests which exhibit a known issue and are temporarily disabled. */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_AWAITSFIX)
    public @interface AwaitsFix {
        /** Point to JIRA entry. */
        public String bugUrl();
    }

    /**
     * Annotation for tests that fail frequently and are not executed in Jenkins builds to not spam
     * mailing lists with false reports.
     *
     * <p>Tests are turned on for developers by default. If you want to disable them, set:
     *
     * <pre>
     * -Dtests.badapples=false
     * </pre>
     *
     * (or do this through {@code ~./lucene.build.properties}).
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = true, sysProperty = SYSPROP_BADAPPLES)
    public @interface BadApple {
        /** Point to JIRA entry. */
        public String bugUrl();
    }

    /**
     * Annotation for test classes that should avoid certain codec types (because they are expensive,
     * for example).
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressCodecs {
        String[] value();
    }

    /**
     * Annotation for test classes that should avoid mock filesystem types (because they test a bug
     * that only happens on linux, for example).
     *
     * <p>You can avoid specific names {@link Class#getSimpleName()} or use the special value <code>*
     * </code> to disable all mock filesystems.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressFileSystems {
        String[] value();
    }

    /**
     * Annotation for test classes that should avoid always omit actual fsync calls from reaching the
     * filesystem.
     *
     * <p>This can be useful, e.g. if they make many lucene commits.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressFsync {}

    /**
     * Marks any suites which are known not to close all the temporary files. This may prevent temp.
     * files and folders from being cleaned up after the suite is completed.
     *
     * @see CrateLuceneTestCase#createTempDir()
     * @see CrateLuceneTestCase#createTempFile(String, String)
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressTempFileChecks {
        /** Point to JIRA entry. */
        public String bugUrl() default "None";
    }

    /**
     * Ignore {@link TestRuleLimitSysouts} for any suite which is known to print over the default
     * limit of bytes to {@link System#out} or {@link System#err}.
     *
     * @see TestRuleLimitSysouts
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressSysoutChecks {
        /** Point to JIRA entry. */
        public String bugUrl();
    }

    /**
     * Suppress the default {@code reproduce with: ant test...} Your own listener can be added as
     * needed for your build.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SuppressReproduceLine {}

    // -----------------------------------------------------------------
    // Truly immutable fields and constants, initialized once and valid
    // for all suites ever since.
    // -----------------------------------------------------------------

    /**
     * True if and only if tests are run in verbose mode. If this flag is false tests are not expected
     * to print any messages. Enforced with {@link TestRuleLimitSysouts}.
     */
    public static final boolean VERBOSE = systemPropertyAsBoolean("tests.verbose", false);

    /** Enables or disables dumping of {@link InfoStream} messages. */
    public static final boolean INFOSTREAM = systemPropertyAsBoolean("tests.infostream", VERBOSE);

    public static final boolean TEST_ASSERTS_ENABLED = systemPropertyAsBoolean("tests.asserts", true);

    /**
     * The default (embedded resource) lines file.
     *
     * @see #TEST_LINE_DOCS_FILE
     */
    public static final String DEFAULT_LINE_DOCS_FILE = "europarl.lines.txt.gz";

    /**
     * Random sample from enwiki used in tests. See {@code help/tests.txt}. gradle task downloading
     * this data set: {@code gradlew getEnWikiRandomLines}.
     */
    public static final String JENKINS_LARGE_LINE_DOCS_FILE = "enwiki.random.lines.txt";

    /** Gets the codec to run tests with. */
    public static final String TEST_CODEC = System.getProperty("tests.codec", "random");

    /** Gets the postingsFormat to run tests with. */
    public static final String TEST_POSTINGSFORMAT =
        System.getProperty("tests.postingsformat", "random");

    /** Gets the docValuesFormat to run tests with */
    public static final String TEST_DOCVALUESFORMAT =
        System.getProperty("tests.docvaluesformat", "random");

    /** Gets the directory to run tests with */
    public static final String TEST_DIRECTORY = System.getProperty("tests.directory", "random");

    /** The line file used in tests (by {@link LineFileDocs}). */
    public static final String TEST_LINE_DOCS_FILE =
        System.getProperty("tests.linedocsfile", DEFAULT_LINE_DOCS_FILE);

    /** Whether or not {@link Nightly} tests should run. */
    public static final boolean TEST_NIGHTLY =
        systemPropertyAsBoolean(
            SYSPROP_NIGHTLY, Nightly.class.getAnnotation(TestGroup.class).enabled());

    /** Whether or not {@link Weekly} tests should run. */
    public static final boolean TEST_WEEKLY =
        systemPropertyAsBoolean(
            SYSPROP_WEEKLY, Weekly.class.getAnnotation(TestGroup.class).enabled());

    /** Whether or not {@link Monster} tests should run. */
    public static final boolean TEST_MONSTER =
        systemPropertyAsBoolean(
            SYSPROP_MONSTER, Monster.class.getAnnotation(TestGroup.class).enabled());

    /** Whether or not {@link AwaitsFix} tests should run. */
    public static final boolean TEST_AWAITSFIX =
        systemPropertyAsBoolean(
            SYSPROP_AWAITSFIX, AwaitsFix.class.getAnnotation(TestGroup.class).enabled());

    /** Whether or not {@link BadApple} tests should run. */
    public static final boolean TEST_BADAPPLES =
        systemPropertyAsBoolean(
            SYSPROP_BADAPPLES, BadApple.class.getAnnotation(TestGroup.class).enabled());

    /** Throttling, see {@link MockDirectoryWrapper#setThrottling(Throttling)}. */
    public static final Throttling TEST_THROTTLING =
        TEST_NIGHTLY ? Throttling.SOMETIMES : Throttling.NEVER;

    /**
     * A random multiplier which you should use when writing random tests: multiply it by the number
     * of iterations to scale your tests (for nightly builds).
     */
    public static final int RANDOM_MULTIPLIER =
        systemPropertyAsInt("tests.multiplier", TEST_NIGHTLY ? 2 : 1);

    /** Leave temporary files on disk, even on successful runs. */
    public static final boolean LEAVE_TEMPORARY;

    static {
        boolean defaultValue = false;
        for (String property :
            Arrays.asList(
                "tests.leaveTemporary" /* ANT tasks's (junit4) flag. */,
                "tests.leavetemporary" /* lowercase */,
                "tests.leavetmpdir" /* default */,
                "solr.test.leavetmpdir" /* Solr's legacy */)) {
            defaultValue |= systemPropertyAsBoolean(property, false);
        }
        LEAVE_TEMPORARY = defaultValue;
    }

    /**
     * Returns true, if MMapDirectory supports unmapping on this platform (required for Windows), or
     * if we are not on Windows.
     */
    public static boolean hasWorkingMMapOnWindows() {
        return !Constants.WINDOWS || MMapDirectory.UNMAP_SUPPORTED;
    }

    /**
     * Assumes that the current MMapDirectory implementation supports unmapping, so the test will not
     * fail on Windows.
     *
     * @see #hasWorkingMMapOnWindows()
     */
    public static void assumeWorkingMMapOnWindows() {
        assumeTrue(MMapDirectory.UNMAP_NOT_SUPPORTED_REASON, hasWorkingMMapOnWindows());
    }

    /** Filesystem-based {@link Directory} implementations. */
    private static final List<String> FS_DIRECTORIES =
        Arrays.asList(
            "NIOFSDirectory",
            // NIOFSDirectory as replacement for MMapDirectory if unmapping is not supported on
            // Windows (to make randomization stable):
            hasWorkingMMapOnWindows() ? "MMapDirectory" : "NIOFSDirectory");

    /** All {@link Directory} implementations. */
    private static final List<String> CORE_DIRECTORIES;

    static {
        CORE_DIRECTORIES = new ArrayList<>(FS_DIRECTORIES);
        CORE_DIRECTORIES.add(ByteBuffersDirectory.class.getSimpleName());
    }

    /** A {@link org.apache.lucene.search.QueryCachingPolicy} that randomly caches. */
    public static final QueryCachingPolicy MAYBE_CACHE_POLICY =
        new QueryCachingPolicy() {

            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) throws IOException {
                return random().nextBoolean();
            }
        };

    // -----------------------------------------------------------------
    // Fields initialized in class or instance rules.
    // -----------------------------------------------------------------

    // -----------------------------------------------------------------
    // Class level (suite) rules.
    // -----------------------------------------------------------------

    /** Stores the currently class under test. */
    private static final TestRuleStoreClassName classNameRule;

    /** Class environment setup rule. */
    static final CrateTestRuleSetupAndRestoreClassEnv classEnvRule;

    /** Suite failure marker (any error in the test or suite scope). */
    protected static TestRuleMarkFailure suiteFailureMarker;

    /** Temporary files cleanup rule. */
    private static CrateTestRuleTemporaryFilesCleanup tempFilesCleanupRule;

    /**
     * Ignore tests after hitting a designated number of initial failures. This is truly a "static"
     * global singleton since it needs to span the lifetime of all test classes running inside this
     * JVM (it cannot be part of a class rule).
     *
     * <p>This poses some problems for the test framework's tests because these sometimes trigger
     * intentional failures which add up to the global count. This field contains a (possibly)
     * changing reference to {@link TestRuleIgnoreAfterMaxFailures} and we dispatch to its current
     * value from the {@link #classRules} chain using {@link TestRuleDelegate}.
     */
    private static final AtomicReference<TestRuleIgnoreAfterMaxFailures>
        ignoreAfterMaxFailuresDelegate;

    private static final TestRule ignoreAfterMaxFailures;

    static {
        int maxFailures = systemPropertyAsInt(SYSPROP_MAXFAILURES, Integer.MAX_VALUE);
        boolean failFast = systemPropertyAsBoolean(SYSPROP_FAILFAST, false);

        if (failFast) {
            if (maxFailures == Integer.MAX_VALUE) {
                maxFailures = 1;
            } else {
                System.err.println(
                    "Property '"
                    + SYSPROP_MAXFAILURES
                    + "'="
                    + maxFailures
                    + ", 'failfast' is"
                    + " ignored.");
            }
        }

        ignoreAfterMaxFailuresDelegate =
            new AtomicReference<>(new TestRuleIgnoreAfterMaxFailures(maxFailures));
        ignoreAfterMaxFailures = TestRuleDelegate.of(ignoreAfterMaxFailuresDelegate);
    }

    /**
     * Try to capture streams early so that other classes don't have a chance to steal references to
     * them (as is the case with ju.logging handlers).
     */
    static {
        TestRuleLimitSysouts.checkCaptureStreams();
    }

    /**
     * Temporarily substitute the global {@link TestRuleIgnoreAfterMaxFailures}. See {@link
     * #ignoreAfterMaxFailuresDelegate} for some explanation why this method is needed.
     */
    public static TestRuleIgnoreAfterMaxFailures replaceMaxFailureRule(
        TestRuleIgnoreAfterMaxFailures newValue) {
        return ignoreAfterMaxFailuresDelegate.getAndSet(newValue);
    }

    /**
     * This controls how suite-level rules are nested. It is important that _all_ rules declared in
     * {@link CrateLuceneTestCase} are executed in proper order if they depend on each other.
     */
    @ClassRule public static TestRule classRules;

    static {
        RuleChain r =
            RuleChain.outerRule(new TestRuleIgnoreTestSuites())
                .around(ignoreAfterMaxFailures)
                .around(suiteFailureMarker = new TestRuleMarkFailure())
                .around(
                    new VerifyTestClassNamingConvention(
                        "org.apache.lucene", Pattern.compile("(.+\\.)(Test)([^.]+)")))
                .around(new TestRuleAssertionsRequired())
                .around(new TestRuleLimitSysouts(suiteFailureMarker))
                .around(tempFilesCleanupRule = new CrateTestRuleTemporaryFilesCleanup(suiteFailureMarker));
        classRules =
            r.around(new NoClassHooksShadowingRule())
                .around(
                    new NoInstanceHooksOverridesRule() {
                        @Override
                        protected boolean verify(Method key) {
                            String name = key.getName();
                            return !(name.equals("setUp") || name.equals("tearDown"));
                        }
                    })
                .around(classNameRule = new TestRuleStoreClassName())
                .around(
                    new TestRuleRestoreSystemProperties(
                        // Enlist all properties to which we have write access (security manager);
                        // these should be restored to previous state, no matter what the outcome of the
                        // test.

                        // We reset the default locale and timezone; these properties change as a
                        // side-effect
                        "user.language",
                        "user.timezone",

                        // TODO: these should, ideally, be moved to Solr's base class.
                        "solr.directoryFactory",
                        "solr.solr.home",
                        "solr.data.dir"))
                .around(classEnvRule = new CrateTestRuleSetupAndRestoreClassEnv());
    }

    // -----------------------------------------------------------------
    // Test level rules.
    // -----------------------------------------------------------------

    /** Enforces {@link #setUp()} and {@link #tearDown()} calls are chained. */
    private TestRuleSetupTeardownChained parentChainCallRule = new TestRuleSetupTeardownChained();

    /** Save test thread and name. */
    private TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

    /** Taint suite result with individual test failures. */
    private TestRuleMarkFailure testFailureMarker = new TestRuleMarkFailure(suiteFailureMarker);

    /**
     * This controls how individual test rules are nested. It is important that _all_ rules declared
     * in {@link CrateLuceneTestCase} are executed in proper order if they depend on each other.
     */
    @Rule
    public final TestRule ruleChain =
        RuleChain.outerRule(testFailureMarker)
            .around(ignoreAfterMaxFailures)
            .around(threadAndTestNameRule)
            .around(new TestRuleSetupAndRestoreInstanceEnv())
            .around(parentChainCallRule);

    private static final Map<String, FieldType> fieldToType = new HashMap<String, FieldType>();

    enum LiveIWCFlushMode {
        BY_RAM,
        BY_DOCS,
        EITHER
    };

    /** Set by CrateTestRuleSetupAndRestoreClassEnv */
    static LiveIWCFlushMode liveIWCFlushMode;

    static void setLiveIWCFlushMode(LiveIWCFlushMode flushMode) {
        liveIWCFlushMode = flushMode;
    }

    // -----------------------------------------------------------------
    // Suite and test case setup/ cleanup.
    // -----------------------------------------------------------------

    /** For subclasses to override. Overrides must call {@code super.setUp()}. */
    @Before
    public void setUp() throws Exception {
        parentChainCallRule.setupCalled = true;
    }

    /** For subclasses to override. Overrides must call {@code super.tearDown()}. */
    @After
    public void tearDown() throws Exception {
        parentChainCallRule.teardownCalled = true;
        fieldToType.clear();

        // Test is supposed to call this itself, but we do this defensively in case it forgot:
        restoreIndexWriterMaxDocs();
    }

    /**
     * Tells {@link IndexWriter} to enforce the specified limit as the maximum number of documents in
     * one index; call {@link #restoreIndexWriterMaxDocs} once your test is done.
     */
    public void setIndexWriterMaxDocs(int limit) {
        INDEX_PACKAGE_ACCESS.setIndexWriterMaxDocs(limit);
    }

    /** Returns to the default {@link IndexWriter#MAX_DOCS} limit. */
    public void restoreIndexWriterMaxDocs() {
        INDEX_PACKAGE_ACCESS.setIndexWriterMaxDocs(IndexWriter.MAX_DOCS);
    }

    private static final IndexPackageAccess INDEX_PACKAGE_ACCESS =
        TestSecrets.getIndexPackageAccess();

    // -----------------------------------------------------------------
    // Test facilities and facades for subclasses.
    // -----------------------------------------------------------------

    /**
     * Access to the current {@link RandomizedContext}'s Random instance. It is safe to use this
     * method from multiple threads, etc., but it should be called while within a runner's scope (so
     * no static initializers). The returned {@link Random} instance will be <b>different</b> when
     * this method is called inside a {@link BeforeClass} hook (static suite scope) and within {@link
     * Before}/ {@link After} hooks or test methods.
     *
     * <p>The returned instance must not be shared with other threads or cross a single scope's
     * boundary. For example, a {@link Random} acquired within a test method shouldn't be reused for
     * another test case.
     *
     * <p>There is an overhead connected with getting the {@link Random} for a particular context and
     * thread. It is better to cache the {@link Random} locally if tight loops with multiple
     * invocations are present or create a derivative local {@link Random} for millions of calls like
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
     * Registers a {@link Closeable} resource that should be closed after the test completes.
     *
     * @return <code>resource</code> (for call chaining).
     */
    public <T extends Closeable> T closeAfterTest(T resource) {
        return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.TEST);
    }

    /**
     * Registers a {@link Closeable} resource that should be closed after the suite completes.
     *
     * @return <code>resource</code> (for call chaining).
     */
    public static <T extends Closeable> T closeAfterSuite(T resource) {
        return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.SUITE);
    }

    /** Return the current class being tested. */
    public static Class<?> getTestClass() {
        return classNameRule.getTestClass();
    }

    /** Return the name of the currently executing test case. */
    public String getTestName() {
        return threadAndTestNameRule.testMethodName;
    }

    /**
     * Some tests expect the directory to contain a single segment, and want to do tests on that
     * segment's reader. This is an utility method to help them.
     */
  /*
  public static SegmentReader getOnlySegmentReader(DirectoryReader reader) {
    List<LeafReaderContext> subReaders = reader.leaves();
    if (subReaders.size() != 1) {
      throw new IllegalArgumentException(reader + " has " + subReaders.size() + " segments instead of exactly one");
    }
    final LeafReader r = subReaders.get(0).reader();
    assertTrue("expected a SegmentReader but got " + r, r instanceof SegmentReader);
    return (SegmentReader) r;
  }
    */

    /**
     * Some tests expect the directory to contain a single segment, and want to do tests on that
     * segment's reader. This is an utility method to help them.
     */
    public static LeafReader getOnlyLeafReader(IndexReader reader) {
        List<LeafReaderContext> subReaders = reader.leaves();
        if (subReaders.size() != 1) {
            throw new IllegalArgumentException(
                reader + " has " + subReaders.size() + " segments instead of exactly one");
        }
        return subReaders.get(0).reader();
    }

    /**
     * Returns true if and only if the calling thread is the primary thread executing the test case.
     */
    protected boolean isTestThread() {
        assertNotNull("Test case thread not set?", threadAndTestNameRule.testCaseThread);
        return Thread.currentThread() == threadAndTestNameRule.testCaseThread;
    }

    /**
     * Returns a number of at least <code>i</code>
     *
     * <p>The actual number returned will be influenced by whether {@link #TEST_NIGHTLY} is active and
     * {@link #RANDOM_MULTIPLIER}, but also with some random fudge.
     */
    public static int atLeast(Random random, int i) {
        int min = i * RANDOM_MULTIPLIER;
        int max = min + (min / 2);
        return TestUtil.nextInt(random, min, max);
    }

    public static int atLeast(int i) {
        return atLeast(random(), i);
    }

    /**
     * Returns true if something should happen rarely,
     *
     * <p>The actual number returned will be influenced by whether {@link #TEST_NIGHTLY} is active and
     * {@link #RANDOM_MULTIPLIER}.
     */
    public static boolean rarely(Random random) {
        int p = TEST_NIGHTLY ? 5 : 1;
        p += (p * Math.log(RANDOM_MULTIPLIER));
        int min = 100 - Math.min(p, 20); // never more than 20
        return random.nextInt(100) >= min;
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

    public static void assumeTrue(String msg, boolean condition) {
        RandomizedTest.assumeTrue(msg, condition);
    }

    public static void assumeFalse(String msg, boolean condition) {
        RandomizedTest.assumeFalse(msg, condition);
    }

    public static void assumeNoException(String msg, Exception e) {
        RandomizedTest.assumeNoException(msg, e);
    }

    /**
     * Return <code>args</code> as a {@link Set} instance. The order of elements is not preserved in
     * iterators.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> Set<T> asSet(T... args) {
        return new HashSet<>(Arrays.asList(args));
    }

    /**
     * Convenience method for logging an iterator.
     *
     * @param label String logged before/after the items in the iterator
     * @param iter Each next() is toString()ed and logged on its own line. If iter is null this is
     *     logged differently then an empty iterator.
     * @param stream Stream to log messages to.
     */
    public static void dumpIterator(String label, Iterator<?> iter, PrintStream stream) {
        stream.println("*** BEGIN " + label + " ***");
        if (null == iter) {
            stream.println(" ... NULL ...");
        } else {
            while (iter.hasNext()) {
                stream.println(iter.next().toString());
            }
        }
        stream.println("*** END " + label + " ***");
    }

    /**
     * Convenience method for logging an array. Wraps the array in an iterator and delegates
     *
     * @see #dumpIterator(String,Iterator,PrintStream)
     */
    public static void dumpArray(String label, Object[] objs, PrintStream stream) {
        Iterator<?> iter = (null == objs) ? null : Arrays.asList(objs).iterator();
        dumpIterator(label, iter, stream);
    }

    /** create a new index writer config with random defaults */
    public static IndexWriterConfig newIndexWriterConfig() {
        return newIndexWriterConfig(new MockAnalyzer(random()));
    }

    /** create a new index writer config with random defaults */
    public static IndexWriterConfig newIndexWriterConfig(Analyzer a) {
        return newIndexWriterConfig(random(), a);
    }

    /** create a new index writer config with random defaults using the specified random */
    public static IndexWriterConfig newIndexWriterConfig(Random r, Analyzer a) {
        IndexWriterConfig c = new IndexWriterConfig(a);
        c.setSimilarity(classEnvRule.similarity);
        if (VERBOSE) {
            // Even though CrateTestRuleSetupAndRestoreClassEnv calls
            // InfoStream.setDefault, we do it again here so that
            // the PrintStreamInfoStream.messageID increments so
            // that when there are separate instances of
            // IndexWriter created we see "IW 0", "IW 1", "IW 2",
            // ... instead of just always "IW 0":
            c.setInfoStream(
                new CrateTestRuleSetupAndRestoreClassEnv.ThreadNameFixingPrintStreamInfoStream(System.out));
        }

        if (rarely(r)) {
            c.setMergeScheduler(new SerialMergeScheduler());
        } else if (rarely(r)) {
            ConcurrentMergeScheduler cms;
            if (r.nextBoolean()) {
                cms = new ConcurrentMergeScheduler();
            } else {
                cms =
                    new ConcurrentMergeScheduler() {
                        @Override
                        protected synchronized boolean maybeStall(MergeSource mergeSource) {
                            return true;
                        }
                    };
            }
            int maxThreadCount = TestUtil.nextInt(r, 1, 4);
            int maxMergeCount = TestUtil.nextInt(r, maxThreadCount, maxThreadCount + 4);
            cms.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
            if (random().nextBoolean()) {
                cms.disableAutoIOThrottle();
                assertFalse(cms.getAutoIOThrottle());
            }
            cms.setForceMergeMBPerSec(10 + 10 * random().nextDouble());
            c.setMergeScheduler(cms);
        } else {
            // Always use consistent settings, else CMS's dynamic (SSD or not)
            // defaults can change, hurting reproducibility:
            ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();

            // Only 1 thread can run at once (should maybe help reproducibility),
            // with up to 3 pending merges before segment-producing threads are
            // stalled:
            cms.setMaxMergesAndThreads(3, 1);
            c.setMergeScheduler(cms);
        }

        if (r.nextBoolean()) {
            if (rarely(r)) {
                // crazy value
                c.setMaxBufferedDocs(TestUtil.nextInt(r, 2, 15));
            } else {
                // reasonable value
                c.setMaxBufferedDocs(TestUtil.nextInt(r, 16, 1000));
            }
        }

        c.setMergePolicy(newMergePolicy(r));

        if (rarely(r)) {
            c.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(c.getInfoStream()));
        }
        c.setUseCompoundFile(r.nextBoolean());
        c.setReaderPooling(r.nextBoolean());
        if (rarely(r)) {
            c.setCheckPendingFlushUpdate(false);
        }

        if (rarely(r)) {
            c.setIndexWriterEventListener(new MockIndexWriterEventListener());
        }

        switch (r.nextInt(3)) {
            case 0:
                // Disable merge on refresh
                c.setMaxFullFlushMergeWaitMillis(0L);
                break;
            case 1:
                // Very low timeout, merges will likely not be able to run in time
                c.setMaxFullFlushMergeWaitMillis(1L);
                break;
            default:
                // Very long timeout, merges will almost always be able to run in time
                c.setMaxFullFlushMergeWaitMillis(1000L);
                break;
        }

        c.setMaxFullFlushMergeWaitMillis(rarely() ? atLeast(r, 1000) : atLeast(r, 200));
        return c;
    }

    public static MergePolicy newMergePolicy(Random r) {
        return newMergePolicy(r, true);
    }

    public static MergePolicy newMergePolicy(Random r, boolean includeMockMP) {
        if (includeMockMP && rarely(r)) {
            return new MockRandomMergePolicy(r);
        } else if (r.nextBoolean()) {
            return newTieredMergePolicy(r);
        } else if (rarely(r)) {
            return newAlcoholicMergePolicy(r, classEnvRule.timeZone);
        }
        return newLogMergePolicy(r);
    }

    public static MergePolicy newMergePolicy() {
        return newMergePolicy(random());
    }

    public static LogMergePolicy newLogMergePolicy() {
        return newLogMergePolicy(random());
    }

    public static TieredMergePolicy newTieredMergePolicy() {
        return newTieredMergePolicy(random());
    }

    public static AlcoholicMergePolicy newAlcoholicMergePolicy() {
        return newAlcoholicMergePolicy(random(), classEnvRule.timeZone);
    }

    public static AlcoholicMergePolicy newAlcoholicMergePolicy(Random r, TimeZone tz) {
        return new AlcoholicMergePolicy(tz, new Random(r.nextLong()));
    }

    public static LogMergePolicy newLogMergePolicy(Random r) {
        LogMergePolicy logmp = r.nextBoolean() ? new LogDocMergePolicy() : new LogByteSizeMergePolicy();
        logmp.setCalibrateSizeByDeletes(r.nextBoolean());
        if (rarely(r)) {
            logmp.setMergeFactor(TestUtil.nextInt(r, 2, 9));
        } else {
            logmp.setMergeFactor(TestUtil.nextInt(r, 10, 50));
        }
        configureRandom(r, logmp);
        return logmp;
    }

    private static void configureRandom(Random r, MergePolicy mergePolicy) {
        if (r.nextBoolean()) {
            mergePolicy.setNoCFSRatio(0.1 + r.nextDouble() * 0.8);
        } else {
            mergePolicy.setNoCFSRatio(r.nextBoolean() ? 1.0 : 0.0);
        }

        if (rarely(r)) {
            mergePolicy.setMaxCFSSegmentSizeMB(0.2 + r.nextDouble() * 2.0);
        } else {
            mergePolicy.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
        }
    }

    public static TieredMergePolicy newTieredMergePolicy(Random r) {
        TieredMergePolicy tmp = new TieredMergePolicy();
        if (rarely(r)) {
            tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 2, 9));
        } else {
            tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 10, 50));
        }
        if (rarely(r)) {
            tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
        } else {
            tmp.setMaxMergedSegmentMB(10 + r.nextDouble() * 100);
        }
        tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
        tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
        if (rarely(r)) {
            tmp.setSegmentsPerTier(TestUtil.nextInt(r, 2, 20));
        } else {
            tmp.setSegmentsPerTier(TestUtil.nextInt(r, 10, 50));
        }
        configureRandom(r, tmp);
        tmp.setDeletesPctAllowed(20 + random().nextDouble() * 30);
        return tmp;
    }

    public static MergePolicy newLogMergePolicy(boolean useCFS) {
        MergePolicy logmp = newLogMergePolicy();
        logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
        return logmp;
    }

    public static MergePolicy newLogMergePolicy(boolean useCFS, int mergeFactor) {
        LogMergePolicy logmp = newLogMergePolicy();
        logmp.setNoCFSRatio(useCFS ? 1.0 : 0.0);
        logmp.setMergeFactor(mergeFactor);
        return logmp;
    }

    public static MergePolicy newLogMergePolicy(int mergeFactor) {
        LogMergePolicy logmp = newLogMergePolicy();
        logmp.setMergeFactor(mergeFactor);
        return logmp;
    }

    // if you want it in LiveIndexWriterConfig: it must and will be tested here.
    public static void maybeChangeLiveIndexWriterConfig(Random r, LiveIndexWriterConfig c) {
        boolean didChange = false;

        String previous = c.toString();

        if (rarely(r)) {
            // change flush parameters:
            // this is complicated because the api requires you "invoke setters in a magical order!"
            // LUCENE-5661: workaround for race conditions in the API
            synchronized (c) {
                boolean flushByRAM;
                switch (liveIWCFlushMode) {
                    case BY_RAM:
                        flushByRAM = true;
                        break;
                    case BY_DOCS:
                        flushByRAM = false;
                        break;
                    case EITHER:
                        flushByRAM = r.nextBoolean();
                        break;
                    default:
                        throw new AssertionError();
                }
                if (flushByRAM) {
                    c.setRAMBufferSizeMB(TestUtil.nextInt(r, 1, 10));
                    c.setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH);
                } else {
                    if (rarely(r)) {
                        // crazy value
                        c.setMaxBufferedDocs(TestUtil.nextInt(r, 2, 15));
                    } else {
                        // reasonable value
                        c.setMaxBufferedDocs(TestUtil.nextInt(r, 16, 1000));
                    }
                    c.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
                }
            }
            didChange = true;
        }

        if (rarely(r)) {
            IndexWriter.IndexReaderWarmer curWarmer = c.getMergedSegmentWarmer();
            if (curWarmer == null || curWarmer instanceof SimpleMergedSegmentWarmer) {
                // change warmer parameters
                if (r.nextBoolean()) {
                    c.setMergedSegmentWarmer(new SimpleMergedSegmentWarmer(c.getInfoStream()));
                } else {
                    c.setMergedSegmentWarmer(null);
                }
            }
            didChange = true;
        }

        if (rarely(r)) {
            // change CFS flush parameters
            c.setUseCompoundFile(r.nextBoolean());
            didChange = true;
        }

        if (rarely(r)) {
            // change CMS merge parameters
            MergeScheduler ms = c.getMergeScheduler();
            if (ms instanceof ConcurrentMergeScheduler) {
                ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler) ms;
                int maxThreadCount = TestUtil.nextInt(r, 1, 4);
                int maxMergeCount = TestUtil.nextInt(r, maxThreadCount, maxThreadCount + 4);
                boolean enableAutoIOThrottle = random().nextBoolean();
                if (enableAutoIOThrottle) {
                    cms.enableAutoIOThrottle();
                } else {
                    cms.disableAutoIOThrottle();
                }
                cms.setMaxMergesAndThreads(maxMergeCount, maxThreadCount);
                didChange = true;
            }
        }

        if (rarely(r)) {
            MergePolicy mp = c.getMergePolicy();
            configureRandom(r, mp);
            if (mp instanceof LogMergePolicy) {
                LogMergePolicy logmp = (LogMergePolicy) mp;
                logmp.setCalibrateSizeByDeletes(r.nextBoolean());
                if (rarely(r)) {
                    logmp.setMergeFactor(TestUtil.nextInt(r, 2, 9));
                } else {
                    logmp.setMergeFactor(TestUtil.nextInt(r, 10, 50));
                }
            } else if (mp instanceof TieredMergePolicy) {
                TieredMergePolicy tmp = (TieredMergePolicy) mp;
                if (rarely(r)) {
                    tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 2, 9));
                } else {
                    tmp.setMaxMergeAtOnce(TestUtil.nextInt(r, 10, 50));
                }
                if (rarely(r)) {
                    tmp.setMaxMergedSegmentMB(0.2 + r.nextDouble() * 2.0);
                } else {
                    tmp.setMaxMergedSegmentMB(r.nextDouble() * 100);
                }
                tmp.setFloorSegmentMB(0.2 + r.nextDouble() * 2.0);
                tmp.setForceMergeDeletesPctAllowed(0.0 + r.nextDouble() * 30.0);
                if (rarely(r)) {
                    tmp.setSegmentsPerTier(TestUtil.nextInt(r, 2, 20));
                } else {
                    tmp.setSegmentsPerTier(TestUtil.nextInt(r, 10, 50));
                }
                configureRandom(r, tmp);
                tmp.setDeletesPctAllowed(20 + random().nextDouble() * 30);
            }
            didChange = true;
        }
        if (VERBOSE && didChange) {
            String current = c.toString();
            String[] previousLines = previous.split("\n");
            String[] currentLines = current.split("\n");
            StringBuilder diff = new StringBuilder();

            // this should always be the case, diff each line
            if (previousLines.length == currentLines.length) {
                for (int i = 0; i < previousLines.length; i++) {
                    if (!previousLines[i].equals(currentLines[i])) {
                        diff.append("- ").append(previousLines[i]).append("\n");
                        diff.append("+ ").append(currentLines[i]).append("\n");
                    }
                }
            } else {
                // but just in case of something ridiculous...
                diff.append(current.toString());
            }

            // its possible to be empty, if we "change" a value to what it had before.
            if (diff.length() > 0) {
                System.out.println("NOTE: LuceneTestCase: randomly changed IWC's live settings:");
                System.out.println(diff);
            }
        }
    }

    /**
     * Returns a new Directory instance. Use this when the test does not care about the specific
     * Directory implementation (most tests).
     *
     * <p>The Directory is wrapped with {@link BaseDirectoryWrapper}. this means usually it will be
     * picky, such as ensuring that you properly close it and all open files in your test. It will
     * emulate some features of Windows, such as not allowing open files to be overwritten.
     */
    public static BaseDirectoryWrapper newDirectory() {
        return newDirectory(random());
    }

    /** Like {@link #newDirectory} except randomly the {@link VirusCheckingFS} may be installed */
    public static BaseDirectoryWrapper newMaybeVirusCheckingDirectory() {
        if (random().nextInt(5) == 4) {
            Path path = addVirusChecker(createTempDir());
            return newFSDirectory(path);
        } else {
            return newDirectory(random());
        }
    }

    /**
     * Returns a new Directory instance, using the specified random. See {@link #newDirectory()} for
     * more information.
     */
    public static BaseDirectoryWrapper newDirectory(Random r) {
        return wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), rarely(r), false);
    }

    /**
     * Returns a new Directory instance, using the specified random. See {@link #newDirectory()} for
     * more information.
     */
    public static BaseDirectoryWrapper newDirectory(Random r, LockFactory lf) {
        return wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY, lf), rarely(r), false);
    }

    public static MockDirectoryWrapper newMockDirectory() {
        return newMockDirectory(random());
    }

    public static MockDirectoryWrapper newMockDirectory(Random r) {
        return (MockDirectoryWrapper)
            wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY), false, false);
    }

    public static MockDirectoryWrapper newMockDirectory(Random r, LockFactory lf) {
        return (MockDirectoryWrapper)
            wrapDirectory(r, newDirectoryImpl(r, TEST_DIRECTORY, lf), false, false);
    }

    public static MockDirectoryWrapper newMockFSDirectory(Path f) {
        return (MockDirectoryWrapper) newFSDirectory(f, FSLockFactory.getDefault(), false);
    }

    public static MockDirectoryWrapper newMockFSDirectory(Path f, LockFactory lf) {
        return (MockDirectoryWrapper) newFSDirectory(f, lf, false);
    }

    public static Path addVirusChecker(Path path) {
        if (TestUtil.hasVirusChecker(path) == false) {
            VirusCheckingFS fs = new VirusCheckingFS(path.getFileSystem(), random().nextLong());
            path = fs.wrapPath(path);
        }
        return path;
    }

    /**
     * Returns a new Directory instance, with contents copied from the provided directory. See {@link
     * #newDirectory()} for more information.
     */
    public static BaseDirectoryWrapper newDirectory(Directory d) throws IOException {
        return newDirectory(random(), d);
    }

    /** Returns a new FSDirectory instance over the given file, which must be a folder. */
    public static BaseDirectoryWrapper newFSDirectory(Path f) {
        return newFSDirectory(f, FSLockFactory.getDefault());
    }

    /** Like {@link #newFSDirectory(Path)}, but randomly insert {@link VirusCheckingFS} */
    public static BaseDirectoryWrapper newMaybeVirusCheckingFSDirectory(Path f) {
        if (random().nextInt(5) == 4) {
            f = addVirusChecker(f);
        }
        return newFSDirectory(f, FSLockFactory.getDefault());
    }

    /** Returns a new FSDirectory instance over the given file, which must be a folder. */
    public static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf) {
        return newFSDirectory(f, lf, rarely());
    }

    private static BaseDirectoryWrapper newFSDirectory(Path f, LockFactory lf, boolean bare) {
        String fsdirClass = TEST_DIRECTORY;
        if (fsdirClass.equals("random")) {
            fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES);
        }

        Class<? extends FSDirectory> clazz;
        try {
            try {
                clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
            } catch (
                @SuppressWarnings("unused")
                ClassCastException e) {
                // TEST_DIRECTORY is not a sub-class of FSDirectory, so draw one at random
                fsdirClass = RandomPicks.randomFrom(random(), FS_DIRECTORIES);
                clazz = CommandLineUtil.loadFSDirectoryClass(fsdirClass);
            }

            Directory fsdir = newFSDirectoryImpl(clazz, f, lf);
            BaseDirectoryWrapper wrapped = wrapDirectory(random(), fsdir, bare, true);
            return wrapped;
        } catch (Exception e) {
            Rethrow.rethrow(e);
            throw null; // dummy to prevent compiler failure
        }
    }

    private static Directory newFileSwitchDirectory(Random random, Directory dir1, Directory dir2) {
        List<String> fileExtensions =
            Arrays.asList(
                "fdt", "fdx", "tim", "tip", "si", "fnm", "pos", "dii", "dim", "nvm", "nvd", "dvm",
                "dvd");
        Collections.shuffle(fileExtensions, random);
        fileExtensions = fileExtensions.subList(0, 1 + random.nextInt(fileExtensions.size()));
        return new FileSwitchDirectory(new HashSet<>(fileExtensions), dir1, dir2, true);
    }

    /**
     * Returns a new Directory instance, using the specified random with contents copied from the
     * provided directory. See {@link #newDirectory()} for more information.
     */
    public static BaseDirectoryWrapper newDirectory(Random r, Directory d) throws IOException {
        Directory impl = newDirectoryImpl(r, TEST_DIRECTORY);
        for (String file : d.listAll()) {
            if (file.startsWith(IndexFileNames.SEGMENTS)
                || IndexFileNames.CODEC_FILE_PATTERN.matcher(file).matches()) {
                impl.copyFrom(d, file, file, newIOContext(r));
            }
        }
        return wrapDirectory(r, impl, rarely(r), false);
    }

    private static BaseDirectoryWrapper wrapDirectory(
        Random random, Directory directory, boolean bare, boolean filesystem) {
        // IOContext randomization might make NRTCachingDirectory make bad decisions, so avoid
        // using it if the user requested a filesystem directory.
        if (rarely(random) && !bare && filesystem == false) {
            directory = new NRTCachingDirectory(directory, random.nextDouble(), random.nextDouble());
        }

        if (bare) {
            BaseDirectoryWrapper base = new RawDirectoryWrapper(directory);
            closeAfterSuite(new CloseableDirectory(base, suiteFailureMarker));
            return base;
        } else {
            MockDirectoryWrapper mock = new MockDirectoryWrapper(random, directory);

            mock.setThrottling(TEST_THROTTLING);
            closeAfterSuite(new CloseableDirectory(mock, suiteFailureMarker));
            return mock;
        }
    }

    public static Field newStringField(String name, String value, Store stored) {
        return newField(
            random(),
            name,
            value,
            stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
    }

    public static Field newStringField(String name, BytesRef value, Store stored) {
        return newField(
            random(),
            name,
            value,
            stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
    }

    public static Field newTextField(String name, String value, Store stored) {
        return newField(
            random(),
            name,
            value,
            stored == Store.YES ? TextField.TYPE_STORED : TextField.TYPE_NOT_STORED);
    }

    public static Field newStringField(Random random, String name, String value, Store stored) {
        return newField(
            random,
            name,
            value,
            stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
    }

    public static Field newStringField(Random random, String name, BytesRef value, Store stored) {
        return newField(
            random,
            name,
            value,
            stored == Store.YES ? StringField.TYPE_STORED : StringField.TYPE_NOT_STORED);
    }

    public static Field newTextField(Random random, String name, String value, Store stored) {
        return newField(
            random,
            name,
            value,
            stored == Store.YES ? TextField.TYPE_STORED : TextField.TYPE_NOT_STORED);
    }

    public static Field newField(String name, String value, FieldType type) {
        return newField(random(), name, value, type);
    }

    // TODO: if we can pull out the "make term vector options
    // consistent across all instances of the same field name"
    // write-once schema sort of helper class then we can
    // remove the sync here.  We can also fold the random
    // "enable norms" (now commented out, below) into that:
    public static synchronized Field newField(
        Random random, String name, Object value, FieldType type) {

        // Defeat any consumers that illegally rely on intern'd
        // strings (we removed this from Lucene a while back):
        name = new String(name);

        FieldType prevType = fieldToType.get(name);
        if (prevType != null) {
            // always use the same fieldType for the same field name
            return createField(name, value, prevType);
        }

        // TODO: once all core & test codecs can index
        // offsets, sometimes randomly turn on offsets if we are
        // already indexing positions...

        FieldType newType = new FieldType(type);
        if (!newType.stored() && random.nextBoolean()) {
            newType.setStored(true); // randomly store it
        }
        if (newType.indexOptions() != IndexOptions.NONE) {
            if (!newType.storeTermVectors() && random.nextBoolean()) {
                newType.setStoreTermVectors(true);
                if (!newType.storeTermVectorPositions()) {
                    newType.setStoreTermVectorPositions(random.nextBoolean());
                    if (newType.storeTermVectorPositions()) {
                        if (!newType.storeTermVectorPayloads()) {
                            newType.setStoreTermVectorPayloads(random.nextBoolean());
                        }
                    }
                }
                if (!newType.storeTermVectorOffsets()) {
                    newType.setStoreTermVectorOffsets(random.nextBoolean());
                }

                if (VERBOSE) {
                    System.out.println("NOTE: LuceneTestCase: upgrade name=" + name + " type=" + newType);
                }
            }
        }
        newType.freeze();
        fieldToType.put(name, newType);

        // TODO: we need to do this, but smarter, ie, most of
        // the time we set the same value for a given field but
        // sometimes (rarely) we change it up:
    /*
    if (newType.omitNorms()) {
      newType.setOmitNorms(random.nextBoolean());
    }
    */

        return createField(name, value, newType);
    }

    private static Field createField(String name, Object value, FieldType fieldType) {
        if (value instanceof String) {
            return new Field(name, (String) value, fieldType);
        } else if (value instanceof BytesRef) {
            return new Field(name, (BytesRef) value, fieldType);
        } else {
            throw new IllegalArgumentException("value must be String or BytesRef");
        }
    }

    private static final String[] availableLanguageTags =
        Arrays.stream(Locale.getAvailableLocales())
            .map(Locale::toLanguageTag)
            .sorted()
            .distinct()
            .toArray(String[]::new);

    /**
     * Return a random Locale from the available locales on the system.
     *
     * @see <a href="http://issues.apache.org/jira/browse/LUCENE-4020">LUCENE-4020</a>
     */
    public static Locale randomLocale(Random random) {
        return localeForLanguageTag(
            availableLanguageTags[random.nextInt(availableLanguageTags.length)]);
    }

    /**
     * Return a random TimeZone from the available timezones on the system
     *
     * @see <a href="http://issues.apache.org/jira/browse/LUCENE-4020">LUCENE-4020</a>
     */
    public static TimeZone randomTimeZone(Random random) {
        String[] tzIds = TimeZone.getAvailableIDs();
        return TimeZone.getTimeZone(tzIds[random.nextInt(tzIds.length)]);
    }

    /** return a Locale object equivalent to its programmatic name */
    public static Locale localeForLanguageTag(String languageTag) {
        return new Locale.Builder().setLanguageTag(languageTag).build();
    }

    private static Directory newFSDirectoryImpl(
        Class<? extends FSDirectory> clazz, Path path, LockFactory lf) throws IOException {
        FSDirectory d = null;
        try {
            d = CommandLineUtil.newFSDirectory(clazz, path, lf);
        } catch (ReflectiveOperationException e) {
            Rethrow.rethrow(e);
        }
        return d;
    }

    static Directory newDirectoryImpl(Random random, String clazzName) {
        return newDirectoryImpl(random, clazzName, FSLockFactory.getDefault());
    }

    static Directory newDirectoryImpl(Random random, String clazzName, LockFactory lf) {
        if (clazzName.equals("random")) {
            if (rarely(random)) {
                clazzName = RandomPicks.randomFrom(random, CORE_DIRECTORIES);
            } else if (rarely(random)) {
                String clazzName1 =
                    rarely(random)
                        ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
                        : ByteBuffersDirectory.class.getName();
                String clazzName2 =
                    rarely(random)
                        ? RandomPicks.randomFrom(random, CORE_DIRECTORIES)
                        : ByteBuffersDirectory.class.getName();
                return newFileSwitchDirectory(
                    random,
                    newDirectoryImpl(random, clazzName1, lf),
                    newDirectoryImpl(random, clazzName2, lf));
            } else {
                clazzName = ByteBuffersDirectory.class.getName();
            }
        }

        try {
            final Class<? extends Directory> clazz = CommandLineUtil.loadDirectoryClass(clazzName);
            // If it is a FSDirectory type, try its ctor(Path)
            if (FSDirectory.class.isAssignableFrom(clazz)) {
                final Path dir = createTempDir("index-" + clazzName);
                return newFSDirectoryImpl(clazz.asSubclass(FSDirectory.class), dir, lf);
            }

            // See if it has a Path/LockFactory ctor even though it's not an
            // FSDir subclass:
            try {
                Constructor<? extends Directory> pathCtor =
                    clazz.getConstructor(Path.class, LockFactory.class);
                final Path dir = createTempDir("index");
                return pathCtor.newInstance(dir, lf);
            } catch (
                @SuppressWarnings("unused")
                NoSuchMethodException nsme) {
                // Ignore
            }

            // the remaining dirs are no longer filesystem based, so we must check that the
            // passedLockFactory is not file based:
            if (!(lf instanceof FSLockFactory)) {
                // try ctor with only LockFactory
                try {
                    return clazz.getConstructor(LockFactory.class).newInstance(lf);
                } catch (
                    @SuppressWarnings("unused")
                    NoSuchMethodException nsme) {
                    // Ignore
                }
            }

            // try empty ctor
            return clazz.getConstructor().newInstance();
        } catch (Exception e) {
            Rethrow.rethrow(e);
            throw null; // dummy to prevent compiler failure
        }
    }

    public static IndexReader wrapReader(IndexReader r) throws IOException {
        Random random = random();

        for (int i = 0, c = random.nextInt(6) + 1; i < c; i++) {
            switch (random.nextInt(5)) {
                case 0:
                    // will create no FC insanity in atomic case, as ParallelLeafReader has own cache key:
                    if (VERBOSE) {
                        System.out.println(
                            "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with ParallelLeaf/CompositeReader");
                    }
                    r =
                        (r instanceof LeafReader)
                            ? new ParallelLeafReader((LeafReader) r)
                            : new ParallelCompositeReader((CompositeReader) r);
                    break;
                case 1:
                    if (r instanceof LeafReader) {
                        final LeafReader ar = (LeafReader) r;
                        final List<String> allFields = new ArrayList<>();
                        for (FieldInfo fi : ar.getFieldInfos()) {
                            allFields.add(fi.name);
                        }
                        Collections.shuffle(allFields, random);
                        final int end = allFields.isEmpty() ? 0 : random.nextInt(allFields.size());
                        final Set<String> fields = new HashSet<>(allFields.subList(0, end));
                        // will create no FC insanity as ParallelLeafReader has own cache key:
                        if (VERBOSE) {
                            System.out.println(
                                "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                                + r
                                + " with ParallelLeafReader");
                        }
                        r =
                            new ParallelLeafReader(
                                new FieldFilterLeafReader(ar, fields, false),
                                new FieldFilterLeafReader(ar, fields, true));
                    }
                    break;
                case 2:
                    // Häckidy-Hick-Hack: a standard Reader will cause FC insanity, so we use
                    // QueryUtils' reader with a fake cache key, so insanity checker cannot walk
                    // along our reader:
                    if (VERBOSE) {
                        System.out.println(
                            "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with AssertingLeaf/DirectoryReader");
                    }
                    if (r instanceof LeafReader) {
                        r = new AssertingLeafReader((LeafReader) r);
                    } else if (r instanceof DirectoryReader) {
                        r = new AssertingDirectoryReader((DirectoryReader) r);
                    }
                    break;
                case 3:
                    if (VERBOSE) {
                        System.out.println(
                            "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with MismatchedLeaf/DirectoryReader");
                    }
                    if (r instanceof LeafReader) {
                        r = new MismatchedLeafReader((LeafReader) r, random);
                    } else if (r instanceof DirectoryReader) {
                        r = new MismatchedDirectoryReader((DirectoryReader) r, random);
                    }
                    break;
                case 4:
                    if (VERBOSE) {
                        System.out.println(
                            "NOTE: LuceneTestCase.wrapReader: wrapping previous reader="
                            + r
                            + " with MergingCodecReader");
                    }
                    if (r instanceof CodecReader) {
                        r = new MergingCodecReader((CodecReader) r);
                    } else if (r instanceof DirectoryReader) {
                        boolean allLeavesAreCodecReaders = true;
                        for (LeafReaderContext ctx : r.leaves()) {
                            if (ctx.reader() instanceof CodecReader == false) {
                                allLeavesAreCodecReaders = false;
                                break;
                            }
                        }
                        if (allLeavesAreCodecReaders) {
                            r = new MergingDirectoryReaderWrapper((DirectoryReader) r);
                        }
                    }
                    break;
                default:
                    fail("should not get here");
            }
        }

        if (VERBOSE) {
            System.out.println("wrapReader wrapped: " + r);
        }

        return r;
    }

    /** Sometimes wrap the IndexReader as slow, parallel or filter reader (or combinations of that) */
    public static IndexReader maybeWrapReader(IndexReader r) throws IOException {
        if (rarely()) {
            r = wrapReader(r);
        }
        return r;
    }

    /** TODO: javadoc */
    public static IOContext newIOContext(Random random) {
        return newIOContext(random, IOContext.DEFAULT);
    }

    /** TODO: javadoc */
    public static IOContext newIOContext(Random random, IOContext oldContext) {
        final int randomNumDocs = random.nextInt(4192);
        final int size = random.nextInt(512) * randomNumDocs;
        if (oldContext.flushInfo != null) {
            // Always return at least the estimatedSegmentSize of
            // the incoming IOContext:
            return new IOContext(
                new FlushInfo(randomNumDocs, Math.max(oldContext.flushInfo.estimatedSegmentSize, size)));
        } else if (oldContext.mergeInfo != null) {
            // Always return at least the estimatedMergeBytes of
            // the incoming IOContext:
            return new IOContext(
                new MergeInfo(
                    randomNumDocs,
                    Math.max(oldContext.mergeInfo.estimatedMergeBytes, size),
                    random.nextBoolean(),
                    TestUtil.nextInt(random, 1, 100)));
        } else {
            // Make a totally random IOContext:
            final IOContext context;
            switch (random.nextInt(5)) {
                case 0:
                    context = IOContext.DEFAULT;
                    break;
                case 1:
                    context = IOContext.READ;
                    break;
                case 2:
                    context = IOContext.READONCE;
                    break;
                case 3:
                    context = new IOContext(new MergeInfo(randomNumDocs, size, true, -1));
                    break;
                case 4:
                    context = new IOContext(new FlushInfo(randomNumDocs, size));
                    break;
                default:
                    context = IOContext.DEFAULT;
            }
            return context;
        }
    }

    private static final QueryCache DEFAULT_QUERY_CACHE = IndexSearcher.getDefaultQueryCache();
    private static final QueryCachingPolicy DEFAULT_CACHING_POLICY =
        IndexSearcher.getDefaultQueryCachingPolicy();

    @Before
    public void overrideTestDefaultQueryCache() {
        // Make sure each test method has its own cache
        overrideDefaultQueryCache();
    }

    @BeforeClass
    public static void overrideDefaultQueryCache() {
        // we need to reset the query cache in an @BeforeClass so that tests that
        // instantiate an IndexSearcher in an @BeforeClass method use a fresh new cache
        IndexSearcher.setDefaultQueryCache(
            new LRUQueryCache(10000, 1 << 25, context -> true, Float.POSITIVE_INFINITY));
        IndexSearcher.setDefaultQueryCachingPolicy(MAYBE_CACHE_POLICY);
    }

    @AfterClass
    public static void resetDefaultQueryCache() {
        IndexSearcher.setDefaultQueryCache(DEFAULT_QUERY_CACHE);
        IndexSearcher.setDefaultQueryCachingPolicy(DEFAULT_CACHING_POLICY);
    }

    @BeforeClass
    public static void setupCPUCoreCount() {
        // Randomize core count so CMS varies its dynamic defaults, and this also "fixes" core
        // count from the master seed so it will always be the same on reproduce:
        int numCores = TestUtil.nextInt(random(), 1, 4);
        System.setProperty(
            ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY, Integer.toString(numCores));
    }

    @AfterClass
    public static void restoreCPUCoreCount() {
        System.clearProperty(ConcurrentMergeScheduler.DEFAULT_CPU_CORE_COUNT_PROPERTY);
    }

    /** Create a new searcher over the reader. This searcher might randomly use threads. */
    public static IndexSearcher newSearcher(IndexReader r) {
        return newSearcher(r, true);
    }

    /** Create a new searcher over the reader. This searcher might randomly use threads. */
    public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
        return newSearcher(r, maybeWrap, true);
    }

    /**
     * Create a new searcher over the reader. This searcher might randomly use threads. if <code>
     * maybeWrap</code> is true, this searcher might wrap the reader with one that returns null for
     * getSequentialSubReaders. If <code>wrapWithAssertions</code> is true, this searcher might be an
     * {@link AssertingIndexSearcher} instance.
     */
    public static IndexSearcher newSearcher(
        IndexReader r, boolean maybeWrap, boolean wrapWithAssertions) {
        return newSearcher(r, maybeWrap, wrapWithAssertions, rarely());
    }

    /**
     * Create a new searcher over the reader. If <code>
     * maybeWrap</code> is true, this searcher might wrap the reader with one that returns null for
     * getSequentialSubReaders. If <code>wrapWithAssertions</code> is true, this searcher might be an
     * {@link AssertingIndexSearcher} instance. The searcher will use threads if <code>useThreads
     * </code> is set to true.
     */
    public static IndexSearcher newSearcher(
        IndexReader r, boolean maybeWrap, boolean wrapWithAssertions, boolean useThreads) {
        Random random = random();
        if (useThreads == false) {
            if (maybeWrap) {
                try {
                    r = maybeWrapReader(r);
                } catch (IOException e) {
                    Rethrow.rethrow(e);
                }
            }
            // TODO: this whole check is a coverage hack, we should move it to tests for various
            // filterreaders.
            // ultimately whatever you do will be checkIndex'd at the end anyway.
            if (random.nextInt(500) == 0 && r instanceof LeafReader) {
                // TODO: not useful to check DirectoryReader (redundant with checkindex)
                // but maybe sometimes run this on the other crazy readers maybeWrapReader creates?
                try {
                    TestUtil.checkReader(r);
                } catch (IOException e) {
                    Rethrow.rethrow(e);
                }
            }
            final IndexSearcher ret;
            if (wrapWithAssertions) {
                ret =
                    random.nextBoolean()
                        ? new AssertingIndexSearcher(random, r)
                        : new AssertingIndexSearcher(random, r.getContext());
            } else {
                ret = random.nextBoolean() ? new IndexSearcher(r) : new IndexSearcher(r.getContext());
            }
            ret.setSimilarity(classEnvRule.similarity);
            return ret;
        } else {
            int threads = 0;
            final ThreadPoolExecutor ex;
            if (r.getReaderCacheHelper() == null || random.nextBoolean()) {
                ex = null;
            } else {
                threads = TestUtil.nextInt(random, 1, 8);
                ex =
                    new ThreadPoolExecutor(
                        threads,
                        threads,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<Runnable>(),
                        new NamedThreadFactory("LuceneTestCase"));
                // uncomment to intensify LUCENE-3840
                // ex.prestartAllCoreThreads();
            }
            if (ex != null) {
                if (VERBOSE) {
                    System.out.println(
                        "NOTE: newSearcher using ExecutorService with " + threads + " threads");
                }
                r.getReaderCacheHelper()
                    .addClosedListener(cacheKey -> TestUtil.shutdownExecutorService(ex));
            }
            IndexSearcher ret;
            if (wrapWithAssertions) {
                ret =
                    random.nextBoolean()
                        ? new AssertingIndexSearcher(random, r, ex)
                        : new AssertingIndexSearcher(random, r.getContext(), ex);
            } else if (random.nextBoolean()) {
                int maxDocPerSlice = 1 + random.nextInt(100000);
                int maxSegmentsPerSlice = 1 + random.nextInt(20);
                ret =
                    new IndexSearcher(r, ex) {
                        @Override
                        protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
                            return slices(leaves, maxDocPerSlice, maxSegmentsPerSlice);
                        }
                    };
            } else {
                ret =
                    random.nextBoolean() ? new IndexSearcher(r, ex) : new IndexSearcher(r.getContext(), ex);
            }
            ret.setSimilarity(classEnvRule.similarity);
            ret.setQueryCachingPolicy(MAYBE_CACHE_POLICY);
            if (random().nextBoolean()) {
              ret.setTimeout(
                  new QueryTimeout() {
                    @Override
                    public boolean shouldExit() {
                      return false;
                    }
                  });
            }
            return ret;
        }
    }

    /**
     * Gets a resource from the test's classpath as {@link Path}. This method should only be used, if
     * a real file is needed. To get a stream, code should prefer {@link #getDataInputStream(String)}.
     */
    protected Path getDataPath(String name) throws IOException {
        try {
            return Paths.get(
                IOUtils.requireResourceNonNull(this.getClass().getResource(name), name).toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    /** Gets a resource from the test's classpath as {@link InputStream}. */
    protected InputStream getDataInputStream(String name) throws IOException {
        return IOUtils.requireResourceNonNull(this.getClass().getResourceAsStream(name), name);
    }

    public void assertReaderEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        assertReaderStatisticsEquals(info, leftReader, rightReader);
        assertTermsEquals(info, leftReader, rightReader, true);
        assertNormsEquals(info, leftReader, rightReader);
        assertStoredFieldsEquals(info, leftReader, rightReader);
        assertTermVectorsEquals(info, leftReader, rightReader);
        assertDocValuesEquals(info, leftReader, rightReader);
        assertDeletedDocsEquals(info, leftReader, rightReader);
        assertFieldInfosEquals(info, leftReader, rightReader);
        assertPointsEquals(info, leftReader, rightReader);
    }

    /** checks that reader-level statistics are the same */
    public void assertReaderStatisticsEquals(
        String info, IndexReader leftReader, IndexReader rightReader) throws IOException {
        // Somewhat redundant: we never delete docs
        assertEquals(info, leftReader.maxDoc(), rightReader.maxDoc());
        assertEquals(info, leftReader.numDocs(), rightReader.numDocs());
        assertEquals(info, leftReader.numDeletedDocs(), rightReader.numDeletedDocs());
        assertEquals(info, leftReader.hasDeletions(), rightReader.hasDeletions());
    }

    /** Fields api equivalency */
    public void assertTermsEquals(
        String info, IndexReader leftReader, IndexReader rightReader, boolean deep)
        throws IOException {
        Set<String> leftFields = new HashSet<>(FieldInfos.getIndexedFields(leftReader));
        Set<String> rightFields = new HashSet<>(FieldInfos.getIndexedFields(rightReader));
        assertEquals(info, leftFields, rightFields);

        for (String field : leftFields) {
            assertTermsEquals(
                info,
                leftReader,
                MultiTerms.getTerms(leftReader, field),
                MultiTerms.getTerms(rightReader, field),
                deep);
        }
    }

    /** Terms api equivalency */
    public void assertTermsEquals(
        String info, IndexReader leftReader, Terms leftTerms, Terms rightTerms, boolean deep)
        throws IOException {
        if (leftTerms == null || rightTerms == null) {
            assertNull(info, leftTerms);
            assertNull(info, rightTerms);
            return;
        }
        assertTermsStatisticsEquals(info, leftTerms, rightTerms);
        assertEquals("hasOffsets", leftTerms.hasOffsets(), rightTerms.hasOffsets());
        assertEquals("hasPositions", leftTerms.hasPositions(), rightTerms.hasPositions());
        assertEquals("hasPayloads", leftTerms.hasPayloads(), rightTerms.hasPayloads());

        TermsEnum leftTermsEnum = leftTerms.iterator();
        TermsEnum rightTermsEnum = rightTerms.iterator();
        assertTermsEnumEquals(info, leftReader, leftTermsEnum, rightTermsEnum, true);

        assertTermsSeekingEquals(info, leftTerms, rightTerms);

        if (deep) {
            int numIntersections = atLeast(3);
            for (int i = 0; i < numIntersections; i++) {
                String re = AutomatonTestUtil.randomRegexp(random());
                CompiledAutomaton automaton =
                    new CompiledAutomaton(new RegExp(re, RegExp.NONE).toAutomaton());
                if (automaton.type == CompiledAutomaton.AUTOMATON_TYPE.NORMAL) {
                    // TODO: test start term too
                    TermsEnum leftIntersection = leftTerms.intersect(automaton, null);
                    TermsEnum rightIntersection = rightTerms.intersect(automaton, null);
                    assertTermsEnumEquals(info, leftReader, leftIntersection, rightIntersection, rarely());
                }
            }
        }
    }

    /** checks collection-level statistics on Terms */
    public void assertTermsStatisticsEquals(String info, Terms leftTerms, Terms rightTerms)
        throws IOException {
        assertEquals(info, leftTerms.getDocCount(), rightTerms.getDocCount());
        assertEquals(info, leftTerms.getSumDocFreq(), rightTerms.getSumDocFreq());
        assertEquals(info, leftTerms.getSumTotalTermFreq(), rightTerms.getSumTotalTermFreq());
        if (leftTerms.size() != -1 && rightTerms.size() != -1) {
            assertEquals(info, leftTerms.size(), rightTerms.size());
        }
    }

    /**
     * checks the terms enum sequentially if deep is false, it does a 'shallow' test that doesnt go
     * down to the docsenums
     */
    public void assertTermsEnumEquals(
        String info,
        IndexReader leftReader,
        TermsEnum leftTermsEnum,
        TermsEnum rightTermsEnum,
        boolean deep)
        throws IOException {
        BytesRef term;
        PostingsEnum leftPositions = null;
        PostingsEnum rightPositions = null;
        PostingsEnum leftDocs = null;
        PostingsEnum rightDocs = null;

        while ((term = leftTermsEnum.next()) != null) {
            assertEquals(info, term, rightTermsEnum.next());
            assertTermStatsEquals(info, leftTermsEnum, rightTermsEnum);
            if (deep) {
                assertDocsAndPositionsEnumEquals(
                    info,
                    leftPositions = leftTermsEnum.postings(leftPositions, PostingsEnum.ALL),
                    rightPositions = rightTermsEnum.postings(rightPositions, PostingsEnum.ALL));

                assertPositionsSkippingEquals(
                    info,
                    leftReader,
                    leftTermsEnum.docFreq(),
                    leftPositions = leftTermsEnum.postings(leftPositions, PostingsEnum.ALL),
                    rightPositions = rightTermsEnum.postings(rightPositions, PostingsEnum.ALL));

                // with freqs:
                assertDocsEnumEquals(
                    info,
                    leftDocs = leftTermsEnum.postings(leftDocs),
                    rightDocs = rightTermsEnum.postings(rightDocs),
                    true);

                // w/o freqs:
                assertDocsEnumEquals(
                    info,
                    leftDocs = leftTermsEnum.postings(leftDocs, PostingsEnum.NONE),
                    rightDocs = rightTermsEnum.postings(rightDocs, PostingsEnum.NONE),
                    false);

                // with freqs:
                assertDocsSkippingEquals(
                    info,
                    leftReader,
                    leftTermsEnum.docFreq(),
                    leftDocs = leftTermsEnum.postings(leftDocs),
                    rightDocs = rightTermsEnum.postings(rightDocs),
                    true);

                // w/o freqs:
                assertDocsSkippingEquals(
                    info,
                    leftReader,
                    leftTermsEnum.docFreq(),
                    leftDocs = leftTermsEnum.postings(leftDocs, PostingsEnum.NONE),
                    rightDocs = rightTermsEnum.postings(rightDocs, PostingsEnum.NONE),
                    false);
            }
        }
        assertNull(info, rightTermsEnum.next());
    }

    /** checks docs + freqs + positions + payloads, sequentially */
    public void assertDocsAndPositionsEnumEquals(
        String info, PostingsEnum leftDocs, PostingsEnum rightDocs) throws IOException {
        assertNotNull(leftDocs);
        assertNotNull(rightDocs);
        assertEquals(info, -1, leftDocs.docID());
        assertEquals(info, -1, rightDocs.docID());
        int docid;
        while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            assertEquals(info, docid, rightDocs.nextDoc());
            int freq = leftDocs.freq();
            assertEquals(info, freq, rightDocs.freq());
            for (int i = 0; i < freq; i++) {
                assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
                assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
                assertEquals(info, leftDocs.startOffset(), rightDocs.startOffset());
                assertEquals(info, leftDocs.endOffset(), rightDocs.endOffset());
            }
        }
        assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
    }

    /** checks docs + freqs, sequentially */
    public void assertDocsEnumEquals(
        String info, PostingsEnum leftDocs, PostingsEnum rightDocs, boolean hasFreqs)
        throws IOException {
        if (leftDocs == null) {
            assertNull(rightDocs);
            return;
        }
        assertEquals(info, -1, leftDocs.docID());
        assertEquals(info, -1, rightDocs.docID());
        int docid;
        while ((docid = leftDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            assertEquals(info, docid, rightDocs.nextDoc());
            if (hasFreqs) {
                assertEquals(info, leftDocs.freq(), rightDocs.freq());
            }
        }
        assertEquals(info, DocIdSetIterator.NO_MORE_DOCS, rightDocs.nextDoc());
    }

    /** checks advancing docs */
    public void assertDocsSkippingEquals(
        String info,
        IndexReader leftReader,
        int docFreq,
        PostingsEnum leftDocs,
        PostingsEnum rightDocs,
        boolean hasFreqs)
        throws IOException {
        if (leftDocs == null) {
            assertNull(rightDocs);
            return;
        }
        int docid = -1;
        int averageGap = leftReader.maxDoc() / (1 + docFreq);
        int skipInterval = 16;

        while (true) {
            if (random().nextBoolean()) {
                // nextDoc()
                docid = leftDocs.nextDoc();
                assertEquals(info, docid, rightDocs.nextDoc());
            } else {
                // advance()
                int skip =
                    docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
                docid = leftDocs.advance(skip);
                assertEquals(info, docid, rightDocs.advance(skip));
            }

            if (docid == DocIdSetIterator.NO_MORE_DOCS) {
                return;
            }
            if (hasFreqs) {
                assertEquals(info, leftDocs.freq(), rightDocs.freq());
            }
        }
    }

    /** checks advancing docs + positions */
    public void assertPositionsSkippingEquals(
        String info,
        IndexReader leftReader,
        int docFreq,
        PostingsEnum leftDocs,
        PostingsEnum rightDocs)
        throws IOException {
        if (leftDocs == null || rightDocs == null) {
            assertNull(leftDocs);
            assertNull(rightDocs);
            return;
        }

        int docid = -1;
        int averageGap = leftReader.maxDoc() / (1 + docFreq);
        int skipInterval = 16;

        while (true) {
            if (random().nextBoolean()) {
                // nextDoc()
                docid = leftDocs.nextDoc();
                assertEquals(info, docid, rightDocs.nextDoc());
            } else {
                // advance()
                int skip =
                    docid + (int) Math.ceil(Math.abs(skipInterval + random().nextGaussian() * averageGap));
                docid = leftDocs.advance(skip);
                assertEquals(info, docid, rightDocs.advance(skip));
            }

            if (docid == DocIdSetIterator.NO_MORE_DOCS) {
                return;
            }
            int freq = leftDocs.freq();
            assertEquals(info, freq, rightDocs.freq());
            for (int i = 0; i < freq; i++) {
                assertEquals(info, leftDocs.nextPosition(), rightDocs.nextPosition());
                assertEquals(info, leftDocs.getPayload(), rightDocs.getPayload());
            }
        }
    }

    private void assertTermsSeekingEquals(String info, Terms leftTerms, Terms rightTerms)
        throws IOException {

        // just an upper bound
        int numTests = atLeast(20);
        Random random = random();

        TermsEnum leftEnum = null;

        // collect this number of terms from the left side
        HashSet<BytesRef> tests = new HashSet<>();
        int numPasses = 0;
        while (numPasses < 10 && tests.size() < numTests) {
            leftEnum = leftTerms.iterator();
            BytesRef term = null;
            while ((term = leftEnum.next()) != null) {
                int code = random.nextInt(10);
                if (code == 0) {
                    // the term
                    tests.add(BytesRef.deepCopyOf(term));
                } else if (code == 1) {
                    // truncated subsequence of term
                    term = BytesRef.deepCopyOf(term);
                    if (term.length > 0) {
                        // truncate it
                        term.length = random.nextInt(term.length);
                    }
                } else if (code == 2) {
                    // term, but ensure a non-zero offset
                    byte[] newbytes = new byte[term.length + 5];
                    System.arraycopy(term.bytes, term.offset, newbytes, 5, term.length);
                    tests.add(new BytesRef(newbytes, 5, term.length));
                } else if (code == 3) {
                    switch (random().nextInt(3)) {
                        case 0:
                            tests.add(new BytesRef()); // before the first term
                            break;
                        case 1:
                            tests.add(new BytesRef(new byte[] {(byte) 0xFF, (byte) 0xFF})); // past the last term
                            break;
                        case 2:
                            tests.add(new BytesRef(TestUtil.randomSimpleString(random()))); // random term
                            break;
                        default:
                            throw new AssertionError();
                    }
                }
            }
            numPasses++;
        }

        TermsEnum rightEnum = rightTerms.iterator();

        ArrayList<BytesRef> shuffledTests = new ArrayList<>(tests);
        Collections.shuffle(shuffledTests, random);

        for (BytesRef b : shuffledTests) {
            if (rarely()) {
                // make new enums
                leftEnum = leftTerms.iterator();
                rightEnum = rightTerms.iterator();
            }

            final boolean seekExact = random().nextBoolean();

            if (seekExact) {
                assertEquals(info, leftEnum.seekExact(b), rightEnum.seekExact(b));
            } else {
                SeekStatus leftStatus = leftEnum.seekCeil(b);
                SeekStatus rightStatus = rightEnum.seekCeil(b);
                assertEquals(info, leftStatus, rightStatus);
                if (leftStatus != SeekStatus.END) {
                    assertEquals(info, leftEnum.term(), rightEnum.term());
                    assertTermStatsEquals(info, leftEnum, rightEnum);
                }
            }
        }
    }

    /** checks term-level statistics */
    public void assertTermStatsEquals(String info, TermsEnum leftTermsEnum, TermsEnum rightTermsEnum)
        throws IOException {
        assertEquals(info, leftTermsEnum.docFreq(), rightTermsEnum.docFreq());
        assertEquals(info, leftTermsEnum.totalTermFreq(), rightTermsEnum.totalTermFreq());
    }

    /** checks that norms are the same across all fields */
    public void assertNormsEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        Set<String> leftFields = new HashSet<>(FieldInfos.getIndexedFields(leftReader));
        Set<String> rightFields = new HashSet<>(FieldInfos.getIndexedFields(rightReader));
        assertEquals(info, leftFields, rightFields);

        for (String field : leftFields) {
            NumericDocValues leftNorms = MultiDocValues.getNormValues(leftReader, field);
            NumericDocValues rightNorms = MultiDocValues.getNormValues(rightReader, field);
            if (leftNorms != null && rightNorms != null) {
                assertDocValuesEquals(info, leftReader.maxDoc(), leftNorms, rightNorms);
            } else {
                assertNull(info, leftNorms);
                assertNull(info, rightNorms);
            }
        }
    }

    /** checks that stored fields of all documents are the same */
    public void assertStoredFieldsEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        assert leftReader.maxDoc() == rightReader.maxDoc();
        StoredFields leftStoredFields = leftReader.storedFields();
        StoredFields rightStoredFields = rightReader.storedFields();
        for (int i = 0; i < leftReader.maxDoc(); i++) {
            Document leftDoc = leftStoredFields.document(i);
            Document rightDoc = rightStoredFields.document(i);

            // TODO: I think this is bogus because we don't document what the order should be
            // from these iterators, etc. I think the codec/IndexReader should be free to order this stuff
            // in whatever way it wants (e.g. maybe it packs related fields together or something)
            // To fix this, we sort the fields in both documents by name, but
            // we still assume that all instances with same name are in order:
            Comparator<IndexableField> comp =
                new Comparator<IndexableField>() {
                    @Override
                    public int compare(IndexableField arg0, IndexableField arg1) {
                        return arg0.name().compareTo(arg1.name());
                    }
                };
            List<IndexableField> leftFields = new ArrayList<>(leftDoc.getFields());
            List<IndexableField> rightFields = new ArrayList<>(rightDoc.getFields());
            Collections.sort(leftFields, comp);
            Collections.sort(rightFields, comp);

            Iterator<IndexableField> leftIterator = leftFields.iterator();
            Iterator<IndexableField> rightIterator = rightFields.iterator();
            while (leftIterator.hasNext()) {
                assertTrue(info, rightIterator.hasNext());
                assertStoredFieldEquals(info, leftIterator.next(), rightIterator.next());
            }
            assertFalse(info, rightIterator.hasNext());
        }
    }

    /** checks that two stored fields are equivalent */
    public void assertStoredFieldEquals(
        String info, IndexableField leftField, IndexableField rightField) {
        assertEquals(info, leftField.name(), rightField.name());
        assertEquals(info, leftField.binaryValue(), rightField.binaryValue());
        assertEquals(info, leftField.stringValue(), rightField.stringValue());
        assertEquals(info, leftField.numericValue(), rightField.numericValue());
        // TODO: should we check the FT at all?
    }

    /** checks that term vectors across all fields are equivalent */
    public void assertTermVectorsEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        assert leftReader.maxDoc() == rightReader.maxDoc();
        TermVectors leftVectors = leftReader.termVectors();
        TermVectors rightVectors = rightReader.termVectors();
        for (int i = 0; i < leftReader.maxDoc(); i++) {
            Fields leftFields = leftVectors.get(i);
            Fields rightFields = rightVectors.get(i);

            // Fields could be null if there are no postings,
            // but then it must be null for both
            if (leftFields == null || rightFields == null) {
                assertNull(info, leftFields);
                assertNull(info, rightFields);
                return;
            }
            if (leftFields.size() != -1 && rightFields.size() != -1) {
                assertEquals(info, leftFields.size(), rightFields.size());
            }

            Iterator<String> leftEnum = leftFields.iterator();
            Iterator<String> rightEnum = rightFields.iterator();
            while (leftEnum.hasNext()) {
                String field = leftEnum.next();
                assertEquals(info, field, rightEnum.next());
                assertTermsEquals(
                    info, leftReader, leftFields.terms(field), rightFields.terms(field), rarely());
            }
            assertFalse(rightEnum.hasNext());
        }
    }

    private static Set<String> getDVFields(IndexReader reader) {
        Set<String> fields = new HashSet<>();
        for (FieldInfo fi : FieldInfos.getMergedFieldInfos(reader)) {
            if (fi.getDocValuesType() != DocValuesType.NONE) {
                fields.add(fi.name);
            }
        }

        return fields;
    }

    /** checks that docvalues across all fields are equivalent */
    public void assertDocValuesEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        Set<String> leftFields = getDVFields(leftReader);
        Set<String> rightFields = getDVFields(rightReader);
        assertEquals(info, leftFields, rightFields);

        for (String field : leftFields) {
            // TODO: clean this up... very messy
            {
                NumericDocValues leftValues = MultiDocValues.getNumericValues(leftReader, field);
                NumericDocValues rightValues = MultiDocValues.getNumericValues(rightReader, field);
                if (leftValues != null && rightValues != null) {
                    assertDocValuesEquals(info, leftReader.maxDoc(), leftValues, rightValues);
                } else {
                    assertTrue(
                        info + ": left numeric doc values for field=\"" + field + "\" are not null",
                        leftValues == null || leftValues.nextDoc() == NO_MORE_DOCS);
                    assertTrue(
                        info + ": right numeric doc values for field=\"" + field + "\" are not null",
                        rightValues == null || rightValues.nextDoc() == NO_MORE_DOCS);
                }
            }

            {
                BinaryDocValues leftValues = MultiDocValues.getBinaryValues(leftReader, field);
                BinaryDocValues rightValues = MultiDocValues.getBinaryValues(rightReader, field);
                if (leftValues != null && rightValues != null) {
                    while (true) {
                        int docID = leftValues.nextDoc();
                        assertEquals(docID, rightValues.nextDoc());
                        if (docID == NO_MORE_DOCS) {
                            break;
                        }
                        assertEquals(leftValues.binaryValue(), rightValues.binaryValue());
                    }
                } else {
                    assertTrue(info, leftValues == null || leftValues.nextDoc() == NO_MORE_DOCS);
                    assertTrue(info, rightValues == null || rightValues.nextDoc() == NO_MORE_DOCS);
                }
            }

            {
                SortedDocValues leftValues = MultiDocValues.getSortedValues(leftReader, field);
                SortedDocValues rightValues = MultiDocValues.getSortedValues(rightReader, field);
                if (leftValues != null && rightValues != null) {
                    // numOrds
                    assertEquals(info, leftValues.getValueCount(), rightValues.getValueCount());
                    // ords
                    for (int i = 0; i < leftValues.getValueCount(); i++) {
                        final BytesRef left = BytesRef.deepCopyOf(leftValues.lookupOrd(i));
                        final BytesRef right = rightValues.lookupOrd(i);
                        assertEquals(info, left, right);
                    }
                    // bytes
                    for (int docID = 0; docID < leftReader.maxDoc(); docID++) {
                        assertEquals(docID, leftValues.nextDoc());
                        assertEquals(docID, rightValues.nextDoc());
                        final BytesRef left = BytesRef.deepCopyOf(leftValues.lookupOrd(leftValues.ordValue()));
                        final BytesRef right = rightValues.lookupOrd(rightValues.ordValue());
                        assertEquals(info, left, right);
                    }
                } else {
                    assertNull(info, leftValues);
                    assertNull(info, rightValues);
                }
            }

            {
                SortedSetDocValues leftValues = MultiDocValues.getSortedSetValues(leftReader, field);
                SortedSetDocValues rightValues = MultiDocValues.getSortedSetValues(rightReader, field);
                if (leftValues != null && rightValues != null) {
                    // numOrds
                    assertEquals(info, leftValues.getValueCount(), rightValues.getValueCount());
                    // ords
                    for (int i = 0; i < leftValues.getValueCount(); i++) {
                        final BytesRef left = BytesRef.deepCopyOf(leftValues.lookupOrd(i));
                        final BytesRef right = rightValues.lookupOrd(i);
                        assertEquals(info, left, right);
                    }
                    // ord lists
                    while (true) {
                        int docID = leftValues.nextDoc();
                        assertEquals(docID, rightValues.nextDoc());
                        if (docID == NO_MORE_DOCS) {
                            break;
                        }
                        assertEquals(info, leftValues.docValueCount(), rightValues.docValueCount());
                        for (int i = 0; i < leftValues.docValueCount(); i++) {
                            assertEquals(info, leftValues.nextOrd(), rightValues.nextOrd());
                        }
                    }
                } else {
                    assertNull(info, leftValues);
                    assertNull(info, rightValues);
                }
            }

            {
                SortedNumericDocValues leftValues =
                    MultiDocValues.getSortedNumericValues(leftReader, field);
                SortedNumericDocValues rightValues =
                    MultiDocValues.getSortedNumericValues(rightReader, field);
                if (leftValues != null && rightValues != null) {
                    while (true) {
                        int docID = leftValues.nextDoc();
                        assertEquals(docID, rightValues.nextDoc());
                        if (docID == NO_MORE_DOCS) {
                            break;
                        }
                        assertEquals(info, leftValues.docValueCount(), rightValues.docValueCount());
                        for (int j = 0; j < leftValues.docValueCount(); j++) {
                            assertEquals(info, leftValues.nextValue(), rightValues.nextValue());
                        }
                    }
                } else {
                    assertNull(info, leftValues);
                    assertNull(info, rightValues);
                }
            }
        }
    }

    public void assertDocValuesEquals(
        String info, int num, NumericDocValues leftDocValues, NumericDocValues rightDocValues)
        throws IOException {
        assertNotNull(info, leftDocValues);
        assertNotNull(info, rightDocValues);
        while (true) {
            int leftDocID = leftDocValues.nextDoc();
            int rightDocID = rightDocValues.nextDoc();
            assertEquals(leftDocID, rightDocID);
            if (leftDocID == NO_MORE_DOCS) {
                return;
            }
            assertEquals(leftDocValues.longValue(), rightDocValues.longValue());
        }
    }

    // TODO: this is kinda stupid, we don't delete documents in the test.
    public void assertDeletedDocsEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        assert leftReader.numDeletedDocs() == rightReader.numDeletedDocs();
        Bits leftBits = MultiBits.getLiveDocs(leftReader);
        Bits rightBits = MultiBits.getLiveDocs(rightReader);

        if (leftBits == null || rightBits == null) {
            assertNull(info, leftBits);
            assertNull(info, rightBits);
            return;
        }

        assert leftReader.maxDoc() == rightReader.maxDoc();
        assertEquals(info, leftBits.length(), rightBits.length());
        for (int i = 0; i < leftReader.maxDoc(); i++) {
            assertEquals(info, leftBits.get(i), rightBits.get(i));
        }
    }

    public void assertFieldInfosEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        FieldInfos leftInfos = FieldInfos.getMergedFieldInfos(leftReader);
        FieldInfos rightInfos = FieldInfos.getMergedFieldInfos(rightReader);

        // TODO: would be great to verify more than just the names of the fields!
        TreeSet<String> left = new TreeSet<>();
        TreeSet<String> right = new TreeSet<>();

        for (FieldInfo fi : leftInfos) {
            left.add(fi.name);
        }

        for (FieldInfo fi : rightInfos) {
            right.add(fi.name);
        }

        assertEquals(info, left, right);
    }

    // naive silly memory heavy uninversion!!  maps docID -> packed values (a Set because a given doc
    // can be multi-valued)
    private Map<Integer, Set<BytesRef>> uninvert(String fieldName, IndexReader reader)
        throws IOException {
        final Map<Integer, Set<BytesRef>> docValues = new HashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {

            PointValues points = ctx.reader().getPointValues(fieldName);
            if (points == null) {
                continue;
            }

            points.intersect(
                new PointValues.IntersectVisitor() {
                    @Override
                    public void visit(int docID) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void visit(int docID, byte[] packedValue) throws IOException {
                        int topDocID = ctx.docBase + docID;
                        if (docValues.containsKey(topDocID) == false) {
                            docValues.put(topDocID, new HashSet<BytesRef>());
                        }
                        docValues.get(topDocID).add(new BytesRef(packedValue.clone()));
                    }

                    @Override
                    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                        // We pretend our query shape is so hairy that it crosses every single cell:
                        return PointValues.Relation.CELL_CROSSES_QUERY;
                    }
                });
        }

        return docValues;
    }

    public void assertPointsEquals(String info, IndexReader leftReader, IndexReader rightReader)
        throws IOException {
        FieldInfos fieldInfos1 = FieldInfos.getMergedFieldInfos(leftReader);
        FieldInfos fieldInfos2 = FieldInfos.getMergedFieldInfos(rightReader);
        for (FieldInfo fieldInfo1 : fieldInfos1) {
            if (fieldInfo1.getPointDimensionCount() != 0) {
                FieldInfo fieldInfo2 = fieldInfos2.fieldInfo(fieldInfo1.name);
                // same data dimension count?
                assertEquals(
                    info, fieldInfo2.getPointDimensionCount(), fieldInfo2.getPointDimensionCount());
                // same index dimension count?
                assertEquals(
                    info,
                    fieldInfo2.getPointIndexDimensionCount(),
                    fieldInfo2.getPointIndexDimensionCount());
                // same bytes per dimension?
                assertEquals(info, fieldInfo2.getPointNumBytes(), fieldInfo2.getPointNumBytes());

                assertEquals(
                    info + " field=" + fieldInfo1.name,
                    uninvert(fieldInfo1.name, leftReader),
                    uninvert(fieldInfo1.name, rightReader));
            }
        }

        // make sure FieldInfos2 doesn't have any point fields that FieldInfo1 didn't have
        for (FieldInfo fieldInfo2 : fieldInfos2) {
            if (fieldInfo2.getPointDimensionCount() != 0) {
                FieldInfo fieldInfo1 = fieldInfos1.fieldInfo(fieldInfo2.name);
                // same data dimension count?
                assertEquals(
                    info, fieldInfo2.getPointDimensionCount(), fieldInfo1.getPointDimensionCount());
                // same index dimension count?
                assertEquals(
                    info,
                    fieldInfo2.getPointIndexDimensionCount(),
                    fieldInfo1.getPointIndexDimensionCount());
                // same bytes per dimension?
                assertEquals(info, fieldInfo2.getPointNumBytes(), fieldInfo1.getPointNumBytes());

                // we don't need to uninvert and compare here ... we did that in the first loop above
            }
        }
    }

    /** Inspects stack trace to figure out if a method of a specific class called us. */
    public static boolean callStackContains(Class<?> clazz, String methodName) {
        final String className = clazz.getName();
        return StackWalker.getInstance()
            .walk(
                s ->
                    s.skip(1) // exclude this utility method
                        .anyMatch(
                            f ->
                                className.equals(f.getClassName())
                                && methodName.equals(f.getMethodName())));
    }

    /**
     * Inspects stack trace to figure out if one of the given method names (no class restriction)
     * called us.
     */
    public static boolean callStackContainsAnyOf(String... methodNames) {
        return StackWalker.getInstance()
            .walk(
                s ->
                    s.skip(1) // exclude this utility method
                        .map(StackFrame::getMethodName)
                        .anyMatch(Set.of(methodNames)::contains));
    }

    /** Inspects stack trace if the given class called us. */
    public static boolean callStackContains(Class<?> clazz) {
        return StackWalker.getInstance()
            .walk(
                s ->
                    s.skip(1) // exclude this utility method
                        .map(StackFrame::getClassName)
                        .anyMatch(clazz.getName()::equals));
    }

    /** A runnable that can throw any checked exception. */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Throwable;
    }

    /** A {@link java.util.function.Consumer} that can throw any checked exception. */
    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    /** Checks a specific exception class is thrown by the given runnable, and returns it. */
    public static <T extends Throwable> T expectThrows(
        Class<T> expectedType, ThrowingRunnable runnable) {
        return expectThrows(
            expectedType,
            "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown",
            runnable);
    }

    /** Checks a specific exception class is thrown by the given runnable, and returns it. */
    public static <T extends Throwable> T expectThrows(
        Class<T> expectedType, String noExceptionMessage, ThrowingRunnable runnable) {
        final Throwable thrown = _expectThrows(Collections.singletonList(expectedType), runnable);
        if (expectedType.isInstance(thrown)) {
            return expectedType.cast(thrown);
        }
        if (null == thrown) {
            throw new AssertionFailedError(noExceptionMessage);
        }
        AssertionFailedError assertion =
            new AssertionFailedError(
                "Unexpected exception type, expected "
                + expectedType.getSimpleName()
                + " but got "
                + thrown);
        assertion.initCause(thrown);
        throw assertion;
    }

    /** Checks a specific exception class is thrown by the given runnable, and returns it. */
    public static <T extends Throwable> T expectThrowsAnyOf(
        List<Class<? extends T>> expectedTypes, ThrowingRunnable runnable) {
        if (expectedTypes.isEmpty()) {
            throw new AssertionError("At least one expected exception type is required?");
        }

        final Throwable thrown = _expectThrows(expectedTypes, runnable);
        if (null != thrown) {
            for (Class<? extends T> expectedType : expectedTypes) {
                if (expectedType.isInstance(thrown)) {
                    return expectedType.cast(thrown);
                }
            }
        }

        List<String> exceptionTypes =
            expectedTypes.stream().map(c -> c.getSimpleName()).collect(Collectors.toList());

        if (thrown != null) {
            AssertionFailedError assertion =
                new AssertionFailedError(
                    "Unexpected exception type, expected any of "
                    + exceptionTypes
                    + " but got: "
                    + thrown);
            assertion.initCause(thrown);
            throw assertion;
        } else {
            throw new AssertionFailedError(
                "Expected any of the following exception types: "
                + exceptionTypes
                + " but no exception was thrown.");
        }
    }

    /**
     * Checks that specific wrapped and outer exception classes are thrown by the given runnable, and
     * returns the wrapped exception.
     */
    public static <TO extends Throwable, TW extends Throwable> TW expectThrows(
        Class<TO> expectedOuterType, Class<TW> expectedWrappedType, ThrowingRunnable runnable) {
        final Throwable thrown = _expectThrows(Collections.singletonList(expectedOuterType), runnable);
        if (null == thrown) {
            throw new AssertionFailedError(
                "Expected outer exception "
                + expectedOuterType.getSimpleName()
                + " but no exception was thrown.");
        }
        if (expectedOuterType.isInstance(thrown)) {
            Throwable cause = thrown.getCause();
            if (expectedWrappedType.isInstance(cause)) {
                return expectedWrappedType.cast(cause);
            } else {
                AssertionFailedError assertion =
                    new AssertionFailedError(
                        "Unexpected wrapped exception type, expected "
                        + expectedWrappedType.getSimpleName()
                        + " but got: "
                        + cause);
                assertion.initCause(thrown);
                throw assertion;
            }
        }
        AssertionFailedError assertion =
            new AssertionFailedError(
                "Unexpected outer exception type, expected "
                + expectedOuterType.getSimpleName()
                + " but got: "
                + thrown);
        assertion.initCause(thrown);
        throw assertion;
    }

    /**
     * Checks that one of the specified wrapped and outer exception classes are thrown by the given
     * runnable, and returns the outer exception.
     *
     * <p>This method accepts outer exceptions with no wrapped exception; an empty list of expected
     * wrapped exception types indicates no wrapped exception.
     */
    public static <TO extends Throwable, TW extends Throwable> TO expectThrowsAnyOf(
        LinkedHashMap<Class<? extends TO>, List<Class<? extends TW>>> expectedOuterToWrappedTypes,
        ThrowingRunnable runnable) {
        final List<Class<? extends TO>> outerClasses =
            expectedOuterToWrappedTypes.keySet().stream().collect(Collectors.toList());
        final Throwable thrown = _expectThrows(outerClasses, runnable);

        if (null == thrown) {
            List<String> outerTypes =
                outerClasses.stream().map(Class::getSimpleName).collect(Collectors.toList());
            throw new AssertionFailedError(
                "Expected any of the following outer exception types: "
                + outerTypes
                + " but no exception was thrown.");
        }
        for (Map.Entry<Class<? extends TO>, List<Class<? extends TW>>> entry :
            expectedOuterToWrappedTypes.entrySet()) {
            Class<? extends TO> expectedOuterType = entry.getKey();
            List<Class<? extends TW>> expectedWrappedTypes = entry.getValue();
            Throwable cause = thrown.getCause();
            if (expectedOuterType.isInstance(thrown)) {
                if (expectedWrappedTypes.isEmpty()) {
                    return null; // no wrapped exception
                } else {
                    for (Class<? extends TW> expectedWrappedType : expectedWrappedTypes) {
                        if (expectedWrappedType.isInstance(cause)) {
                            return expectedOuterType.cast(thrown);
                        }
                    }
                    List<String> wrappedTypes =
                        expectedWrappedTypes.stream().map(Class::getSimpleName).collect(Collectors.toList());
                    AssertionFailedError assertion =
                        new AssertionFailedError(
                            "Unexpected wrapped exception type, expected one of "
                            + wrappedTypes
                            + " but got: "
                            + cause);
                    assertion.initCause(thrown);
                    throw assertion;
                }
            }
        }
        List<String> outerTypes =
            outerClasses.stream().map(Class::getSimpleName).collect(Collectors.toList());
        AssertionFailedError assertion =
            new AssertionFailedError(
                "Unexpected outer exception type, expected one of "
                + outerTypes
                + " but got: "
                + thrown);
        assertion.initCause(thrown);
        throw assertion;
    }

    /**
     * Helper method for {@link #expectThrows} and {@link #expectThrowsAnyOf} that takes care of
     * propagating any {@link AssertionError} or {@link AssumptionViolatedException} instances thrown
     * if and only if they are super classes of the <code>expectedTypes</code>. Otherwise simply
     * returns any {@link Throwable} thrown, regardless of type, or null if the <code>runnable</code>
     * completed w/o error.
     */
    private static Throwable _expectThrows(
        List<? extends Class<?>> expectedTypes, ThrowingRunnable runnable) {

        try {
            runnable.run();
        } catch (AssertionError | AssumptionViolatedException ae) {
            for (Class<?> expectedType : expectedTypes) {
                if (expectedType.isInstance(ae)) { // user is expecting this type explicitly
                    return ae;
                }
            }
            throw ae;
        } catch (Throwable e) {
            return e;
        }
        return null;
    }

    /**
     * Returns true if the file exists (can be opened), false if it cannot be opened, and (unlike
     * Java's File.exists) throws IOException if there's some unexpected error.
     */
    public static boolean slowFileExists(Directory dir, String fileName) throws IOException {
        try {
            dir.openInput(fileName, IOContext.DEFAULT).close();
            return true;
        } catch (@SuppressWarnings("unused") NoSuchFileException | FileNotFoundException e) {
            return false;
        }
    }

    /**
     * Creates an empty, temporary folder (when the name of the folder is of no importance).
     *
     * @see #createTempDir(String)
     */
    public static Path createTempDir() {
        return createTempDir("tempDir");
    }

    /**
     * Creates an empty, temporary folder with the given name prefix.
     *
     * <p>The folder will be automatically removed after the test class completes successfully. The
     * test should close any file handles that would prevent the folder from being removed.
     */
    public static Path createTempDir(String prefix) {
        return tempFilesCleanupRule.createTempDir(prefix);
    }

    /**
     * Creates an empty file with the given prefix and suffix.
     *
     * <p>The file will be automatically removed after the test class completes successfully. The test
     * should close any file handles that would prevent the folder from being removed.
     */
    public static Path createTempFile(String prefix, String suffix) throws IOException {
        return tempFilesCleanupRule.createTempFile(prefix, suffix);
    }

    /**
     * Creates an empty temporary file.
     *
     * @see #createTempFile(String, String)
     */
    public static Path createTempFile() throws IOException {
        return createTempFile("tempFile", ".tmp");
    }

    /** True if assertions (-ea) are enabled (at least for this class). */
    public static final boolean assertsAreEnabled;

    static {
        boolean enabled = false;
        assert enabled = true; // Intentional side-effect!!!
        assertsAreEnabled = enabled;
    }

    /**
     * Compares two strings with a collator, also looking to see if the the strings are impacted by
     * jdk bugs. may not avoid all jdk bugs in tests. see
     * https://bugs.openjdk.java.net/browse/JDK-8071862
     */
    @SuppressForbidden(reason = "dodges JDK-8071862")
    public static int collate(Collator collator, String s1, String s2) {
        int v1 = collator.compare(s1, s2);
        int v2 = collator.getCollationKey(s1).compareTo(collator.getCollationKey(s2));
        // if collation keys don't really respect collation order, things are screwed.
        assumeTrue("hit JDK collator bug", Integer.signum(v1) == Integer.signum(v2));
        return v1;
    }

    /** Ensures that the MergePolicy has sane values for tests that test with lots of documents. */
    protected static IndexWriterConfig ensureSaneIWCOnNightly(IndexWriterConfig conf) {
        if (LuceneTestCase.TEST_NIGHTLY) {
            // newIWConfig makes smallish max seg size, which
            // results in tons and tons of segments for this test
            // when run nightly:
            MergePolicy mp = conf.getMergePolicy();
            if (mp instanceof TieredMergePolicy) {
                ((TieredMergePolicy) mp).setMaxMergedSegmentMB(5000.);
            } else if (mp instanceof LogByteSizeMergePolicy) {
                ((LogByteSizeMergePolicy) mp).setMaxMergeMB(1000.);
            } else if (mp instanceof LogMergePolicy) {
                ((LogMergePolicy) mp).setMaxMergeDocs(100000);
            }
            // when running nightly, merging can still have crazy parameters,
            // and might use many per-field codecs. turn on CFS for IW flushes
            // and ensure CFS ratio is reasonable to keep it contained.
            conf.setUseCompoundFile(true);
            mp.setNoCFSRatio(Math.max(0.25d, mp.getNoCFSRatio()));
        }
        return conf;
    }

    /**
     * Creates a {@link BytesRef} holding UTF-8 bytes for the incoming String, that sometimes uses a
     * non-zero {@code offset}, and non-zero end-padding, to tickle latent bugs that fail to look at
     * {@code BytesRef.offset}.
     */
    public static BytesRef newBytesRef(String s) {
        return newBytesRef(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Creates a copy of the incoming {@link BytesRef} that sometimes uses a non-zero {@code offset},
     * and non-zero end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
     */
    public static BytesRef newBytesRef(BytesRef b) {
        assert b.isValid();
        return newBytesRef(b.bytes, b.offset, b.length);
    }

    /**
     * Creates a random BytesRef from the incoming bytes that sometimes uses a non-zero {@code
     * offset}, and non-zero end-padding, to tickle latent bugs that fail to look at {@code
     * BytesRef.offset}.
     */
    public static BytesRef newBytesRef(byte[] b) {
        return newBytesRef(b, 0, b.length);
    }

    /**
     * Creates a random empty BytesRef that sometimes uses a non-zero {@code offset}, and non-zero
     * end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
     */
    public static BytesRef newBytesRef() {
        return newBytesRef(new byte[0], 0, 0);
    }

    /**
     * Creates a random empty BytesRef, with at least the requested length of bytes free, that
     * sometimes uses a non-zero {@code offset}, and non-zero end-padding, to tickle latent bugs that
     * fail to look at {@code BytesRef.offset}.
     */
    public static BytesRef newBytesRef(int byteLength) {
        return newBytesRef(new byte[byteLength], 0, byteLength);
    }

    /**
     * Creates a copy of the incoming bytes slice that sometimes uses a non-zero {@code offset}, and
     * non-zero end-padding, to tickle latent bugs that fail to look at {@code BytesRef.offset}.
     */
    public static BytesRef newBytesRef(byte[] bytesIn, int offset, int length) {
        // System.out.println("LTC.newBytesRef!  bytesIn.length=" + bytesIn.length + " offset=" + offset
        //                 + " length=" + length);

        assert bytesIn.length >= offset + length
            : "got offset=" + offset + " length=" + length + " bytesIn.length=" + bytesIn.length;

        // randomly set a non-zero offset
        int startOffset;
        if (random().nextBoolean()) {
            startOffset = RandomNumbers.randomIntBetween(random(), 1, 20);
        } else {
            startOffset = 0;
        }

        // also randomly set an end padding:
        int endPadding;
        if (random().nextBoolean()) {
            endPadding = RandomNumbers.randomIntBetween(random(), 1, 20);
        } else {
            endPadding = 0;
        }

        byte[] bytes = new byte[startOffset + length + endPadding];

        System.arraycopy(bytesIn, offset, bytes, startOffset, length);
        // System.out.println("LTC:  return bytes.length=" + bytes.length + " startOffset=" +
        //                 startOffset + " length=" + length);

        BytesRef it = new BytesRef(bytes, startOffset, length);
        assert it.isValid();

        if (RandomNumbers.randomIntBetween(random(), 1, 17) == 7) {
            // try to ferret out bugs in this method too!
            return newBytesRef(it.bytes, it.offset, it.length);
        }

        return it;
    }
}
