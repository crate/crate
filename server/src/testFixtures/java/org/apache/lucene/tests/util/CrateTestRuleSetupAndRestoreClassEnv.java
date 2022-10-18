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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.codecs.asserting.AssertingDocValuesFormat;
import org.apache.lucene.tests.codecs.asserting.AssertingPostingsFormat;
import org.apache.lucene.tests.codecs.cheapbastard.CheapBastardCodec;
import org.apache.lucene.tests.codecs.compressing.CompressingCodec;
import org.apache.lucene.tests.codecs.mockrandom.MockRandomPostingsFormat;
import org.apache.lucene.tests.index.RandomCodec;
import org.apache.lucene.tests.search.similarities.AssertingSimilarity;
import org.apache.lucene.tests.search.similarities.RandomSimilarity;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.junit.internal.AssumptionViolatedException;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/**
 * Setup and restore suite-level environment (fine grained junk that doesn't fit anywhere else).
 *
 *  Copy of {@link org.apache.lucene.tests.util.TestRuleSetupAndRestoreClassEnv}
 */
final class CrateTestRuleSetupAndRestoreClassEnv extends AbstractBeforeAfterRule {
    private Codec savedCodec;
    private Locale savedLocale;
    private TimeZone savedTimeZone;
    private InfoStream savedInfoStream;

    Locale locale;
    TimeZone timeZone;
    Similarity similarity;
    Codec codec;

    /** Indicates whether the rule has executed its {@link #before()} method fully. */
    private boolean initialized;

    /** @see CrateLuceneTestCase.SuppressCodecs */
    HashSet<String> avoidCodecs;

    static class ThreadNameFixingPrintStreamInfoStream extends PrintStreamInfoStream {
        public ThreadNameFixingPrintStreamInfoStream(PrintStream out) {
            super(out);
        }

        @Override
        public void message(String component, String message) {
            if ("TP".equals(component)) {
                return; // ignore test points!
            }
            final String name;
            if (Thread.currentThread().getName().startsWith("TEST-")) {
                // The name of the main thread is way too
                // long when looking at IW verbose output...
                name = "main";
            } else {
                name = Thread.currentThread().getName();
            }
            stream.println(
                component + " " + messageID + " [" + getTimestamp() + "; " + name + "]: " + message);
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    protected void before() throws Exception {
        // enable this by default, for IDE consistency with ant tests (as it's the default from ant)
        // TODO: really should be in solr base classes, but some extend LTC directly.
        // we do this in beforeClass, because some tests currently disable it
        if (System.getProperty("solr.directoryFactory") == null) {
            System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockDirectoryFactory");
        }

        // if verbose: print some debugging stuff about which codecs are loaded.
        if (CrateLuceneTestCase.VERBOSE) {
            System.out.println("Loaded codecs: " + Codec.availableCodecs());
            System.out.println("Loaded postingsFormats: " + PostingsFormat.availablePostingsFormats());
        }

        savedInfoStream = InfoStream.getDefault();
        final Random random = RandomizedContext.current().getRandom();
        final boolean v = random.nextBoolean();
        if (CrateLuceneTestCase.INFOSTREAM) {
            InfoStream.setDefault(new ThreadNameFixingPrintStreamInfoStream(System.out));
        } else if (v) {
            InfoStream.setDefault(new NullInfoStream());
        }

        Class<?> targetClass = RandomizedContext.current().getTargetClass();
        avoidCodecs = new HashSet<>();
        if (targetClass.isAnnotationPresent(CrateLuceneTestCase.SuppressCodecs.class)) {
            CrateLuceneTestCase.SuppressCodecs a = targetClass.getAnnotation(CrateLuceneTestCase.SuppressCodecs.class);
            avoidCodecs.addAll(Arrays.asList(a.value()));
        }

        savedCodec = Codec.getDefault();
        int randomVal = random.nextInt(11);
        if ("default".equals(CrateLuceneTestCase.TEST_CODEC)) {
            codec = savedCodec; // just use the default, don't randomize
        } else if (("random".equals(CrateLuceneTestCase.TEST_POSTINGSFORMAT) == false)
                   || ("random".equals(CrateLuceneTestCase.TEST_DOCVALUESFORMAT) == false)) {
            // the user wired postings or DV: this is messy
            // refactor into RandomCodec....

            final PostingsFormat format;
            if ("random".equals(CrateLuceneTestCase.TEST_POSTINGSFORMAT)) {
                format = new AssertingPostingsFormat();
            } else if ("MockRandom".equals(CrateLuceneTestCase.TEST_POSTINGSFORMAT)) {
                format = new MockRandomPostingsFormat(new Random(random.nextLong()));
            } else {
                format = PostingsFormat.forName(CrateLuceneTestCase.TEST_POSTINGSFORMAT);
            }

            final DocValuesFormat dvFormat;
            if ("random".equals(CrateLuceneTestCase.TEST_DOCVALUESFORMAT)) {
                dvFormat = new AssertingDocValuesFormat();
            } else {
                dvFormat = DocValuesFormat.forName(CrateLuceneTestCase.TEST_DOCVALUESFORMAT);
            }

            codec =
                new AssertingCodec() {
                    @Override
                    public PostingsFormat getPostingsFormatForField(String field) {
                        return format;
                    }

                    @Override
                    public DocValuesFormat getDocValuesFormatForField(String field) {
                        return dvFormat;
                    }

                    @Override
                    public String toString() {
                        return super.toString() + ": " + format.toString() + ", " + dvFormat.toString();
                    }
                };
        } else if ("CheapBastard".equals(CrateLuceneTestCase.TEST_CODEC)
                   || ("random".equals(CrateLuceneTestCase.TEST_CODEC)
                       && randomVal == 8
                       && !shouldAvoidCodec("CheapBastard")
                       && !shouldAvoidCodec("Lucene41"))) {
            // we also avoid this codec if Lucene41 is avoided, since thats the postings format it uses.
            codec = new CheapBastardCodec();
        } else if ("Asserting".equals(CrateLuceneTestCase.TEST_CODEC)
                   || ("random".equals(CrateLuceneTestCase.TEST_CODEC) && randomVal == 7 && !shouldAvoidCodec("Asserting"))) {
            codec = new AssertingCodec();
        } else if ("Compressing".equals(CrateLuceneTestCase.TEST_CODEC)
                   || ("random".equals(CrateLuceneTestCase.TEST_CODEC) && randomVal == 6 && !shouldAvoidCodec("Compressing"))) {
            codec = CompressingCodec.randomInstance(random);
        } else if ("Lucene94".equals(CrateLuceneTestCase.TEST_CODEC)
                   || ("random".equals(CrateLuceneTestCase.TEST_CODEC) && randomVal == 5 && !shouldAvoidCodec("Lucene94"))) {
            codec = new Lucene94Codec(RandomPicks.randomFrom(random, Lucene94Codec.Mode.values()));
        } else if (!"random".equals(CrateLuceneTestCase.TEST_CODEC)) {
            codec = Codec.forName(CrateLuceneTestCase.TEST_CODEC);
        } else if ("random".equals(CrateLuceneTestCase.TEST_POSTINGSFORMAT)) {
            codec = new RandomCodec(random, avoidCodecs);
        } else {
            assert false;
        }
        Codec.setDefault(codec);

        // Initialize locale/ timezone.
        String testLocale = System.getProperty("tests.locale", "random");
        String testTimeZone = System.getProperty("tests.timezone", "random");

        // Always pick a random one for consistency (whether tests.locale was specified or not).
        savedLocale = Locale.getDefault();
        Locale randomLocale = CrateLuceneTestCase.randomLocale(random);
        locale = testLocale.equals("random") ? randomLocale : CrateLuceneTestCase.localeForLanguageTag(testLocale);
        Locale.setDefault(locale);

        savedTimeZone = TimeZone.getDefault();
        TimeZone randomTimeZone = CrateLuceneTestCase.randomTimeZone(CrateLuceneTestCase.random());
        timeZone = testTimeZone.equals("random") ? randomTimeZone : TimeZone.getTimeZone(testTimeZone);
        TimeZone.setDefault(timeZone);
        similarity = new AssertingSimilarity(new RandomSimilarity(CrateLuceneTestCase.random()));

        // Check codec restrictions once at class level.
        try {
            checkCodecRestrictions(codec);
        } catch (AssumptionViolatedException e) {
            System.err.println(
                "NOTE: "
                + e.getMessage()
                + " Suppressed codecs: "
                + Arrays.toString(avoidCodecs.toArray()));
            throw e;
        }

        // We have "stickiness" so that sometimes all we do is vary the RAM buffer size, other times
        // just the doc count to flush by, else both.
        // This way the assertMemory in DocumentsWriterFlushControl sometimes runs (when we always flush
        // by RAM).
        CrateLuceneTestCase.LiveIWCFlushMode flushMode;
        switch (CrateLuceneTestCase.random().nextInt(3)) {
            case 0:
                flushMode = CrateLuceneTestCase.LiveIWCFlushMode.BY_RAM;
                break;
            case 1:
                flushMode = CrateLuceneTestCase.LiveIWCFlushMode.BY_DOCS;
                break;
            case 2:
                flushMode = CrateLuceneTestCase.LiveIWCFlushMode.EITHER;
                break;
            default:
                throw new AssertionError();
        }

        CrateLuceneTestCase.setLiveIWCFlushMode(flushMode);

        initialized = true;
    }

    /**
     * Check codec restrictions.
     *
     * @throws AssumptionViolatedException if the class does not work with a given codec.
     */
    private void checkCodecRestrictions(Codec codec) {
        CrateLuceneTestCase.assumeFalse(
            "Class not allowed to use codec: " + codec.getName() + ".",
            shouldAvoidCodec(codec.getName()));

        if (codec instanceof RandomCodec && !avoidCodecs.isEmpty()) {
            for (String name : ((RandomCodec) codec).formatNames) {
                CrateLuceneTestCase.assumeFalse(
                    "Class not allowed to use postings format: " + name + ".", shouldAvoidCodec(name));
            }
        }

        PostingsFormat pf = codec.postingsFormat();
        CrateLuceneTestCase.assumeFalse(
            "Class not allowed to use postings format: " + pf.getName() + ".",
            shouldAvoidCodec(pf.getName()));

        CrateLuceneTestCase.assumeFalse(
            "Class not allowed to use postings format: " + CrateLuceneTestCase.TEST_POSTINGSFORMAT + ".",
            shouldAvoidCodec(CrateLuceneTestCase.TEST_POSTINGSFORMAT));
    }

    /** After suite cleanup (always invoked). */
    @Override
    protected void after() throws Exception {
        Codec.setDefault(savedCodec);
        InfoStream.setDefault(savedInfoStream);
        if (savedLocale != null) Locale.setDefault(savedLocale);
        if (savedTimeZone != null) TimeZone.setDefault(savedTimeZone);
    }

    /** Should a given codec be avoided for the currently executing suite? */
    private boolean shouldAvoidCodec(String codec) {
        return !avoidCodecs.isEmpty() && avoidCodecs.contains(codec);
    }
}
