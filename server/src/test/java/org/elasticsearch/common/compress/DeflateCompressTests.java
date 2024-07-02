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

package org.elasticsearch.common.compress;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.tests.util.LineFileDocs;
import org.elasticsearch.test.ESTestCase;

/**
 * Test streaming compression (e.g. used for recovery)
 */
public class DeflateCompressTests extends ESTestCase {

    private final Compressor compressor = new DeflateCompressor();

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            byte bytes[] = new byte[r.nextInt(1, 100000)];
            r.nextBytes(bytes);
            doTest(bytes);
        }
    }

    public void testRandomThreads() throws Exception {
        final Random r = random();
        int threadCount = r.nextInt(2, 6);
        Thread[] threads = new Thread[threadCount];
        final CountDownLatch startingGun = new CountDownLatch(1);
        for (int tid=0; tid < threadCount; tid++) {
            final long seed = r.nextLong();
            threads[tid] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(seed);
                        startingGun.await();
                        for (int i = 0; i < 10; i++) {
                            byte bytes[] = new byte[r.nextInt(1, 100000)];
                            r.nextBytes(bytes);
                            doTest(bytes);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[tid].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }

    public void testLineDocs() throws IOException {
        Random r = random();
        LineFileDocs lineFileDocs = new LineFileDocs(r);
        for (int i = 0; i < 10; i++) {
            int numDocs = r.nextInt( 1, 200);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int j = 0; j < numDocs; j++) {
                String s = lineFileDocs.nextDoc().get("body");
                bos.write(s.getBytes(StandardCharsets.UTF_8));
            }
            doTest(bos.toByteArray());
        }
        lineFileDocs.close();
    }

    public void testLineDocsThreads() throws Exception {
        final Random r = random();
        int threadCount = r.nextInt( 2, 6);
        Thread[] threads = new Thread[threadCount];
        final CountDownLatch startingGun = new CountDownLatch(1);
        for (int tid=0; tid < threadCount; tid++) {
            final long seed = r.nextLong();
            threads[tid] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(seed);
                        startingGun.await();
                        LineFileDocs lineFileDocs = new LineFileDocs(r);
                        for (int i = 0; i < 10; i++) {
                            int numDocs = r.nextInt( 1, 200);
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            for (int j = 0; j < numDocs; j++) {
                                String s = lineFileDocs.nextDoc().get("body");
                                bos.write(s.getBytes(StandardCharsets.UTF_8));
                            }
                            doTest(bos.toByteArray());
                        }
                        lineFileDocs.close();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[tid].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }

    public void testRepetitionsL() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numLongs = r.nextInt( 1, 10000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long theValue = r.nextLong();
            for (int j = 0; j < numLongs; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextLong();
                }
                bos.write((byte) (theValue >>> 56));
                bos.write((byte) (theValue >>> 48));
                bos.write((byte) (theValue >>> 40));
                bos.write((byte) (theValue >>> 32));
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testRepetitionsLThreads() throws Exception {
        final Random r = random();
        int threadCount = r.nextInt( 2, 6);
        Thread[] threads = new Thread[threadCount];
        final CountDownLatch startingGun = new CountDownLatch(1);
        for (int tid=0; tid < threadCount; tid++) {
            final long seed = r.nextLong();
            threads[tid] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(seed);
                        startingGun.await();
                        for (int i = 0; i < 10; i++) {
                            int numLongs = r.nextInt( 1, 10000);
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            long theValue = r.nextLong();
                            for (int j = 0; j < numLongs; j++) {
                                if (r.nextInt(10) == 0) {
                                    theValue = r.nextLong();
                                }
                                bos.write((byte) (theValue >>> 56));
                                bos.write((byte) (theValue >>> 48));
                                bos.write((byte) (theValue >>> 40));
                                bos.write((byte) (theValue >>> 32));
                                bos.write((byte) (theValue >>> 24));
                                bos.write((byte) (theValue >>> 16));
                                bos.write((byte) (theValue >>> 8));
                                bos.write((byte) theValue);
                            }
                            doTest(bos.toByteArray());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[tid].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }

    public void testRepetitionsI() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numInts = r.nextInt( 1, 20000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int theValue = r.nextInt();
            for (int j = 0; j < numInts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextInt();
                }
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testRepetitionsIThreads() throws Exception {
        final Random r = random();
        int threadCount = r.nextInt( 2, 6);
        Thread[] threads = new Thread[threadCount];
        final CountDownLatch startingGun = new CountDownLatch(1);
        for (int tid=0; tid < threadCount; tid++) {
            final long seed = r.nextLong();
            threads[tid] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(seed);
                        startingGun.await();
                        for (int i = 0; i < 10; i++) {
                            int numInts = r.nextInt( 1, 20000);
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            int theValue = r.nextInt();
                            for (int j = 0; j < numInts; j++) {
                                if (r.nextInt(10) == 0) {
                                    theValue = r.nextInt();
                                }
                                bos.write((byte) (theValue >>> 24));
                                bos.write((byte) (theValue >>> 16));
                                bos.write((byte) (theValue >>> 8));
                                bos.write((byte) theValue);
                            }
                            doTest(bos.toByteArray());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[tid].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }

    public void testRepetitionsS() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numShorts = r.nextInt( 1, 40000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            short theValue = (short) r.nextInt(65535);
            for (int j = 0; j < numShorts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = (short) r.nextInt(65535);
                }
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testMixed() throws IOException {
        Random r = random();
        LineFileDocs lineFileDocs = new LineFileDocs(r);
        for (int i = 0; i < 2; ++i) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int prevInt = r.nextInt();
            long prevLong = r.nextLong();
            while (bos.size() < 400000) {
                switch (r.nextInt(4)) {
                case 0:
                    addInt(r, prevInt, bos);
                    break;
                case 1:
                    addLong(r, prevLong, bos);
                    break;
                case 2:
                    addString(lineFileDocs, bos);
                    break;
                case 3:
                    addBytes(r, bos);
                    break;
                default:
                    throw new IllegalStateException("Random is broken");
                }
            }
            doTest(bos.toByteArray());
        }
    }

    private void addLong(Random r, long prev, ByteArrayOutputStream bos) {
        long theValue = prev;
        if (r.nextInt(10) != 0) {
            theValue = r.nextLong();
        }
        bos.write((byte) (theValue >>> 56));
        bos.write((byte) (theValue >>> 48));
        bos.write((byte) (theValue >>> 40));
        bos.write((byte) (theValue >>> 32));
        bos.write((byte) (theValue >>> 24));
        bos.write((byte) (theValue >>> 16));
        bos.write((byte) (theValue >>> 8));
        bos.write((byte) theValue);
    }

    private void addInt(Random r, int prev, ByteArrayOutputStream bos) {
        int theValue = prev;
        if (r.nextInt(10) != 0) {
            theValue = r.nextInt();
        }
        bos.write((byte) (theValue >>> 24));
        bos.write((byte) (theValue >>> 16));
        bos.write((byte) (theValue >>> 8));
        bos.write((byte) theValue);
    }

    private void addString(LineFileDocs lineFileDocs, ByteArrayOutputStream bos) throws IOException {
        String s = lineFileDocs.nextDoc().get("body");
        bos.write(s.getBytes(StandardCharsets.UTF_8));
    }

    private void addBytes(Random r, ByteArrayOutputStream bos) throws IOException {
        byte bytes[] = new byte[r.nextInt( 1, 10000)];
        r.nextBytes(bytes);
        bos.write(bytes);
    }

    public void testRepetitionsSThreads() throws Exception {
        final Random r = random();
        int threadCount = r.nextInt( 2, 6);
        Thread[] threads = new Thread[threadCount];
        final CountDownLatch startingGun = new CountDownLatch(1);
        for (int tid=0; tid < threadCount; tid++) {
            final long seed = r.nextLong();
            threads[tid] = new Thread() {
                @Override
                public void run() {
                    try {
                        Random r = new Random(seed);
                        startingGun.await();
                        for (int i = 0; i < 10; i++) {
                            int numShorts = r.nextInt( 1, 40000);
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            short theValue = (short) r.nextInt(65535);
                            for (int j = 0; j < numShorts; j++) {
                                if (r.nextInt(10) == 0) {
                                    theValue = (short) r.nextInt(65535);
                                }
                                bos.write((byte) (theValue >>> 8));
                                bos.write((byte) theValue);
                            }
                            doTest(bos.toByteArray());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            threads[tid].start();
        }
        startingGun.countDown();
        for (Thread t : threads) {
            t.join();
        }
    }

    private void doTest(byte bytes[]) throws IOException {
        InputStream rawIn = new ByteArrayInputStream(bytes);
        Compressor c = compressor;

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final Random r = random();
        int bufferSize = r.nextBoolean() ? 65535 : r.nextInt(1, 70000);
        int prepadding = r.nextInt(70000);
        int postpadding = r.nextInt(70000);
        byte[] buffer = new byte[prepadding + bufferSize + postpadding];
        int len;
        try (OutputStream os = c.threadLocalOutputStream(bos)) {
            r.nextBytes(buffer); // fill block completely with junk
            while ((len = rawIn.read(buffer, prepadding, bufferSize)) != -1) {
                os.write(buffer, prepadding, len);
            }
        }
        rawIn.close();

        // now we have compressed byte array
        InputStream in = c.threadLocalInputStream(new ByteArrayInputStream(bos.toByteArray()));

        // randomize constants again
        bufferSize = r.nextBoolean() ? 65535 : r.nextInt(1, 70000);
        prepadding = r.nextInt(70000);
        postpadding = r.nextInt(70000);
        buffer = new byte[prepadding + bufferSize + postpadding];
        r.nextBytes(buffer); // fill block completely with junk

        ByteArrayOutputStream uncompressedOut = new ByteArrayOutputStream();
        while ((len = in.read(buffer, prepadding, bufferSize)) != -1) {
            uncompressedOut.write(buffer, prepadding, len);
        }
        uncompressedOut.close();

        assertThat(bytes).isEqualTo(uncompressedOut.toByteArray());
    }
}
