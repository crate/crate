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

package io.crate.test.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.testing.SQLResponse;

/// Partial port of sqllogictest.py from crate-qa
///
/// Parses [sqllogic test files](https://www.sqlite.org/sqllogic/doc/trunk/about.wiki)
/// into a list of [Cmd]. Those commands (statement or query) are supposed to be executed
/// against CrateDB and their result can then be validated using [Cmd#validate(List)]
public final class SQLLogicParser {

    private static final Pattern HASHING_RE =
        Pattern.compile("(\\d+) values hashing to ([a-z0-9]+)");

    SQLLogicParser() {}

    /** Thrown when the actual result does not match the expected one. */
    public static final class IncorrectResultException extends AssertionError {

        public IncorrectResultException(String message) {
            super(message);
        }
    }

    /** Sort mode declared on the query line */
    public enum SortMode {
        NOSORT,
        ROWSORT,
        VALUESORT,
        /**
         * Custom mode (added in the Python port too): keeps the row/column
         * shape and asserts the database's natural row order. Each expected
         * line is split with {@code "| "} into columns.
         */
        ROWS;

        static SortMode fromToken(String token) {
            return switch (token) {
                case "nosort" -> NOSORT;
                case "rowsort" -> ROWSORT;
                case "valuesort" -> VALUESORT;
                case "rows" -> ROWS;
                default -> throw new IllegalArgumentException("Unknown sort mode: " + token);
            };
        }
    }

    /** Per-column type code from the query line. */
    public enum ColumnFormat {
        I, R, T;

        /** Convert the textual cell representation to a typed Java value. */
        public Object format(String value) {
            switch (this) {
                case I:
                    // Some integer-typed expected values come from numeric/double
                    // columns and serialize as e.g. "1.0". Try long first,
                    // fall back to truncating a double.
                    try {
                        return Long.parseLong(value);
                    } catch (NumberFormatException nfe) {
                        return (long) Double.parseDouble(value);
                    }
                case R:
                    return Double.parseDouble(value);
                case T:
                    return value;
                default:
                    throw new AssertionError(this);
            }
        }
    }

    /** Parsed test command — either a {@link StatementCmd} or {@link QueryCmd}. */
    public abstract static sealed class Cmd permits StatementCmd, QueryCmd {

        /** The SQL text (without the statement/query header). */
        public abstract String getQuery();

        public abstract void validate(SQLResponse response);

        static Cmd parse(Path filename, List<NumberedLine> lines) {
            if (lines.isEmpty()) {
                throw new IllegalArgumentException("Empty command after stripping skipif/onlyif: " + lines);
            }
            NumberedLine first = lines.getFirst();
            if (first.line().startsWith("statement")) {
                String query = Lists.joinOn("\n", lines.subList(1, lines.size()), NumberedLine::line);
                boolean expectOk = first.line().endsWith("ok");
                return new StatementCmd(first.lnum(), expectOk, query);
            }
            if (first.line().startsWith("query")) {
                return new QueryCmd(lines, filename.toString());
            }
            throw new IllegalArgumentException("Could not parse command: " + lines);
        }
    }

    /** A {@code statement ok|error} block. */
    public static final class StatementCmd extends Cmd {

        private final int lnum;
        private final boolean expectOk;
        private final String query;

        StatementCmd(int lnum, boolean expectOk, String query) {
            this.lnum = lnum;
            this.expectOk = expectOk;
            this.query = query;
        }

        @Override
        public String getQuery() {
            return query;
        }

        @Override
        public String toString() {
            return "Statement<line:" + lnum + ", q: " + truncate(query, 30) + ">";
        }

        public boolean isExpectOk() {
            return expectOk;
        }

        @Override
        public void validate(SQLResponse response) {
        }
    }

    /** A {@code query <formats> <sort> [<label>]} block, optionally followed by results. */
    public static final class QueryCmd extends Cmd {
        private final String query;
        private final int lnum;

        private final List<ColumnFormat> resultFormats;
        private final SortMode sort;

        /// null when there is no result block at all.
        @Nullable
        private final List<String> rawExpected;
        private final String filename;
        private final String testName;

        QueryCmd(List<NumberedLine> lines, String filename) {
            this.filename = filename;

            // Header: "query <formats> <sort> [<label>]"
            NumberedLine first = lines.getFirst();
            this.lnum = first.lnum();
            String[] header = first.line().split("\\s+");
            String formatsStr = header[1];
            String sortStr = header[2];
            this.testName = header[3];

            for (char c : formatsStr.toCharArray()) {
                if (c != 'I' && c != 'R' && c != 'T') {
                    throw new IllegalArgumentException(
                        "Invalid result format codes: " + formatsStr + "\n" + lines);
                }
            }
            this.resultFormats = new ArrayList<>(formatsStr.length());
            for (char c : formatsStr.toCharArray()) {
                resultFormats.add(ColumnFormat.valueOf(String.valueOf(c)));
            }
            this.sort = SortMode.fromToken(sortStr);

            // Find the '----' separator and split the body.
            int sep = -1;
            for (int i = 1; i < lines.size(); i++) {
                if (lines.get(i).line().startsWith("---")) {
                    sep = i;
                    break;
                }
            }
            if (sep == -1) {
                this.query = Lists.joinOn(" ", lines.subList(1, lines.size()), NumberedLine::line);
                this.rawExpected = null;
            } else {
                this.query = Lists.joinOn(" ", lines.subList(1, sep), NumberedLine::line);
                this.rawExpected = new ArrayList<>(Lists.map(lines.subList(sep + 1, lines.size()), NumberedLine::line));
            }
        }

        @Override
        public void validate(SQLResponse response) {
            if (rawExpected == null) {
                return;
            }
            if (rawExpected.size() == 1) {
                Matcher m = HASHING_RE.matcher(rawExpected.getFirst());
                if (m.matches()) {
                    int expectedValues = Integer.parseInt(m.group(1));
                    String expectedHash = m.group(2);
                    validateHash(response, expectedValues, expectedHash, filename, testName, lnum);
                    return;
                }
            }
            List<Object> expected = (sort == SortMode.ROWS)
                ? formatExpectedRows(rawExpected)
                : formatExpectedFlat(rawExpected);
            validateCmpResult(response, expected, query, filename, testName, lnum);
        }

        private void validateCmpResult(SQLResponse response,
                                       List<Object> expected,
                                       String query,
                                       String filename,
                                       String testname,
                                       int lnum) {

            int colCount = response.cols().length;
            if (colCount != resultFormats.size()) {
                throw new IncorrectResultException(String.format(
                    Locale.ENGLISH,
                    "[%s:%d][%s] Expected noCols: %s. Got: %s running %s",
                    filename,
                    lnum,
                    testname,
                    resultFormats.size(),
                    colCount,
                    query
                ));
            }
            List<List<Object>> rows = new ArrayList<>();
            for (Object[] resultRow : response.rows()) {
                List<Object> row = new ArrayList<>(colCount);
                for (int c = 0; c < colCount; c++) {
                    if (resultRow[c] == null) {
                        row.add("NULL");
                    } else {
                        String raw = resultRow[c].toString();
                        ColumnFormat fmt = resultFormats.get(c);
                        row.add(fmt.format(raw));
                    }
                }
                rows.add(row);
            }

            if (sort == SQLLogicParser.SortMode.ROWSORT) {
                rows.sort(SQLLogicParser::lexicographicByString);
            }

            List<Object> actual;
            if (sort == SQLLogicParser.SortMode.ROWS) {
                actual = new ArrayList<>(rows);
            } else {
                actual = new ArrayList<>(rows.size() * Math.max(1, colCount));
                for (List<Object> r : rows) {
                    actual.addAll(r);
                }
            }

            if (sort == SQLLogicParser.SortMode.VALUESORT) {
                // Explicit lambda avoids relying on overload resolution
                // for the polymorphic String.valueOf method reference.
                actual.sort(Comparator.comparing(String::valueOf));
            }

        if (!actual.equals(expected)) {
            throw new IncorrectResultException(String.format(
                Locale.ENGLISH,
                "[%s:%d][%s] Expected rows: %s. Got: %s running %s",
                filename,
                lnum,
                testname,
                expected,
                actual,
                query
            ));
        }
    }

        /** Parse each expected line as a multi-column row separated by "| ". */
        private List<Object> formatExpectedRows(List<String> lines) {
            List<Object> out = new ArrayList<>(lines.size());
            for (String line : lines) {
                String[] cols = line.split(Pattern.quote("| "), -1);
                List<Object> row = new ArrayList<>(cols.length);
                for (int j = 0; j < cols.length; j++) {
                    if ("NULL".equals(cols[j])) {
                        row.add("NULL");
                        continue;
                    }
                    ColumnFormat fmt = resultFormats.get(j);
                    row.add(fmt.format(cols[j]));
                }
                out.add(row);
            }
            return out;
        }

        /** Parse expected lines as flat values, applying per-column types round-robin. */
        private List<Object> formatExpectedFlat(List<String> lines) {
            List<Object> out = new ArrayList<>(lines.size());
            for (int i = 0; i < lines.size(); i++) {
                String v = lines.get(i);
                if (v == null || "NULL".equals(v)) {
                    out.add("NULL");
                } else {
                    ColumnFormat fmt = resultFormats.get(i % resultFormats.size());
                    out.add(fmt.format(v));
                }
            }
            return out;
        }

        public String testName() {
            return testName;
        }

        @Override
        public String getQuery() {
            return query;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Query<");
            sb.append(lnum);
            sb.append(": ");
            for (ColumnFormat f : resultFormats) {
                sb.append(f.name());
            }
            sb.append(", ");
            sb.append(sort.name().toLowerCase());
            sb.append(", ");
            sb.append(truncate(query, 30));
            sb.append(">");

            return sb.toString();
        }
    }

    /** Lexicographic comparator over per-column string representations. */
    public static int lexicographicByString(List<Object> a, List<Object> b) {
        Iterator<Object> it1 = a.iterator();
        Iterator<Object> it2 = b.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            int cmp = String.valueOf(it1.next()).compareTo(String.valueOf(it2.next()));
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(a.size(), b.size());
    }

    private static void validateHash(SQLResponse response,
                                     int expectedValues,
                                     String expectedHash,
                                     String filename,
                                     String testname,
                                     int lnum) {
        long got = response.rowCount();
        if (got != expectedValues) {
            throw new IncorrectResultException(
                "Expected " + expectedValues + " values, got " + got);
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (Object[] row : response.rows()) {
                for (Object value : row) {
                    if (value == null) {
                        value = "NULL";
                    }
                    md.update(String.valueOf(value).getBytes(StandardCharsets.US_ASCII));
                    md.update((byte) '\n');
                }
            }
            String digest = bytesToHex(md.digest());
            if (!digest.equals(expectedHash)) {
                throw new IncorrectResultException(String.format(
                    Locale.ENGLISH,
                    "[%s:%d][%s] Expected values hashing to %s:. Got: %s running %s",
                    filename,
                    lnum,
                    testname,
                    expectedHash,
                    digest,
                    response
                ));
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IncorrectResultException(e.getMessage());
        }
    }



    private static String bytesToHex(byte[] bytes) {
        char[] hex = "0123456789abcdef".toCharArray();
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            out[i * 2] = hex[v >>> 4];
            out[i * 2 + 1] = hex[v & 0x0F];
        }
        return new String(out);
    }

    private static String truncate(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n);
    }

    record NumberedLine(int lnum, String line) {}

    public static Stream<Cmd> parse(Path file) throws IOException {
        AtomicInteger lnum = new AtomicInteger(0);
        Gatherer<NumberedLine, List<NumberedLine>, Cmd> gatherCmds = Gatherer.ofSequential(
            ArrayList::new,
            (state, element, downstream) -> {
                String line = element.line();
                if (!line.isEmpty()) {
                    state.add(element);
                } else if (!state.isEmpty()) {
                    Cmd cmd = Cmd.parse(file, state);
                    state.clear();
                    return downstream.push(cmd);
                }
                return true;
            },
            (state, downstream) -> {
                if (!state.isEmpty()) {
                    downstream.push(Cmd.parse(file, state));
                }
            }
        );
        return Files.lines(file).sequential()
            .map(line -> new NumberedLine(lnum.incrementAndGet(), line))
            .filter(nline -> !(nline.line().startsWith("#") || nline.line().startsWith("hash-treshold")))
            .gather(gatherCmds);
    }
}
