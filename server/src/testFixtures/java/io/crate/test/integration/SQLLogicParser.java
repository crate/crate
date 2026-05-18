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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/// Partial port of sqllogictest.py from crate-qa
///
/// Parses [sqllogic test files](https://www.sqlite.org/sqllogic/doc/trunk/about.wiki)
/// into a list of [Cmd]. Those commands (statement or query) are supposed to be executed
/// against CrateDB and their result can then be validated using [ResultValidator#validate(List)]
public final class SQLLogicParser {

    private static final Pattern VARCHAR_RE = Pattern.compile("VARCHAR\\(\\d+\\)");
    private static final Pattern HASHING_RE =
        Pattern.compile("(\\d+) values hashing to ([a-z0-9]+)");

    SQLLogicParser() {}

    /** Replace {@code VARCHAR(n)} with {@code STRING} */
    static String varcharToString(String sql) {
        return VARCHAR_RE.matcher(sql).replaceAll("STRING");
    }

    /** Thrown when the actual cursor result does not match the expected one. */
    public static final class IncorrectResultException extends RuntimeException {

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
    public abstract static class Cmd {

        /** The SQL text (without the statement/query header). */
        public abstract String getQuery();
    }

    /** A {@code statement ok|error} block. */
    public static final class StatementCmd extends Cmd {
        private final boolean expectOk;
        private final String query;

        StatementCmd(List<String> lines) {
            this.expectOk = lines.getFirst().endsWith("ok");
            this.query = String.join("\n", lines.subList(1, lines.size()));
        }

        @Override
        public String getQuery() {
            return varcharToString(query);
        }

        @Override
        public String toString() {
            return "Statement<" + truncate(query, 30) + ">";
        }

        public boolean isExpectOk() {
            return expectOk;
        }
    }

    /** A {@code query <formats> <sort> [<label>]} block, optionally followed by results. */
    public static final class QueryCmd extends Cmd {
        private final String query;
        public final List<ColumnFormat> resultFormats;
        public final SortMode sort;
        // null when there is no result block at all.
        private final List<String> rawExpected;
        private final String filename;
        public final ResultValidator validator;

        QueryCmd(List<String> lines, String filename) {
            this.filename = filename;

            // Header: "query <formats> <sort> [<label>]"
            String[] header = lines.getFirst().split("\\s+");
            String formatsStr = header[1];
            String sortStr = header[2];

            for (char c : formatsStr.toCharArray()) {
                if (c != 'I' && c != 'R' && c != 'T') {
                    throw new IllegalArgumentException(
                        "Invalid result format codes: " + formatsStr + "\n" + lines);
                }
            }
            List<ColumnFormat> formats = new ArrayList<>(formatsStr.length());
            for (char c : formatsStr.toCharArray()) {
                formats.add(ColumnFormat.valueOf(String.valueOf(c)));
            }
            this.resultFormats = Collections.unmodifiableList(formats);
            this.sort = SortMode.fromToken(sortStr);

            // Find the '----' separator and split the body.
            int sep = -1;
            for (int i = 1; i < lines.size(); i++) {
                if (lines.get(i).startsWith("---")) {
                    sep = i;
                    break;
                }
            }
            if (sep == -1) {
                this.query = String.join(" ", lines.subList(1, lines.size()));
                this.rawExpected = null;
            } else {
                this.query = String.join(" ", lines.subList(1, sep));
                this.rawExpected = new ArrayList<>(lines.subList(sep + 1, lines.size()));
            }

            this.validator = initValidator();
        }

        private ResultValidator initValidator() {
            if (rawExpected == null) {
                return _ -> {};
            }
            if (rawExpected.size() == 1) {
                Matcher m = HASHING_RE.matcher(rawExpected.getFirst());
                if (m.matches()) {
                    int expectedValues = Integer.parseInt(m.group(1));
                    String expectedHash = m.group(2);
                    return rows -> validateHash(rows, expectedValues, expectedHash, filename);
                }
            }
            List<Object> expected = (sort == SortMode.ROWS)
                ? formatExpectedRows(rawExpected)
                : formatExpectedFlat(rawExpected);
            return actual -> validateCmpResult(actual, expected, query, filename);
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
                    ColumnFormat fmt = resultFormats.get(j % resultFormats.size());
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

        @Override
        public String getQuery() {
            return varcharToString(query);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Query<");
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

    @FunctionalInterface
    public interface ResultValidator {
        void validate(List<Object> rows);
    }

    private static void validateHash(List<Object> rows,
                                     int expectedValues,
                                     String expectedHash,
                                     String filename) {
        int got = rows.size();
        if (got != expectedValues) {
            throw new IncorrectResultException(
                "Expected " + expectedValues + " values, got " + got);
        }
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            for (Object row : rows) {
                md.update(String.valueOf(row).getBytes(StandardCharsets.US_ASCII));
                md.update((byte) '\n');
            }
            String digest = bytesToHex(md.digest());
            if (!digest.equals(expectedHash)) {
                throw new IncorrectResultException(
                    "[" + filename + "] Expected values hashing to " + expectedHash
                        + ". Got " + digest + "\n" + rows);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IncorrectResultException(e.getMessage());
        }
    }

    private static void validateCmpResult(List<Object> actual,
                                          List<Object> expected,
                                          String query,
                                          String filename) {
        if (!actual.equals(expected)) {
            throw new IncorrectResultException(
                "[" + filename + "] Expected rows: " + expected + ". Got " + actual
                    + " running " + query);
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

    /** Group raw lines into commands (separated by blank lines). */
    public static List<List<String>> getCommands(BufferedReader br) throws IOException {
        List<List<String>> out = new ArrayList<>();
        List<String> current = new ArrayList<>();
        String line;
        while ((line = br.readLine()) != null) {
            if (line.startsWith("#") || line.startsWith("hash-threshold")) {
                continue;
            }
            if (!line.isEmpty()) {
                current.add(line);
            } else if (!current.isEmpty()) {
                out.add(current);
                current = new ArrayList<>();
            }
        }
        if (!current.isEmpty()) {
            out.add(current);
        }
        return out;
    }

    /** Parse a command into a {@link StatementCmd} or {@link QueryCmd}. */
    public static Cmd parseCmd(List<String> cmd, String filename) {
        // Strip leading skipif/onlyif lines.
        List<String> lines = new ArrayList<>(cmd);
        while (!lines.isEmpty()
            && (lines.getFirst().startsWith("skipif")
            || lines.getFirst().startsWith("onlyif"))) {
            lines.removeFirst();
        }
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("Empty command after stripping skipif/onlyif: " + cmd);
        }
        String type = lines.getFirst();
        if (type.startsWith("statement")) {
            return new StatementCmd(lines);
        }
        if (type.startsWith("query")) {
            return new QueryCmd(lines, filename);
        }
        throw new IllegalArgumentException("Could not parse command: " + cmd);
    }
}
