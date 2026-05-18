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

package io.crate.integrationtests;

import static io.crate.test.integration.SQLLogicParser.parseCmd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.test.integration.SQLLogicParser;
import io.crate.testing.UseJdbc;

/**
 * Runs sqllogic style *.test files against CrateDB
 */
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SQLLogicITest extends IntegTestCase {

    private void runFile(Path file) {
        String schema = "doc";
        try (BufferedReader br = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            List<List<String>> commands = SQLLogicParser.getCommands(br).stream()
                .toList();

            boolean dmlDone = false;
            try {
                for (List<String> cmd : commands) {
                    SQLLogicParser.Cmd parsed = parseCmd(cmd, file.toString());
                    if (parsed instanceof SQLLogicParser.StatementCmd stmt) {
                        try {
                            execute(stmt.getQuery(), schema);
                        } catch (Exception e) {
                            if (stmt.isExpectOk()) {
                                throw new SQLLogicParser.IncorrectResultException(e.getMessage());
                            }
                        }
                    }
                    if (parsed instanceof SQLLogicParser.QueryCmd query) {
                        if (!dmlDone) {
                            dmlDone = true;
                            refreshTables(schema);
                        }

                        execute(query.getQuery(), schema);
                        int colCount = response.cols().length;
                        List<List<Object>> rows = new ArrayList<>();
                        for (Object[] resultRow : response.rows()) {
                            List<Object> row = new ArrayList<>(colCount);
                            for (int c = 0; c < colCount; c++) {
                                if (resultRow[c] == null) {
                                    row.add("NULL");
                                } else {
                                    String raw = resultRow[c].toString();
                                    SQLLogicParser.ColumnFormat fmt = query.resultFormats.get(c % query.resultFormats.size());
                                    row.add(fmt.format(raw));
                                }
                            }
                            rows.add(row);
                        }

                        if (query.sort == SQLLogicParser.SortMode.ROWSORT) {
                            rows.sort(SQLLogicParser::lexicographicByString);
                        }

                        List<Object> actual;
                        if (query.sort == SQLLogicParser.SortMode.ROWS) {
                            actual = new ArrayList<>(rows);
                        } else {
                            actual = new ArrayList<>(rows.size() * Math.max(1, colCount));
                            for (List<Object> r : rows) {
                                actual.addAll(r);
                            }
                        }

                        if (query.sort == SQLLogicParser.SortMode.VALUESORT) {
                            // Explicit lambda avoids relying on overload resolution
                            // for the polymorphic String.valueOf method reference.
                            actual.sort(Comparator.comparing(String::valueOf));
                        }

                        query.validator.validate(actual);

                    }
                }
            } finally {
                dropRelations(schema);
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Test
    @UseJdbc(1)
    public void testFiles() throws Exception {
        Path testsLocation = getDataPath("/integtests");
        try (Stream<Path> files = Files.list(testsLocation)) {
            files
                .filter(path -> path.toString().endsWith(".test"))
                .forEach(this::runFile);
        }
    }

    private void refreshTables(String schema) {
        List<String> tables = listRelations(schema, "BASE TABLE");
        for (String table : tables) {
            execute("refresh table " + table);
        }
    }

    private void dropRelations(String schema) {
        List<String> tables = listRelations(schema, "BASE TABLE");
        for (String table : tables) {
            execute("drop table " + table);
        }
        List<String> views = listRelations(schema, "VIEW");
        for (String view : views) {
            execute("drop view " + view);
        }
    }

    private List<String> listRelations(String schema, String type) {
        execute("select table_schema, table_name from information_schema.tables " +
                "where table_schema = ? and table_type = ?", new Object[]{schema, type});
        List<String> tables = new ArrayList<>((int) response.rowCount());
        for (Object[] row : response.rows()) {
            tables.add('"' + row[0].toString() + "\".\"" + row[1].toString() + '"');
        }
        return tables;
    }
}
