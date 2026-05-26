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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.elasticsearch.test.IntegTestCase;
import org.jspecify.annotations.Nullable;
import org.junit.Test;

import io.crate.test.integration.SQLLogicParser;
import io.crate.test.integration.SQLLogicParser.Cmd;
import io.crate.test.integration.SQLLogicParser.QueryCmd;
import io.crate.test.integration.SQLLogicParser.StatementCmd;
import io.crate.testing.SqlLogic;
import io.crate.testing.UseJdbc;

/**
 * Runs sqllogic style *.test files against CrateDB
 */
@IntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SQLLogicITest extends IntegTestCase {

    private void runFile(Path file, @Nullable String testName) {
        String schema = "doc";
        try (var cmds = SQLLogicParser.parse(file)) {
            boolean dmlDone = false;
            Iterator<Cmd> iterator = cmds.iterator();
            while (iterator.hasNext()) {
                Cmd cmd = iterator.next();
                switch (cmd) {
                    case StatementCmd stmt -> {
                        try {
                            execute(stmt.getQuery(), schema);
                        } catch (Exception e) {
                            if (stmt.isExpectOk()) {
                                throw new SQLLogicParser.IncorrectResultException(e.getMessage());
                            }
                        }
                    }
                    case QueryCmd query -> {
                        if (testName == null || query.testName().equals(testName)) {
                            if (!dmlDone) {
                                dmlDone = true;
                                refreshTables(schema);
                            }
                            var response = execute(query.getQuery(), schema);
                            query.validate(response);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } finally {
            dropRelations(schema);
        }
    }

    @Test
    @UseJdbc(1)
    public void testFiles() throws Exception {
        Predicate<Path> fileFilter = path -> path.toString().endsWith(".test");
        final String[] testName = new String[] {null};
        SqlLogic annotation = getTestAnnotation(SqlLogic.class);
        if (annotation != null) {
            if (annotation.file().isEmpty() == false) {
                fileFilter = path -> annotation.file().equals(path.getFileName().toString());
            }
            if (annotation.testName().isEmpty() == false) {
                testName[0] = annotation.testName();
            }
        }

        Path testsLocation = getDataPath("/integtests");
        try (Stream<Path> files = Files.list(testsLocation)) {
            files
                .filter(fileFilter)
                .forEach(path -> runFile(path, testName[0]));
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
