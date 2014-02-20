/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.cratedb.sql.parser;

import org.junit.ComparisonFailure;
import org.junit.Ignore;

import java.io.*;
import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

@Ignore
public class TestBase
{
    protected TestBase() {
    }

    protected String caseName, sql, expected, error;

    protected TestBase(String caseName, String sql, String expected, String error) {
        this.caseName = caseName;
        this.sql = sql;
        this.expected = expected;
        this.error = error;
    }

    public static File[] listSQLFiles(File dir) {
        File[] result = dir.listFiles(new RegexFilenameFilter(".*\\.sql"));
        Arrays.sort(result, new Comparator<File>() {
                        public int compare(File f1, File f2) {
                            return f1.getName().compareTo(f2.getName());
                        }
                    });
        return result;
    }

    public static File changeSuffix(File sqlFile, String suffix) {
        return new File(sqlFile.getParentFile(),
                        sqlFile.getName().replace(".sql", suffix));
    }

    public static String fileContents(File file) throws IOException {
        FileReader reader = null;
        try {
            reader = new FileReader(file);
            StringBuilder str = new StringBuilder();
            char[] buf = new char[128];
            while (true) {
                int nc = reader.read(buf);
                if (nc < 0) break;
                str.append(buf, 0, nc);
            }
            return str.toString();
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    public static String[] fileContentsArray(File file) throws IOException {
        FileReader reader = null;
        List<String> result = new ArrayList<String>();
        try {
            reader = new FileReader(file);
            BufferedReader buffered = new BufferedReader(reader);
            while (true) {
                String line = buffered.readLine();
                if (line == null) break;
                result.add(line);
            }
        }
        finally {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ex) {
                }
            }
        }
        return result.toArray(new String[result.size()]);
    }

    public static Collection<Object[]> sqlAndExpected(File dir) 
            throws IOException {
        return sqlAndExpected(dir, false);
    }

    public static Collection<Object[]> sqlAndExpectedAndParams(File dir) 
            throws IOException {
        return sqlAndExpected(dir, true);
    }
    
    static final boolean RUN_FAILING_TESTS = Boolean.getBoolean("cratedb.sql.parser.test" +
            ".runFailing");

    public static Collection<Object[]> sqlAndExpected(File dir, 
                                                      boolean andParams)
            throws IOException {
        Collection<Object[]> result = new ArrayList<Object[]>();
        for (File sqlFile : listSQLFiles(dir)) {
            String caseName = sqlFile.getName().replace(".sql", "");
            if (changeSuffix(sqlFile, ".fail").exists() && !RUN_FAILING_TESTS)
                continue;
            String sql = fileContents(sqlFile);
            String expected, error;
            File expectedFile = changeSuffix(sqlFile, ".expected");
            if (expectedFile.exists())
                expected = fileContents(expectedFile);
            else
                expected = null;
            File errorFile = changeSuffix(sqlFile, ".error");
            if (errorFile.exists())
                error = fileContents(errorFile);
            else
                error = null;
            if (andParams) {
                String[] params = null;
                File paramsFile = changeSuffix(sqlFile, ".params");
                if (paramsFile.exists()) {
                    params = fileContentsArray(paramsFile);
                }
                result.add(new Object[] {
                               caseName, sql, expected, error, params
                           });
            }
            else {
                result.add(new Object[] {
                               caseName, sql, expected, error
                           });
            }
        }
        return result;
    }

    /** A class implementing this can call {@link #generateAndCheckResult(). */
    public interface GenerateAndCheckResult {
        public String generateResult() throws Exception;
        public void checkResult(String result) throws IOException;
    }

    public static void generateAndCheckResult(GenerateAndCheckResult handler,
                                              String caseName, 
                                              String expected, String error) 
            throws Exception {
        if ((expected != null) && (error != null)) {
            fail(caseName + ": both expected result and expected error specified.");
        }
        String result = null;
        Exception errorResult = null;
        try {
            result = handler.generateResult();
        }
        catch (Exception ex) {
            errorResult = ex;
        }
        if (error != null) {
            if (errorResult == null)
                fail(caseName + ": error expected but none thrown");
            else
                assertEquals(caseName, error, errorResult.toString());
        }
        else if (errorResult != null) {
            throw errorResult;
        }
        else if (expected == null) {
            fail(caseName + " no expected result given. actual='" + result + "'");
        }
        else {
            handler.checkResult(result);
        }
    }

    /** @see GenerateAndCheckResult */
    protected void generateAndCheckResult() throws Exception {
        generateAndCheckResult((GenerateAndCheckResult)this, caseName, expected, error);
    }

    public static void assertEqualsWithoutHashes(String caseName,
                                                 String expected, String actual) 
            throws IOException {
        assertEqualsWithoutPattern(caseName, 
                                   expected, actual, 
                                   CompareWithoutHashes.HASH_REGEX);
    }

    public static void assertEqualsWithoutPattern(String caseName,
                                                  String expected, String actual, 
                                                  String regex) 
            throws IOException {
        CompareWithoutHashes comparer = new CompareWithoutHashes(regex);
        if (!comparer.match(new StringReader(expected), new StringReader(actual)))
            throw new ComparisonFailure(caseName, comparer.converter(expected,actual), actual);
    }

}
