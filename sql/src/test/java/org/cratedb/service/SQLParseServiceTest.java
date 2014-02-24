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

package org.cratedb.service;

import org.cratedb.SQLCrateNodesTest;
import org.cratedb.action.sql.ParsedStatement;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class SQLParseServiceTest extends SQLCrateNodesTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }
    private static SQLParseService sqlParseService = null;


    @Before
    public void startSQLParseService() {
        sqlParseService = cluster().getInstance(SQLParseService.class);
    }

    @AfterClass
    public static void cleanUpSQLParseService() {
        sqlParseService = null;
    }

    public void testParse() throws Exception {
        ParsedStatement stmt = sqlParseService.parse("create table t1 (c1 int)");

        // this test just ensures that the ParseService calls a Visitor and begins to parse the statement
        // the actual result isn't really enforced. Feel free to change the expected TableUnknownException
        // later on if something else makes more sense.
    }



    @Test
    public void testUnparseSelect() throws Exception {
        String unparsed = sqlParseService.unparse("SelECT mycol froM mytable wheRe a IS not" +
                " null order by mycol", null);
        assertEquals(
                "SELECT mycol FROM mytable WHERE NOT (a IS NULL) ORDER BY mycol",
                unparsed
        );
    }

    @Test
    public void testUnparseSelectGroupBy() throws Exception {
        String unparsed = sqlParseService.unparse("SELECT sum(mycol) FROM mytable group by " +
                "mycol having a > ?",
                new Object[]{Math.PI});
        assertEquals(
                "SELECT SUM(mycol) FROM mytable GROUP BY mycol HAVING a > 3.141592653589793",
                unparsed
        );
    }

    @Test
    public void testUnparseCreateAnalyzer() throws Exception {
        String unparsed = sqlParseService.unparse("CREATE ANALYZER mygerman EXTENDS german WITH" +
                "( stopwords=['der', ?, ?, 'wer', 'wie', 'was'])", new Object[]{"die", "das"});
        assertEquals(
                "CREATE ANALYZER mygerman EXTENDS german WITH (\"stopwords\"=['der','die','das','wer','wie','was'])",
                unparsed
        );
    }

    @Test
    public void testUnparseCreateAnalyzer2() throws Exception {
        String unparsed = sqlParseService.unparse("CREaTE ANALYZER mycustom (" +
                "tokenizer whitespace," +
                "char_filters (" +
                "  html_strip," +
                "  mymapping (" +
                "    type='mapping'," +
                "    mappings=['a=>b', ?]" +
                "  )" +
                ")," +
                "token_filters WITH (" +
                "  snowball," +
                "  myfilter with (" +
                "    type='ngram'," +
                "    max_ngram=?" +
                "  )" +
                ")" +
                ")", new Object[]{"b=>c", 4});
        assertEquals(
                "CREATE ANALYZER mycustom WITH (" +
                        "TOKENIZER whitespace, " +
                        "TOKEN_FILTERS WITH (snowball, myfilter WITH (\"max_ngram\"=4,\"type\"='ngram')), " +
                        "CHAR_FILTERS WITH (html_strip, mymapping WITH (\"mappings\"=['a=>b','b=>c'],\"type\"='mapping')))",
                unparsed
        );
    }
}
