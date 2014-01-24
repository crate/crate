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

package org.cratedb.sql.parser.compiler;

import org.cratedb.sql.parser.TestBase;
import org.cratedb.sql.parser.parser.StatementNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.File;
import java.util.Collection;

import static junit.framework.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BooleanNormalizerTest extends ASTTransformTestBase implements TestBase.GenerateAndCheckResult
{
    public static final File RESOURCE_DIR = 
        new File(ASTTransformTestBase.RESOURCE_DIR, "normalize");

    protected BooleanNormalizer booleanNormalizer;

    @Before
    public void makeNormalizer() throws Exception {
        booleanNormalizer = new BooleanNormalizer(parser);
    }

    @Parameters
    public static Collection<Object[]> statements() throws Exception {
        return sqlAndExpected(RESOURCE_DIR);
    }

    public BooleanNormalizerTest(String caseName, String sql, 
                                 String expected, String error) {
        super(caseName, sql, expected, error);
    }

    @Test
    public void testNormalizer() throws Exception {
        generateAndCheckResult();
    }

    @Override
    public String generateResult() throws Exception {
        StatementNode stmt = parser.parseStatement(sql);
        stmt = booleanNormalizer.normalize(stmt);
        return unparser.toString(stmt);
    }

    @Override
    public void checkResult(String result) {
        assertEquals(caseName, expected, result);
    }

}
