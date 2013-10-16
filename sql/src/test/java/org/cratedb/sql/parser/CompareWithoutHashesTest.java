/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cratedb.sql.parser;

import static org.junit.Assert.*;
import org.junit.*;

public class CompareWithoutHashesTest {
    
    public CompareWithoutHashesTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of match method, of class CompareWithoutHashes.
     */
    @Test
    public void testMatch_String_String() {
        System.out.println("match");
        String s1 = "ABC@123";
        String s2 = "XYZ@456";
        CompareWithoutHashes instance = new CompareWithoutHashes();
        boolean expResult = false;
        boolean result = instance.match(s1, s2);
        assertEquals(result, expResult);
    }

    /**
     * Test of converter method, of class CompareWithoutHashes.
     */
    @Test
    public void testConverter() {
        System.out.println("converter");
        String s1 = "ABC@123 XYZ@789";
        String s2 = "XYZ@456";
        CompareWithoutHashes instance = new CompareWithoutHashes();
        String expResult = "ABC@123 XYZ@456";
        String result = instance.converter(s1, s2);
        assertEquals(expResult, result);
    }
}
