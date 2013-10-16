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

package org.cratedb.sql.parser.parser;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.util.BitSet;
import java.io.IOException;
import java.io.StringReader;

public class StringCharStreamTest
{
    // The contract of a CharStream is simple, but prey to off-by-one errors.
    // Simplest test is comparing against existing JavaCC-inspired implementation.
    // Except that the stream also fixes a few bugs having to do with position at EOF,

    private CharStream s1, s2;
    private static final String STRING = "abc xyz\n1\t2\t3\r\nxxx   yyy\rz";
    private static final char EOF = (char)0;

    @Before
    public void openStreams() {
        s1 = new UCode_CharStream(new StringReader(STRING), 1, 1);
        s2 = new StringCharStream(STRING);
    }

    @After
    public void closeStreams() {
        s1.Done();
        s2.Done();
    }

    @Test
    public void testRead() {
        while (true) {
            char c = read();
            if (c == EOF) break;
        }
    }

    @Test
    public void testBeginTokenAll() {
        while (true) {
            char c = beginToken();
            if (c == EOF) break;
        }
    }

    @Test
    public void testBeginTokenSome() {
        char c = ' ';
        while (true) {
            if (" \t\r\n".indexOf(c) < 0)
                c = read();
            else
                c = beginToken();
            if (c == EOF) break;
        }
    }

    @Test
    public void testBackup() {
        BitSet ns = new BitSet(STRING.length());
        ns.set(0);
        while (true) {
            while (true) {
                char c = read();
                if (c == EOF) break;
            }
            int i = ns.nextClearBit(0);
            if (i >= STRING.length()) break;
            ns.set(i);
            backup(i);
        }
    }

    protected char beginToken() {
        char c1, c2;
        try {
            c1 = s1.BeginToken();
        }
        catch (IOException ex) {
            c1 = EOF;
        }
        try {
            c2 = s2.BeginToken();
        }
        catch (IOException ex) {
            c2 = EOF;
        }
        assertEquals("BeginToken", c1, c2);
        compare((c1 == EOF), (c1 == EOF) || (c1 == '\t'));
        return c1;
    }

    protected char read() {
        char c1, c2;
        try {
            c1 = s1.readChar();
        }
        catch (IOException ex) {
            c1 = EOF;
        }
        try {
            c2 = s2.readChar();
        }
        catch (IOException ex) {
            c2 = EOF;
        }
        assertEquals("readChar", c1, c2);
        compare();
        return c1;
    }

    protected void backup(int amount) {
        s1.backup(amount);
        s2.backup(amount);
        compare();
    }

    protected void compare() {
        compare(false, false);
    }

    protected void compare(boolean expectBeginOffsetEOF,
                           boolean expectBeginColumnDifference) {
        int offset = s2.getEndOffset();
        if (expectBeginOffsetEOF)
            assertEquals("getBeginOffset["+offset+"]", s1.getBeginOffset(), s2.getBeginOffset()-1);
        else
            assertEquals("getBeginOffset["+offset+"]", s1.getBeginOffset(), s2.getBeginOffset());
        assertEquals("getEndOffset["+offset+"]", s1.getEndOffset(), s2.getEndOffset());
        assertEquals("getColumn["+offset+"]", s1.getColumn(), s2.getColumn());
        assertEquals("getLine["+offset+"]", s1.getLine(), s2.getLine());
        if (!expectBeginColumnDifference)
            assertEquals("getBeginColumn["+offset+"]", s1.getBeginColumn(), s2.getBeginColumn());
        assertEquals("getBeginLine["+offset+"]", s1.getBeginLine(), s2.getBeginLine());
        assertEquals("getEndColumn["+offset+"]", s1.getEndColumn(), s2.getEndColumn());
        assertEquals("getEndLine["+offset+"]", s1.getEndLine(), s2.getEndLine());
        if (expectBeginOffsetEOF)
            assertEquals("GetImage["+offset+"]", "", s2.GetImage());
        else
            assertEquals("GetImage["+offset+"]", s1.GetImage(), s2.GetImage());
        int size = offset - s2.getBeginOffset() + 1;
        for (int i = 0; i < size; i++)
            assertEquals("GetSuffix["+offset+"]("+i+")", new String(s1.GetSuffix(i)), new String(s2.GetSuffix(i)));
    }

}
