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

package org.cratedb.sql.parser.parser;

import java.io.EOFException;
import java.io.IOException;

/**
 * {@link CharStream} that simply reads from a string.
 */
public class StringCharStream implements CharStream
{
    private static final IOException EOF = new EOFException();

    private String string;
    private int beginIndex, currentIndex; // 0-based, exclusive end.
    private int currentLine, currentColumn; // 1-based.
    // End represents the position of the last character returned, and
    // in particular if a newline was returned, it at the end of the
    // previous line.
    private int beginLine, beginColumn, endLine, endColumn;
    
    public StringCharStream(String string) {
        init(string);
    }

    public void ReInit(String string) {
        init(string);
    }

    private void init(String string) {
        this.string = string;
        beginIndex = currentIndex = 0;
        currentLine = currentColumn = beginLine = beginColumn = endLine = endColumn = 1;
    }
    
    @Override
    public char BeginToken() throws java.io.IOException {
        beginIndex = currentIndex;
        beginLine = currentLine;
        beginColumn = currentColumn;
        return readChar();
    }

    @Override
    public char readChar() throws java.io.IOException {
        if (currentIndex >= string.length())
            throw EOF;

        return advance();
    }

    @Override
    public void backup(int amount) {
        int target = currentIndex - amount;
        assert (target >= beginIndex);
        currentIndex = beginIndex;
        currentLine = beginLine;
        currentColumn = beginColumn;
        while (currentIndex < target)
            advance();          // Adjusting line / column.
    }

    private char advance() {
        endLine = currentLine;
        endColumn = currentColumn;
        char ch = string.charAt(currentIndex++);
        switch (ch) {
        case '\r':
            if ((currentIndex < string.length()) &&
                (string.charAt(currentIndex) == '\n')) {
                currentColumn++;
                break;
            }
            /* else falls through (bare CR) */
        case '\n':
            currentLine++;
            currentColumn = 1;
            break;
        case '\t':
            endColumn += (8 - (endColumn & 7));
            currentColumn = endColumn + 1;
            break;
        default:
            currentColumn++;
            break;
        }
        return ch;
    }

    @Override
    public int getBeginOffset() {
        return beginIndex;
    }
    @Override
    public int getEndOffset() {
        return currentIndex - 1;   // Want inclusive.
    }

    @Override
    public int getBeginLine() {
        return beginLine;
    }
    @Override
    public int getBeginColumn() {
        return beginColumn;
    }

    @Override
    public int getEndLine() {
        return endLine;
    }
    @Override
    public int getEndColumn() {
        return endColumn;
    }

    @Override
    public int getLine() {
        return getEndLine();
    }
    @Override
    public int getColumn() {
        return getEndColumn();
    }

    @Override
    public String GetImage() {
        return string.substring(beginIndex, currentIndex);
    }

    @Override
    public char[] GetSuffix(int len) {
        char[] result = new char[len];
        string.getChars(currentIndex - len, currentIndex, result, 0);
        return result;
    }

    @Override
    public void Done() {
    }

}
