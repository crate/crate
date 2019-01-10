/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;

import java.util.HashSet;
import java.util.Set;

public class CharGroupTokenizerFactory extends AbstractTokenizerFactory{

    private final Set<Integer> tokenizeOnChars = new HashSet<>();
    private boolean tokenizeOnSpace = false;
    private boolean tokenizeOnLetter = false;
    private boolean tokenizeOnDigit = false;
    private boolean tokenizeOnPunctuation = false;
    private boolean tokenizeOnSymbol = false;

    public CharGroupTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);

        for (final String c : settings.getAsList("tokenize_on_chars")) {
            if (c == null || c.length() == 0) {
                throw new RuntimeException("[tokenize_on_chars] cannot contain empty characters");
            }

            if (c.length() == 1) {
                tokenizeOnChars.add((int) c.charAt(0));
            }
            else if (c.charAt(0) == '\\') {
                tokenizeOnChars.add((int) parseEscapedChar(c));
            } else {
                switch (c) {
                    case "letter":
                        tokenizeOnLetter = true;
                        break;
                    case "digit":
                        tokenizeOnDigit = true;
                        break;
                    case "whitespace":
                        tokenizeOnSpace = true;
                        break;
                    case "punctuation":
                        tokenizeOnPunctuation = true;
                        break;
                    case "symbol":
                        tokenizeOnSymbol = true;
                        break;
                    default:
                        throw new RuntimeException("Invalid escaped char in [" + c + "]");
                }
            }
        }
    }

    private char parseEscapedChar(final String s) {
        int len = s.length();
        char c = s.charAt(0);
        if (c == '\\') {
            if (1 >= len)
                throw new RuntimeException("Invalid escaped char in [" + s + "]");
            c = s.charAt(1);
            switch (c) {
                case '\\':
                    return '\\';
                case 'n':
                    return '\n';
                case 't':
                    return '\t';
                case 'r':
                    return '\r';
                case 'b':
                    return '\b';
                case 'f':
                    return '\f';
                case 'u':
                    if (len > 6) {
                        throw new RuntimeException("Invalid escaped char in [" + s + "]");
                    }
                    return (char) Integer.parseInt(s.substring(2), 16);
                default:
                    throw new RuntimeException("Invalid escaped char " + c + " in [" + s + "]");
            }
        } else {
            throw new RuntimeException("Invalid escaped char [" + s + "]");
        }
    }

    @Override
    public Tokenizer create() {
        return new CharTokenizer() {
            @Override
            protected boolean isTokenChar(int c) {
                if (tokenizeOnSpace && Character.isWhitespace(c)) {
                    return false;
                }
                if (tokenizeOnLetter && Character.isLetter(c)) {
                    return false;
                }
                if (tokenizeOnDigit && Character.isDigit(c)) {
                    return false;
                }
                if (tokenizeOnPunctuation && CharMatcher.Basic.PUNCTUATION.isTokenChar(c)) {
                    return false;
                }
                if (tokenizeOnSymbol && CharMatcher.Basic.SYMBOL.isTokenChar(c)) {
                    return false;
                }
                return !tokenizeOnChars.contains(c);
            }
        };
    }
}
