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

package io.crate.sql.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import io.crate.sql.parser.antlr.v4.QuotedSubscriptExpressionLexer;
import io.crate.sql.tree.Node;

public class QuotedSubscriptExpressionParser {
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                                int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    public static Node detectQuotedSubscriptExpressions(String expression) {
        try {
            return invokeParser("subscript expression", expression);
        } catch (Exception e) {
            return null;
        }
    }

    private static Node invokeParser(String name, String expression) {
        try {
            var lexer = new QuotedSubscriptExpressionLexer(
                new CaseInsensitiveStream(CharStreams.fromString(expression, name)));
            var tokenStream = new CommonTokenStream(lexer);
            var parser = new io.crate.sql.parser.antlr.v4.QuotedSubscriptExpressionParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parser.quotedSubscriptExpression();
            } catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parser.quotedSubscriptExpression();
            }

            return new QuotedSubscriptExpressionASTBuilder().visit(tree);
        } catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }
}
