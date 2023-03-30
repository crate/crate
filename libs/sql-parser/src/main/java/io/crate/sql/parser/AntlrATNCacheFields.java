/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.sql.parser;

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.Locale;

import static java.util.Objects.requireNonNull;

public final class AntlrATNCacheFields {
    private final ATN atn;
    private final PredictionContextCache predictionContextCache;
    private final DFA[] decisionToDFA;

    public AntlrATNCacheFields(ATN atn) {
        this.atn = requireNonNull(atn, "atn is null");
        this.predictionContextCache = new PredictionContextCache();
        this.decisionToDFA = createDecisionToDFA(atn);
    }

    @SuppressWarnings("ObjectEquality")
    public void configureLexer(Lexer lexer) {
        requireNonNull(lexer, "lexer is null");
        // Intentional identity equals comparison
        if (atn != lexer.getATN()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Lexer ATN mismatch: expected %s, found %s", atn, lexer.getATN())
            );
        }
        lexer.setInterpreter(new LexerATNSimulator(lexer, atn, decisionToDFA, predictionContextCache));
    }

    @SuppressWarnings("ObjectEquality")
    public void configureParser(Parser parser) {
        requireNonNull(parser, "parser is null");
        // Intentional identity equals comparison
        if (atn != parser.getATN()) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Parser ATN mismatch: expected %s, found %s", atn, parser.getATN())
            );
        }
        parser.setInterpreter(new ParserATNSimulator(parser, atn, decisionToDFA, predictionContextCache));
    }

    private static DFA[] createDecisionToDFA(ATN atn) {
        DFA[] decisionToDFA = new DFA[atn.getNumberOfDecisions()];
        for (int i = 0; i < decisionToDFA.length; i++) {
            decisionToDFA[i] = new DFA(atn.getDecisionState(i), i);
        }
        return decisionToDFA;
    }
}
