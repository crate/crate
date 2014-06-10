/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.autocomplete;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.sql.parser.CaseInsensitiveStream;
import io.crate.sql.parser.ParsingException;
import io.crate.sql.parser.StatementLexer;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.Token;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Class that can be used to retrieve possible completions for an incomplete SQL statement.
 *
 * The AutoCompleter will tokenize the Statement String using a Lexer and will then analyze it and
 * retrieve possible completions using a {@link io.crate.autocomplete.DataProvider}
 */
public class AutoCompleter {

    private ESLogger logger = Loggers.getLogger(getClass());
    static final ImmutableList<String> START_KEYWORDS = ImmutableList.of(
            "select",
            "update",
            "delete",
            "set",
            "create",
            "drop"
    );

    private final DataProvider dataProvider;
    private final Joiner dotJoiner = Joiner.on(".");

    public AutoCompleter(DataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    public ListenableFuture<CompletionResult> complete(String statement) {
        StatementLexer lexer = getLexer(statement);
        Token token;
        Token nextToken = null;
        List<ListenableFuture<List<String>>> futureCompletions = new ArrayList<>();
        final Context ctx = new Context(statement.length());

        while (true) {
            if (nextToken == null) {
                token = lexer.nextToken();
            } else {
                token = nextToken;
            }
            if (token.getType() == Token.EOF) {
                break;
            } else {
                try {
                    nextToken = lexer.nextToken();
                } catch (ParsingException e) {
                    // as soon as an opening single quote is encountered the nextToken can't be retrieved unless there
                    // is also a closing single quote :(
                    if (e.getMessage().endsWith("mismatched character '<EOF>' expecting '''")) {
                        // assume it is a subscript expression
                        int i = statement.lastIndexOf("['");
                        if (i == statement.length()) {
                            futureCompletions.addAll(getCompletions(ctx, ctx.previousIdent + "['"));
                        } else if (i > 0) {
                            futureCompletions.addAll(getCompletions(ctx, ctx.previousIdent + statement.substring(i)));
                        }
                    }
                    break;
                }
            }
            String tokenText = token.getText();
            boolean lastToken = nextToken.getType() == Token.EOF;

            switch (token.getType()) {
                case StatementLexer.FROM:
                    ctx.previousKeywordToken = StatementLexer.FROM;
                    ctx.visitedFromTable = true;
                    break;
                case StatementLexer.TABLE:
                    ctx.visitedFromTable = true;
                    ctx.table = tokenText;
                    break;
                case StatementLexer.WHERE:
                    ctx.previousKeywordToken = StatementLexer.WHERE;
                    break;
                case StatementLexer.ORDER_BY:
                    ctx.previousKeywordToken = StatementLexer.ORDER_BY;
                    break;
                case StatementLexer.SELECT:
                    ctx.startKeyword = "select";
                    break;
                case StatementLexer.WS:
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, ""));
                    }
                    ctx.parts.clear();
                    break;
                case StatementLexer.T__276:
                    assert tokenText.equals(".") : "T_276 token must be a \".\"";
                    if (ctx.schema == null) {
                        ctx.schema = ctx.previousIdent;
                    }
                    if (!ctx.visitedFromTable && (ctx.table == null || ctx.schema.equals(ctx.table))) {
                        ctx.table = ctx.previousIdent;
                    }
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, ""));
                    }
                    break;
                case StatementLexer.T__279:
                    assert tokenText.equals("[") : "T_279 token must be a \"[\"";
                    ctx.visitedOpeningSquareBracket = true;
                    tokenText = ctx.previousIdent + tokenText;
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, tokenText));
                    } else {
                        ctx.previousIdent = tokenText;
                    }
                    break;
                case StatementLexer.T__280:
                    assert tokenText.equals("]") : "T__280 token must be a \"]\"";
                    ctx.visitedOpeningSquareBracket = false;
                    tokenText = ctx.previousIdent + "'" + tokenText;
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, tokenText));
                    } else {
                        ctx.previousIdent = tokenText;
                    }
                case StatementLexer.STRING:
                    if (ctx.visitedOpeningSquareBracket) {
                        tokenText = ctx.previousIdent + "'" + tokenText;
                        if (lastToken) {
                            futureCompletions.addAll(getCompletions(ctx, tokenText));
                        } else {
                            ctx.previousIdent = tokenText;
                        }
                    }
                    break;
                case StatementLexer.QUOTED_IDENT:
                    ctx.parts.add(String.format("\"%s\"", tokenText));
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, tokenText));
                    } else {
                        if (ctx.previousKeywordToken == StatementLexer.FROM) {
                            ctx.table = tokenText;
                        }
                    }
                    ctx.previousIdent = tokenText;
                    break;
                case StatementLexer.IDENT:
                    ctx.parts.add(tokenText);
                    if (lastToken) {
                        futureCompletions.addAll(getCompletions(ctx, tokenText));
                    } else {
                        if (ctx.previousKeywordToken == StatementLexer.FROM) {
                            ctx.table = tokenText;
                        }
                    }
                    ctx.previousIdent = tokenText;
                    break;
                default:
                    ctx.parts.clear();
                    if (ctx.startKeyword != null) {
                        futureCompletions.add(dataProvider.schemas(tokenText));
                    }
            }
            logger.trace("Token: %s", token);
        }

        final SettableFuture<CompletionResult> result = SettableFuture.create();
        Futures.addCallback(Futures.allAsList(futureCompletions), new FutureCallback<List<List<String>>>() {
            @Override
            public void onSuccess(@Nullable List<List<String>> completionsList) {
                if (completionsList == null) {
                    result.set(new CompletionResult(0, ImmutableList.<String>of()));
                    return;
                }
                if (ctx.parts.size() > 1) {
                    Set<String> fullyQualifiedCompletions = new TreeSet<>();
                    for (List<String> completions : completionsList) {
                        for (String completion : completions) {
                            ctx.parts.set(ctx.parts.size() - 1, completion);
                            fullyQualifiedCompletions.add(dotJoiner.join(ctx.parts));
                        }
                    }
                    result.set(new CompletionResult(ctx.startIdx, fullyQualifiedCompletions));
                } else {
                    Set<String> uniqueSortedCompletions = new TreeSet<>();
                    for (List<String> completions : completionsList) {
                        uniqueSortedCompletions.addAll(completions);
                    }
                    result.set(new CompletionResult(ctx.startIdx, uniqueSortedCompletions));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });
        return result;
    }

    private List<ListenableFuture<List<String>>> getCompletions(Context ctx, String tokenText) {
        ctx.startIdx = calcStartIdx(ctx, tokenText);
        List<ListenableFuture<List<String>>> futureCompletions = new ArrayList<>();
        if (ctx.visitedFromTable) {
            if (ctx.table == null || ctx.table.equals(ctx.schema)) {
                if (ctx.schema == null) {
                    futureCompletions.add(dataProvider.schemas(tokenText));
                }
                futureCompletions.add(dataProvider.tables(ctx.schema, tokenText));
            } else {
                futureCompletions.add(dataProvider.columns(ctx.schema, ctx.table, tokenText));
            }
        } else if (ctx.startKeyword != null) {
            switch (ctx.parts.size()) {
                case 1:
                    // select n -> might be schema, table or column
                    futureCompletions.add(dataProvider.schemas(tokenText));
                    futureCompletions.add(dataProvider.tables(null, tokenText));
                    futureCompletions.add(dataProvider.columns(null, null, tokenText));
                    break;
                case 2:
                    // select a.b -> might be table or column
                    futureCompletions.add(dataProvider.tables(ctx.schema, tokenText));
                    futureCompletions.add(dataProvider.columns(null, ctx.table, tokenText));
                    break;
                case 3:
                    // select a.b.c -> must be column
                    futureCompletions.add(dataProvider.columns(ctx.schema, ctx.table, tokenText));
                    break;
            }
        } else {
            List<String> matchingKeywords = new ArrayList<>();
            for (String keyword : START_KEYWORDS) {
                if (keyword.startsWith(tokenText)) {
                    matchingKeywords.add(keyword);
                }
            }
            futureCompletions.add(Futures.immediateFuture(matchingKeywords));
        }
        return futureCompletions;
    }

    private int calcStartIdx(Context ctx, String tokenText) {
        int numParts = ctx.parts.size();
        int fullTokenLength = tokenText.length();
        for (int i = 0; i < numParts - 1; i++) {
            fullTokenLength += ctx.parts.get(i).length() + 1;
        }
        return ctx.statementLength - fullTokenLength;
    }

    private static class Context {
        private final int statementLength;

        public List<String> parts = new ArrayList<>();
        public String startKeyword = null;
        public String table = null;
        public String schema = null;
        public int previousKeywordToken = 0;
        public String previousIdent = null;
        public boolean visitedFromTable = false;
        public boolean visitedOpeningSquareBracket = false;
        public int startIdx = 0;

        public Context(int statementLength) {
            this.statementLength = statementLength;
        }
    }

    private StatementLexer getLexer(String statement) {
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(statement));
        return new StatementLexer(stream);
    }
}
