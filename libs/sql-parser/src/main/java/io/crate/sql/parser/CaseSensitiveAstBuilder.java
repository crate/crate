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

import io.crate.sql.parser.antlr.v4.SqlBaseParser;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.StringLiteral;

/**
 * For Postgres compatibility, it has been the norm that unquoted identifiers are converted to lower cases.
 * See {@link AstBuilder#visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext)}
 * 
 * However, in some cases the original case sensitivity should be preserved.
 * For example, a generated reference, "col1 AS MySchema.arr_max(..)" stored in the metadata as a string
 * needs to be converted back to {@link io.crate.sql.tree.Expression} as part of building {@link DocTableInfo}.
 * And {@link AstBuilder} would convert "MySchema" to "myschema".
 */
public class CaseSensitiveAstBuilder extends AstBuilder {

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
        return new StringLiteral(context.getText());
    }
}
