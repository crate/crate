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

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.parser;

/**
        This node represents a like comparison operator (no escape)

        If the like pattern is a constant or a parameter then if possible
        the like is modified to include a >= and < operator. In some cases
        the like can be eliminated.  By adding =, >= or < operators it may
        allow indexes to be used to greatly narrow the search range of the
        query, and allow optimizer to estimate number of rows to affected.


        constant or parameter LIKE pattern with prefix followed by optional wild 
        card e.g. Derby%

        CHAR(n), VARCHAR(n) where n < 255

                >=   prefix padded with '\u0000' to length n -- e.g. Derby\u0000\u0000
                <=   prefix appended with '\uffff' -- e.g. Derby\uffff

                [ can eliminate LIKE if constant. ]


        CHAR(n), VARCHAR(n), LONG VARCHAR where n >= 255

                >= prefix backed up one characer
                <= prefix appended with '\uffff'

                no elimination of like


        parameter like pattern starts with wild card e.g. %Derby

        CHAR(n), VARCHAR(n) where n <= 256

                >= '\u0000' padded with '\u0000' to length n
                <= '\uffff'

                no elimination of like

        CHAR(n), VARCHAR(n), LONG VARCHAR where n > 256

                >= NULL

                <= '\uffff'


        Note that the Unicode value '\uffff' is defined as not a character value
        and can be used by a program for any purpose. We use it to set an upper
        bound on a character range with a less than predicate. We only need a single
        '\uffff' appended because the string 'Derby\uffff\uffff' is not a valid
        String because '\uffff' is not a valid character.

**/

public final class LikeEscapeOperatorNode extends TernaryOperatorNode
{

    /**
     * Initializer for a LikeEscapeOperatorNode
     *
     * receiver like pattern [ escape escapeValue ]
     *
     * @param receiver          The left operand of the like: 
     *                                                          column, CharConstant or Parameter
     * @param leftOperand       The right operand of the like: the pattern
     * @param rightOperand  The optional escape clause, null if not present
     */
    public void init(Object receiver,
                     Object leftOperand,
                     Object rightOperand)
    {
        /* By convention, the method name for the like operator is "like" */
        super.init(receiver, leftOperand, rightOperand, 
                   TernaryOperatorNode.OperatorType.LIKE, null); 
    }

}
