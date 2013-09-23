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

import org.cratedb.sql.parser.StandardException;

import java.util.Map;

public interface SQLParserContext
{
    /** Check that string literal is not too long. */
    public void checkStringLiteralLengthLimit(String image) throws StandardException;

    /** Check that identifier is not too long. */
    public void checkIdentifierLengthLimit(String identifier) throws StandardException;
    
    /** Mark as returning a parameter. */
    public void setReturnParameterFlag();

    /** Mark as requesting locale. */
    public void setMessageLocale(String locale);

    /** Get a node factory. */
    public NodeFactory getNodeFactory();

    /**
     * Return a map of AST nodes that have already been printed during a
     * compiler phase, so as to be able to avoid printing a node more than once.
     * @see QueryTreeNode#treePrint(int)
     * @return the map
     */
    public Map getPrintedObjectsMap();

    /** Is the given feature enabled for this parser? */
    public boolean hasFeature(SQLParserFeature feature);

    enum IdentifierCase { UPPER, LOWER, PRESERVE };

    /** How are unquoted identifiers standardized? **/
    public IdentifierCase getIdentifierCase();
}
