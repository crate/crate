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

package org.cratedb.sql.parser.types;

import org.cratedb.sql.parser.StandardException;

/** Character set and collation for character types. */
public final class CharacterTypeAttributes
{
    public static enum CollationDerivation {
        NONE, IMPLICIT, EXPLICIT
    }

    private String characterSet;
    private String collation;
    private CollationDerivation collationDerivation;

    public CharacterTypeAttributes(String characterSet,
                                   String collation, 
                                   CollationDerivation collationDerivation) {
        this.characterSet = characterSet;
        this.collation = collation;
        this.collationDerivation = collationDerivation;
    }

    public String getCharacterSet() {
        return characterSet;
    }

    public String getCollation() {
        return collation;
    }

    public CollationDerivation getCollationDerivation() {
        return collationDerivation;
    }

    public static CharacterTypeAttributes forCharacterSet(String characterSet) {
        return new CharacterTypeAttributes(characterSet, null, null);
    }

    public static CharacterTypeAttributes forCollation(CharacterTypeAttributes base,
                                                       String collation) {
        String characterSet = null;
        if (base != null)
            characterSet = base.characterSet;
        return new CharacterTypeAttributes(characterSet, 
                                           collation, CollationDerivation.EXPLICIT);
    }

    public static CharacterTypeAttributes mergeCollations(CharacterTypeAttributes ta1,
                                                          CharacterTypeAttributes ta2)
            throws StandardException {
        if ((ta1 == null) || (ta1.collationDerivation == null)) {
            return ta2;
        }
        else if ((ta2 == null) || (ta2.collationDerivation == null)) {
            return ta1;
        }
        else if (ta1.collationDerivation == CollationDerivation.EXPLICIT) {
            if (ta2.collationDerivation == CollationDerivation.EXPLICIT) {
                if (!ta1.collation.equals(ta2.collation))
                    throw new StandardException("Incompatible collations: " +
                                                ta1 + " " + ta1.collation + " and " +
                                                ta2 + " " + ta2.collation);
            }
            return ta1;
        }
        else if (ta2.collationDerivation == CollationDerivation.EXPLICIT) {
            return ta2;
        }
        else if ((ta1.collationDerivation == CollationDerivation.IMPLICIT) &&
                 (ta2.collationDerivation == CollationDerivation.IMPLICIT) &&
                 ta1.collation.equals(ta2.collation)) {
            return ta1;
        }
        else {
            return new CharacterTypeAttributes(null, null, CollationDerivation.NONE);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CharacterTypeAttributes)) return false;
        CharacterTypeAttributes other = (CharacterTypeAttributes)o;
        return (((characterSet == null) ?
                 (other.characterSet == null) :
                 characterSet.equals(other.characterSet)) &&
                ((collation == null) ?
                 (other.collation == null) :
                 collation.equals(other.collation)));
    }

    @Override
    public String toString() {
        if ((characterSet == null) && (collation == null)) return "none";
        StringBuilder str = new StringBuilder();
        if (characterSet != null) {
            str.append("CHARACTER SET ");
            str.append(characterSet);
        }
        if (collation != null) {
            if (characterSet != null) str.append(" ");
            str.append("COLLATE ");
            str.append(collation);
        }
        return str.toString();
    }
    
}
