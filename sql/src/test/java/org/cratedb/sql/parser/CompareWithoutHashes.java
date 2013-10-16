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

package org.cratedb.sql.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompareWithoutHashes
{
    public static final String HASH_REGEX = "[\\p{Alnum}]*\\@[\\p{XDigit}]+";

    private Pattern pattern;
    private Map<String,String> equivalences;

    public CompareWithoutHashes() {
        this(HASH_REGEX);
    }

    public CompareWithoutHashes(String regex) {
        this.pattern = Pattern.compile(regex);
        this.equivalences = new HashMap<String,String>();
    }

    public boolean match(Reader r1, Reader r2) throws IOException {
        BufferedReader br1 = new BufferedReader(r1);
        BufferedReader br2 = new BufferedReader(r2);
        while (true) {
            String l1 = br1.readLine();
            String l2 = br2.readLine();

            if (l1 == null) {
                if (l2 == null)
                    break;
                l1 = "";
            }
            else if (l2 == null)
                l2 = "";
            if (!match(l1, l2)) 
                return false;
        }
        return true;
    }

    public boolean match(String s1, String s2) {
        String[][] ha1 = findHashes(s1);
        String[][] ha2 = findHashes(s2);
        if (ha1.length != ha2.length)
            return false;
        for (int i = 0; i < ha1.length; i++) {
            if (!ha1[i][0].equals(ha2[i][0])) // checks that the part before the hash matches
            	return false;
            String oh2 = equivalences.put(ha1[i][1], ha2[i][1]);
            if ((oh2 != null) && !oh2.equals(ha2[i][1]))
                return false;
        }

        // It's possible that equivalences swaps two matches, so need intermediate.
        for (int i = 0; i < ha1.length; i++) {
            s1 = s1.replace(ha1[i][1], "%!" + i + "!%");
        }
        for (int i = 0; i < ha1.length; i++) {
            s1 = s1.replace("%!" + i + "!%", ha2[i][1]);
        }
        
        return s1.equals(s2);
    }

    protected String[][] findHashes(String s) {
        Matcher matcher = pattern.matcher(s);
        List<String[]> matches = new ArrayList<String[]>();
        while (matcher.find()) {
            String hash = matcher.group();
            String[] match = {
                hash.substring(0,hash.indexOf('@')),
                hash
            };
            matches.add(match);
        }
        return matches.toArray(new String[matches.size()][2]);
    }

    public String converter(String s1, String s2) {
        String[][] ha1 = findHashes(s1);
        String[][] ha2 = findHashes(s2);
        int i1 = 0;
        for (int i2 = 0; i1 < ha1.length && i2 < ha2.length; i1++) {
            if (ha1[i1][0].equals(ha2[i2][0]))
                s1 = s1.replace(ha1[i1][1], "%!" + i2 + "!%");
            else {
                adjust:
                for (int ia1 = i1; ia1 < Math.min(i1+2,ha1.length); ia1++) {
                    for (int ia2 = i2; ia2 < Math.min(i2+2,ha2.length); ia2++) {
                        if (ha1[ia1][0].equals(ha2[ia2][0])) {
                            s1 = s1.replace(ha1[ia1][1], "%!" + ia2 + "!%");
                            i1 = ia1;
                            i2 = ia2;
                            break adjust;
                        }
                    }
                }
            }
            i2++;
        }
        for (int i = 0; i < ha2.length; i++) {
            s1 = s1.replace("%!" + i + "!%", ha2[i][1]);
        }
        return s1;
    }
}
