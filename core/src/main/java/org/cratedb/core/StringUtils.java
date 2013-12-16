package org.cratedb.core;

import com.google.common.base.Splitter;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
    private static final Splitter SPLITTER = Splitter.on('.');
    private static final Joiner JOINER = Joiner.on('.');
    private static final Pattern PATTERN = Pattern.compile("\\['([^\\]])*'\\]");
    private static final Pattern SQL_PATTERN = Pattern.compile("(.+?)(?:\\['([^\\]])*'\\])+");

    public static String dottedToSqlPath(String dottedPath) {
        Iterable<String> splitted = SPLITTER.split(dottedPath);
        Iterator<String> iter = splitted.iterator();
        StringBuilder builder = new StringBuilder(iter.next());
        while (iter.hasNext()) {
            builder.append("['").append(iter.next()).append("']");
        }
        return builder.toString();
    }

    public static String sqlToDottedPath(String sqlPath) {
        if (!SQL_PATTERN.matcher(sqlPath).find()) { return sqlPath; }
        List<String> s = new ArrayList<>();
        int idx = sqlPath.indexOf('[');
        s.add(sqlPath.substring(0, idx));
        Matcher m = PATTERN.matcher(sqlPath);
        while (m.find(idx)) {
            String group = m.group(1);
            if (group == null) { group = ""; }
            s.add(group);
            idx = m.end();
        }

        return JOINER.join(s);
    }
}
