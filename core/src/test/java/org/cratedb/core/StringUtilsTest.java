package org.cratedb.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringUtilsTest {
    @Test
    public void testDottedToSQLPath() {
        assertEquals("a['b']", StringUtils.dottedToSqlPath("a.b"));
        assertEquals("a", StringUtils.dottedToSqlPath("a"));
        assertEquals("a['']", StringUtils.dottedToSqlPath("a."));
        assertEquals("a['b']['c']", StringUtils.dottedToSqlPath("a.b.c"));
    }

    @Test
    public void testSQLToDottedPath() {
        assertEquals("a.b", StringUtils.sqlToDottedPath("a['b']"));
        assertEquals("a", StringUtils.sqlToDottedPath("a"));
        assertEquals("a.", StringUtils.sqlToDottedPath("a['']"));
        assertEquals("a.b.c", StringUtils.sqlToDottedPath("a['b']['c']"));
        assertEquals("[]", StringUtils.sqlToDottedPath("[]"));
        assertEquals("a[abc]", StringUtils.sqlToDottedPath("a[abc]"));
    }
}
