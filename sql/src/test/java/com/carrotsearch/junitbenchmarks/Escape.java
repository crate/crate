package com.carrotsearch.junitbenchmarks;

/**
 * Entity escaping.
 */
public final class Escape
{
    public Escape()
    {
        // no instances.
    }

    /*
     * NOTE. I leave these unimplemented for now; there shouldn't be any problems with
     * escaping for method and class names because these are compiler-verified. If there
     * are problems in the future, it'll be easier to escape such strings in one place (or
     * redirect to Apache Commons Lang, for example).
     */

    /**
     * Escape special HTML entities.
     */
    public static String htmlEscape(String string)
    {
        // TODO: implement me.
        return string;
    }

    /**
     * Escape a JSON string.
     */
    public static String jsonEscape(String string)
    {
        // TODO: implement me.
        return string;
    }

    /**
     * Escape an SQL string.
     */
    public static Object sqlEscape(String string)
    {
        // TODO: implement me.
        return string;
    }

    /**
     * Escape XML attribute's value.
     */
    public static String xmlAttrEscape(String string)
    {
        // TODO: implement me.
        return string;
    }
}
