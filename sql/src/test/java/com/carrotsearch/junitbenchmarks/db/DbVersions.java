package com.carrotsearch.junitbenchmarks.db;

/**
 * Database versions for upgrades.
 */
public enum DbVersions
{
    /**
     * No tables present.
     */
    UNINITIALIZED(0),
    
    /**
     * Runs and tests tables, initial.
     */
    VERSION_1(1),

    /**
     * DBVERSION present. Modifications:
     * <pre>
     * ALTER TABLE RUNS ADD CUSTOM_KEY VARCHAR(500);
     * </pre>
     */
    VERSION_2(2);

    public final int version;

    DbVersions(int v)
    {
        this.version = v;
    }

    /**
     * Return
     */
    public static DbVersions fromInt(int ver)
    {
        for (DbVersions v : DbVersions.values())
        {
            if (v.version == ver) return v;
        }
        throw new RuntimeException("No such DB number: " + ver);
    }
}
