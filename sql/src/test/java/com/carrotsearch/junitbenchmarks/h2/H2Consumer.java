package com.carrotsearch.junitbenchmarks.h2;

import com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties;
import com.carrotsearch.junitbenchmarks.db.DbConsumer;
import org.h2.jdbcx.JdbcDataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * {@link com.carrotsearch.junitbenchmarks.db.DbConsumer} implementation for H2.
 */
public class H2Consumer extends DbConsumer
{

    /**
     * The database file name.
     */
    protected File dbFileName;

    /**
     * Creates a consumer with the default file name.
     */
    public H2Consumer()
    {
        this(getDefaultDbName());
    }

    /**
     * Creates a consumer with the default charts and custom key dirs.
     *
     * @param dbFileName the database file name
     */
    public H2Consumer(File dbFileName)
    {
        this(dbFileName, getDefaultChartsDir(), getDefaultCustomKey());
    }

    /**
     * Creates a consumer with the specified database file, charts directory,
     * and custom key value.
     *
     * @param dbFileName the database file
     * @param chartsDir the charts directory
     * @param customKeyValue the custom key value
     */
    public H2Consumer(File dbFileName, File chartsDir, String customKeyValue)
    {
        super(chartsDir, customKeyValue);
        this.dbFileName = dbFileName;
        try
        {
            checkSchema();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Cannot initialize H2 database.", e);
        }
    }

    /**
     * Return the global default DB name.
     */
    protected static File getDefaultDbName()
    {
        final String dbPath = System.getProperty(BenchmarkOptionsSystemProperties.DB_FILE_PROPERTY);
        if (dbPath != null && !dbPath.trim().equals(""))
        {
            return new File(dbPath);
        }
        throw new IllegalArgumentException("Missing global property: "
                + BenchmarkOptionsSystemProperties.DB_FILE_PROPERTY);
    }

    @Override
    protected Connection createConnection() throws SQLException
    {
        final JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:" + dbFileName.getAbsolutePath() + ";DB_CLOSE_ON_EXIT=FALSE");
        ds.setUser("sa");
        Connection results = ds.getConnection();
        results.setAutoCommit(false);
        return results;
    }

    @Override
    public String getMethodChartResultsQuery()
    {
        return getResource(H2Consumer.class, "method-chart-results.sql");
    }

    @Override
    public String getMethodChartPropertiesQuery()
    {
        return getResource(H2Consumer.class, "method-chart-properties.sql");
    }

    @Override
    protected String getCreateRunsSql()
    {
        return getResource(H2Consumer.class, "000-create-runs.sql");
    }

    @Override
    protected String getCreateTestsSql()
    {
        return getResource(H2Consumer.class, "001-create-tests.sql");
    }

    @Override
    protected String getNewRunSql()
    {
        return getResource(H2Consumer.class, "002-new-run.sql");
    }

    @Override
    protected String getTestInsertSql()
    {
        return getResource(H2Consumer.class, "003-new-result.sql");
    }

    @Override
    protected String getCreateDbVersionSql()
    {
        return getResource(H2Consumer.class, "004-create-dbversion.sql");
    }

    @Override
    protected String getAddCustomKeySql()
    {
        return getResource(H2Consumer.class, "005-add-custom-key.sql");
    }
}
