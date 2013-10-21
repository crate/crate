/*
 *Copyright 2012 Carrot Search s.c..
 *
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *Unless required by applicable law or agreed to in writing, software
 *distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *See the License for the specific language governing permissions and
 *limitations under the License.
 */
package com.carrotsearch.junitbenchmarks.db;

import com.carrotsearch.junitbenchmarks.AutocloseConsumer;
import com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties;
import com.carrotsearch.junitbenchmarks.Result;
import java.io.*;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Parent class for database consumer implementations.
 */
public abstract class DbConsumer extends AutocloseConsumer implements Closeable
{
    /*
     * Column indexes in the prepared insert statement.
     */
    public final static int RUN_ID, CLASSNAME, NAME, BENCHMARK_ROUNDS, WARMUP_ROUNDS,
        ROUND_AVG, ROUND_STDDEV, GC_AVG, GC_STDDEV, GC_INVOCATIONS, GC_TIME,
        TIME_BENCHMARK, TIME_WARMUP;
    
    static
    {
        int column = 1;
        RUN_ID = column++;
        CLASSNAME = column++;
        NAME = column++;
        BENCHMARK_ROUNDS = column++;
        WARMUP_ROUNDS = column++;
        ROUND_AVG = column++;
        ROUND_STDDEV = column++;
        GC_AVG = column++;
        GC_STDDEV = column++;
        GC_INVOCATIONS = column++;
        GC_TIME = column++;
        TIME_BENCHMARK = column++;
        TIME_WARMUP = column++;
    }
    
    /**
     * The database connection.
     */
    private Connection connection;
    
    /**
     * Unique primary key for this consumer in the RUNS table.
     */
    private int runId = -1;
    
    /**
     * Insert statement to the tests table.
     */
    private PreparedStatement newTest;
    
    /**
     * The charts directory.
     */
    private File chartsDir;
    
    /**
     * Charting visitors.
     */
    private List<IChartAnnotationVisitor> chartVisitors;
    
    /**
     * The custom key value.
     */
    private String customKeyValue;

    /**
     * Creates a new DbConsumer.
     *
     * @param chartsDir the charts directory
     * @param customKeyValue the custom key value
     */
    public DbConsumer(File chartsDir, String customKeyValue)
    {
        this.chartsDir = chartsDir;
        this.customKeyValue = customKeyValue;
        this.chartVisitors = newChartVisitors();
        AutocloseConsumer.addAutoclose(this);
    }
    
    /*
     *
     */
    private List<IChartAnnotationVisitor> newChartVisitors()
    {
        List<IChartAnnotationVisitor> visitors = new ArrayList<IChartAnnotationVisitor>();
        visitors.add(new MethodChartVisitor());
        visitors.add(new HistoryChartVisitor());
        return visitors;
    }
    
    /**
     * Accept a single benchmark result.
     */
    @Override
    public void accept(Result result)
    {
        // Visit chart collectors.
        final Class<?> clazz = result.getTestClass();
        final Method method = result.getTestMethod();
        for (IChartAnnotationVisitor v : chartVisitors)
        {
            v.visit(clazz, method, result);
        }
        try
        {
            PreparedStatement testInsertStatement = getTestInsertStatement();
            testInsertStatement.setString(CLASSNAME, result.getTestClassName());
            testInsertStatement.setString(NAME, result.getTestMethodName());
            testInsertStatement.setInt(BENCHMARK_ROUNDS, result.benchmarkRounds);
            testInsertStatement.setInt(WARMUP_ROUNDS, result.warmupRounds);
            testInsertStatement.setDouble(ROUND_AVG, result.roundAverage.avg);
            testInsertStatement.setDouble(ROUND_STDDEV, result.roundAverage.stddev);
            testInsertStatement.setDouble(GC_AVG, result.gcAverage.avg);
            testInsertStatement.setDouble(GC_STDDEV, result.gcAverage.stddev);
            testInsertStatement.setInt(GC_INVOCATIONS, (int) result.gcInfo.accumulatedInvocations());
            testInsertStatement.setDouble(GC_TIME, result.gcInfo.accumulatedTime() / 1000.0);
            testInsertStatement.setDouble(TIME_WARMUP, result.warmupTime / 1000.0);
            testInsertStatement.setDouble(TIME_BENCHMARK, result.benchmarkTime / 1000.0);
            testInsertStatement.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Error while saving the benchmark result to H2.", e);
        }
    }
    
    /**
     * Close the database connection and finalizeWhereClause transaction.
     */
    @Override
    public void close()
    {
        try
        {
            if (connection != null)
            {
                if (!connection.isClosed())
                {
                    doClose();
                }
                connection = null;
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to close DB consumer.", e);
        }
    }
    
    /**
     * Rollback all performed operations on request.
     */
    public void rollback()
    {
        try
        {
            connection.rollback();
            this.chartVisitors = newChartVisitors();
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Could not rollback.", e);
        }
    }
    
    /**
     * Retrieve DB version.
     */
    public DbVersions getDbVersion() throws SQLException
    {
        Statement s = getConnection().createStatement();
        ResultSet rs = s.executeQuery("SHOW TABLES");
        Set<String> tables = new HashSet<String>();
        while (rs.next())
        {
            tables.add(rs.getString(1));
        }
    
        if (!tables.contains("DBVERSION"))
        {
            if (tables.contains("RUNS"))
            {
                return DbVersions.VERSION_1;
            }
    
            return DbVersions.UNINITIALIZED;
        }
    
        DbVersions version;
        rs = s.executeQuery("SELECT VERSION FROM DBVERSION");
        if (!rs.next())
        {
            throw new RuntimeException("Missing version row in DBVERSION table.");
        }
    
        version = DbVersions.fromInt(rs.getInt(1));
        if (rs.next()) {
            throw new RuntimeException("More than one row in DBVERSION table.");
        }
    
        return version;
    }
    
    /**
     * Read a given resource from classpath and return UTF-8 decoded string.
     */
    protected static String getResource(Class<?> c, String resourceName)
    {
        try
        {
            InputStream is = c.getResourceAsStream(resourceName);
            if (is == null)
            {
                throw new IOException();
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final byte[] buffer = new byte[1024];
            int cnt;
            while ((cnt = is.read(buffer)) > 0)
            {
                baos.write(buffer, 0, cnt);
            }
            is.close();
            baos.close();

            return new String(baos.toByteArray(), "UTF-8");
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Required resource missing: "
                    + resourceName);
        }
    }
    
    /**
     * Gets the default custom key.
     *
     * @return the default custom key
     */
    public static String getDefaultCustomKey()
    {
        return System.getProperty(BenchmarkOptionsSystemProperties.CUSTOMKEY_PROPERTY);
    }

    /**
     * Gets the default charts directory
     *
     * @return the default charts directory
     */
    public static File getDefaultChartsDir()
    {
        final File file = new File(System.getProperty(BenchmarkOptionsSystemProperties.CHARTS_DIR_PROPERTY, "."));
        if (file.getParentFile() != null)
        {
            file.getParentFile().mkdirs();
        }
        return file;
    }
    
    /**
     * @return Create a row for this consumer's test run.
     */
    private int getRunID(String customKeyValue) throws SQLException
    {
        PreparedStatement s = getConnection().prepareStatement(
                getNewRunSql(), Statement.RETURN_GENERATED_KEYS);
        s.setString(1, System.getProperty("java.runtime.version", "?"));
        s.setString(2, System.getProperty("os.arch", "?"));
        s.setString(3, customKeyValue);
        s.executeUpdate();

        ResultSet rs = s.getGeneratedKeys();
        if (!rs.next())
        {
            throw new SQLException("No autogenerated keys?");
        }
        final int key = rs.getInt(1);
        if (rs.next())
        {
            throw new SQLException("More than one autogenerated key?");
        }

        rs.close();
        s.close();

        return key;
    }
    
    /**
     * Do finalizeWhereClause the consumer; close db connection and emit reports.
     */
    private void doClose() throws Exception
    {
        try
        {
            for (IChartAnnotationVisitor v : chartVisitors)
            {
                v.generate(this);
            }
        } finally
        {
            if (!connection.isClosed())
            {
                connection.commit();
                connection.close();
            }
        }
    }
    
    /**
     * Check database schema and create it if needed.
     */
    protected void checkSchema() throws SQLException
    {
        DbVersions dbVersion = getDbVersion();
        Statement s = connection.createStatement();
        switch (dbVersion)
        {
            case UNINITIALIZED:
                s.execute(getCreateRunsSql());
                s.execute(getCreateTestsSql());
            // fall-through.
            case VERSION_1:
                s.execute(getCreateDbVersionSql());
                s.execute(getAddCustomKeySql());
                updateDbVersion(DbVersions.VERSION_2);
            // fall-through
            case VERSION_2:
                break;

            default:
                throw new RuntimeException("Unexpected database version: "
                        + dbVersion);
        }
        connection.commit();
    }
    
    /**
     * Update database version.
     */
    private void updateDbVersion(DbVersions newVersion) throws SQLException
    {
        Statement s = getConnection().createStatement();
        s.executeUpdate("DELETE FROM DBVERSION");
        s.executeUpdate("INSERT INTO DBVERSION (VERSION) VALUES (" + newVersion.version + ")");
    }
    
    /**
     * Gets the connection, instantiating it if necessary.
     */
    public Connection getConnection() throws SQLException
    {
        if (connection == null)
        {
            connection = createConnection();
        }
        return connection;
    }

    /**
     * Gets the custom key value.
     *
     * @return the custom key value
     */
    public String getCustomKeyValue()
    {
        return customKeyValue;
    }

    /**
     * Gets the charts directory.
     *
     * @return the charts directory
     */
    public File getChartsDir()
    {
        return chartsDir;
    }

    /**
     * Gets the history HTML template.
     *
     * @return the history HTML template
     */
    public String getHistoryHtmlTemplate()
    {
        return getResource(DbConsumer.class, "HistoryChartGenerator.html");
    }

    /**
     * Gets the method HTML template.
     *
     * @return the method html template
     */
    public String getMethodHtmlTemplate()
    {
        return getResource(DbConsumer.class, "MethodChartGenerator.html");
    }

    /**
     * Gets the run ID, lazy-loading it if it was not already read.
     *
     * @return the run Id
     * @throws java.sql.SQLException if the run ID cannot be determined
     */
    public int getRunId() throws SQLException
    {
        if (runId == -1)
        {
            runId = getRunID(customKeyValue);
        }
        return runId;
    }

    /**
     * Lazy-loads the test insert statement.
     *
     * @return the test insert statement
     * @throws java.sql.SQLException if the test insert statement cannot be created
     */
    protected PreparedStatement getTestInsertStatement() throws SQLException
    {
        if (newTest == null)
        {
            newTest = getConnection().prepareStatement(getTestInsertSql());
            newTest.setInt(RUN_ID, getRunId());
        }
        return newTest;
    }

    /**
     * Gets the SQL for obtaining method chart properties.
     *
     * @return the SQL for obtaining method chart properties
     */
    public abstract String getMethodChartPropertiesQuery();

    /**
     * Gets the SQL for obtaining method chart results.
     *
     * @return the SQL for obtaining method chart results
     */
    public abstract String getMethodChartResultsQuery();

    /**
     * Gets the SQL for creating the tests table.
     *
     * @return the SQL for creating the tests table
     */
    protected abstract String getCreateTestsSql();

    /**
     * Gets the SQL for inserting into the test table.
     *
     * @return the SQL for inserting into the test table
     */
    protected abstract String getTestInsertSql();

    /**
     * Gets the SQL for creating the runs table.
     *
     * @return the SQL for creating the runs table
     */
    protected abstract String getCreateRunsSql();

    /**
     * Gets the SQL for inserting into the runs table.
     *
     * @return the SQL for inserting into the runs table
     */
    protected abstract String getNewRunSql();

    /**
     * Gets the SQL for creating the DB Version table.
     *
     * @return the SQL for creating the DB version table
     */
    protected abstract String getCreateDbVersionSql();

    /**
     * Gets the SQL for adding a custom key.
     *
     * @return the SQL for adding a custom key
     */
    protected abstract String getAddCustomKeySql();

    /**
     * Instantiates the database connection.
     *
     * @return a new, open database connection
     * @throws java.sql.SQLException if the database connection cannot be created
     */
    protected abstract Connection createConnection() throws SQLException;
}
