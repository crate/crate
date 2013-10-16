package com.carrotsearch.junitbenchmarks.db;

import com.carrotsearch.junitbenchmarks.Escape;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Report generator utilities.
 */
final class GeneratorUtils
{
    /**
     * Literal 'CLASSNAME'.
     */
    private final static Pattern CLASSNAME_PATTERN = 
        Pattern.compile("CLASSNAME", Pattern.LITERAL);

    /**
     * Return the index of a column labeled <code>name</code>. If no explicit
     * label (SQL's <code>AS</code>) is given, the name defaults to the database
     * column's name.
     */
    public static int getColumnIndex(ResultSet rs, String label)
        throws SQLException
    {
        final ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++)
        {
            if (label.equals(md.getColumnLabel(i)))
                return i;
        }
        throw new RuntimeException("No column labeled: " + label);
    }

    /**
     * Get extra properties associated with the given run. 
     */
    static String getProperties(final DbConsumer consumer) throws SQLException
    {
        Connection connection = consumer.getConnection();
        int runId = consumer.getRunId();
        final StringBuilder buf = new StringBuilder();

        final PreparedStatement s = 
            connection.prepareStatement(consumer.getMethodChartPropertiesQuery());
        s.setInt(1, runId);

        ResultSet rs = s.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next())
        {   
            for (int i = 1; i <= metaData.getColumnCount(); i++)
            {
                final Object obj = rs.getObject(i);
                if (obj == null)
                    continue;
    
                buf.append(metaData.getColumnLabel(i));
                buf.append(": ");
                buf.append(obj);
                buf.append("\n");
            }
        }

        rs.close();
        s.close();

        return Escape.htmlEscape(buf.toString());
    }

    /**
     * Format a given SQL value to be placed in JSON script (add quotes as needed). 
     */
    static Object formatValue(int sqlColumnType, Object val)
    {
        switch (sqlColumnType)
        {
            case Types.VARCHAR:
                return "\"" + Escape.jsonEscape(val.toString()) + "\"";
            case Types.DOUBLE:
            case Types.FLOAT:
                return String.format("%.6f", val).replace(',', '.');
            case Types.NUMERIC:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return val;
        }
        throw new RuntimeException("Unsupported column type: " + sqlColumnType);
    }

    /**
     * Get Google Chart API type for a given SQL type. 
     */
    public static String getMappedType(int sqlColumnType)
    {
        switch (sqlColumnType)
        {
            case Types.VARCHAR:
                return "string";
    
            case Types.NUMERIC:
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return "number";
        }
        throw new RuntimeException("Unsupported column type: " + sqlColumnType);
    }

    /**
     * Preprocess a given template and substitute a fixed token.
     */
    static String replaceToken(String template, String key, String replacement)
    {
        Pattern p = Pattern.compile(key, Pattern.LITERAL);
        return p.matcher(template).replaceAll(Matcher.quoteReplacement(replacement)); 
    }

    /**
     * Save an output resource to a given file. 
     */
    static void save(String fileName, String content) throws IOException
    {
        final File file = new File(fileName);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        final FileOutputStream fos = new FileOutputStream(file);
        fos.write(content.getBytes("UTF-8"));
        fos.close();
    }

    static String getMinMax(double min, double max)
    {
        StringBuilder b = new StringBuilder();
        if (!Double.isNaN(min))
        {
            b.append("min: " + min + ",");
        }
    
        if (!Double.isNaN(max))
        {
            b.append("max: " + max + ",");
        }
    
        return b.toString();
    }

    /**
     * Process file prefix for charts.
     * 
     * @param clazz Chart's class.
     * @param filePrefix File prefix annotation's value (may be empty).
     * @param chartsDir Parent directory for chart output files.
     * @return Fully qualified file name prefix (absolute).
     */
    public static String getFilePrefix(Class<?> clazz, String filePrefix, File chartsDir)
    {
        if (filePrefix == null || filePrefix.trim().equals(""))
        {
            filePrefix = clazz.getName();
        }

        filePrefix = CLASSNAME_PATTERN.matcher(filePrefix).replaceAll(
            Matcher.quoteReplacement(clazz.getName()));

        if (!new File(filePrefix).isAbsolute())
        {
            // For relative prefixes, attach parent directory.
            filePrefix = new File(chartsDir, filePrefix).getAbsolutePath();
        }

        return filePrefix;
    }
}
