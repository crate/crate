package com.carrotsearch.junitbenchmarks.db;

import java.io.File;
import java.sql.*;

/**
 * Generate a snippet of HTML code for a given class and all of its benchmarked methods. 
 */
public final class MethodChartGenerator
{
    private String clazzName;
    private String filePrefix;
    private DbConsumer consumer;

    double min = Double.NaN, max = Double.NaN;

    /**
     * @param filePrefix Prefix for generated files.
     * @param clazzName The target test class (fully qualified name).
     * @param consumer Database consumer for results.
     */
    public MethodChartGenerator(
        String filePrefix, 
        String clazzName,
        DbConsumer consumer)
    {
        this.clazzName = clazzName;
        this.filePrefix = filePrefix;
        this.consumer = consumer;
    }

    /**
     * Generate the chart's HTML.
     */
    public void generate() throws Exception
    {
        final String jsonFileName = filePrefix + ".jsonp";
        final String htmlFileName = filePrefix + ".html";
        
        String template = consumer.getMethodHtmlTemplate();
        template = GeneratorUtils.replaceToken(template, "CLASSNAME", clazzName);
        template = GeneratorUtils.replaceToken(template, "MethodChartGenerator.jsonp", new File(jsonFileName).getName());
        template = GeneratorUtils.replaceToken(template, "/*MINMAX*/", GeneratorUtils.getMinMax(min, max));
        template = GeneratorUtils.replaceToken(template, "PROPERTIES", GeneratorUtils.getProperties(consumer));

        GeneratorUtils.save(htmlFileName, template);
        GeneratorUtils.save(jsonFileName, getData());
    }

    /**
     * Get chart data as JSON string.
     */
    private String getData() throws SQLException
    {
        StringBuilder buf = new StringBuilder();
        buf.append("receiveJsonpData({\n");

        final PreparedStatement s = 
            consumer.getConnection().prepareStatement(consumer.getMethodChartResultsQuery());
        s.setInt(1, consumer.getRunId());
        s.setString(2, clazzName);

        ResultSet rs = s.executeQuery();

        // Emit columns.
        buf.append("\"cols\": [\n");
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++)
        {
            final String colLabel = metaData.getColumnLabel(i);
            final String type = GeneratorUtils.getMappedType(metaData.getColumnType(i));

            buf.append("{\"label\": \"");
            buf.append(colLabel);
            buf.append("\", \"type\": \"");
            buf.append(type);
            buf.append("\"}");
            if (i != metaData.getColumnCount()) buf.append(",");
            buf.append('\n');
        }
        buf.append("],\n");

        buf.append("\"rows\": [\n");
        while (rs.next())
        {
            buf.append("{\"c\": [");
            for (int i = 1; i <= metaData.getColumnCount(); i++)
            {
                if (i > 1) buf.append(", ");
                final Object value = GeneratorUtils.formatValue(metaData.getColumnType(i), rs.getObject(i)); 
                buf.append("{\"v\": ");
                buf.append(value.toString());
                buf.append("}");
            }
            buf.append("]}");
            if (!rs.isLast()) buf.append(",");
            buf.append('\n');
        }
        buf.append("]});\n");

        rs.close();
        return buf.toString();
    }
}
