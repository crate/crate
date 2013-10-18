package com.carrotsearch.junitbenchmarks.db;

import com.carrotsearch.junitbenchmarks.Escape;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Locale;

import static com.carrotsearch.junitbenchmarks.db.GeneratorUtils.getColumnIndex;

/**
 * Generate historical view of a given test class (one or more methods). 
 */
public final class HistoryChartGenerator
{
    private static EnumMap<LabelType, Integer> labelColumns;
    static
    {
        labelColumns = new EnumMap<LabelType, Integer>(LabelType.class);
        labelColumns.put(LabelType.RUN_ID, 0);
        labelColumns.put(LabelType.CUSTOM_KEY, 1);
        labelColumns.put(LabelType.TIMESTAMP, 2);
    }
    
    /**
     * The consumer.
     */
    private DbConsumer consumer;

    private String clazzName;

    /**
     * A list of methods included in the chart.
     */
    private ArrayList<String> methods = new ArrayList<String>();

    /**
     * Maximum number of history steps to fetch.
     */
    private int maxRuns = Integer.MIN_VALUE;

    /**
     * Prefix for output files.
     */
    private String filePrefix;

    /**
     * Min/ max.
     */
    private double min = Double.NaN, max = Double.NaN;
    
    /**
     * Default X-axis label column.
     */
    private final LabelType labelType;

    /**
     * Value holder for row aggregation.
     */
    private static final class StringHolder {
        public String value;
        
        public StringHolder(String value)
        {
            this.value = value;
        }
    };

    /**
     * @param consumer the database consumer. 
     * @param filePrefix Prefix for output files.
     * @param clazzName The target test class (fully qualified name).
     */
    public HistoryChartGenerator( 
        String filePrefix, String clazzName, LabelType labelType, DbConsumer consumer)
    {
        this.clazzName = clazzName;
        this.filePrefix = filePrefix;
        this.labelType = labelType;
        this.consumer = consumer;
    }

    /**
     * Generate the chart's HTML.
     */
    public void generate() throws Exception
    {
        final String jsonFileName = filePrefix + ".jsonp";
        final String htmlFileName = filePrefix + ".html";

        String template = consumer.getHistoryHtmlTemplate();
        template = GeneratorUtils.replaceToken(template, "CLASSNAME", clazzName);
        template = GeneratorUtils.replaceToken(template, "HistoryChartGenerator.jsonp", new File(jsonFileName).getName());
        template = GeneratorUtils.replaceToken(template, "/*MINMAX*/",  GeneratorUtils.getMinMax(min, max));
        template = GeneratorUtils.replaceToken(template, "/*LABELCOLUMN*/", Integer.toString(labelColumns.get(labelType)));
        template = GeneratorUtils.replaceToken(template, "PROPERTIES", getProperties());

        GeneratorUtils.save(htmlFileName, template);
        GeneratorUtils.save(jsonFileName, getData());
    }

    private String getProperties()
    {
        return "Shows historical runs: " + (maxRuns == Integer.MAX_VALUE ? "all" : maxRuns);
    }

    /**
     * Get chart data as JSON string.
     */
    private String getData() throws SQLException
    {
        String methodsRestrictionClause = "";
        if (methods.size() > 0)
        {
            StringBuilder b = new StringBuilder();
            b.append(" AND NAME IN (");
            for (int i = 0; i < methods.size(); i++)
            {
                if (i > 0) b.append(", ");
                b.append("'");
                b.append(Escape.sqlEscape(methods.get(i)));
                b.append("'");
            }
            b.append(")");
            methodsRestrictionClause = b.toString();
        }

        PreparedStatement s;
        ResultSet rs;
        int minRunId = 0;
        if (maxRuns != Integer.MAX_VALUE)
        {
            // Get min. runId to start from.
            s = consumer.getConnection().prepareStatement(
                "SELECT DISTINCT RUN_ID FROM TESTS t, RUNS r " + 
                " WHERE t.classname = ? " +
                " AND t.run_id = r.id " +
                methodsRestrictionClause +
                " ORDER BY RUN_ID DESC " +
                " LIMIT ?");
            s.setString(1, clazzName);
            s.setInt(2, maxRuns);
            rs = s.executeQuery();
            if (rs.last())
            {
                minRunId = rs.getInt(1);
            }
            s.close();
        }

        // Get all the method names within the runs range.
        s = consumer.getConnection().prepareStatement(
            "SELECT DISTINCT NAME FROM TESTS t, RUNS r " +
            " WHERE t.classname = ? " +
            " AND t.run_id = r.id " +
            methodsRestrictionClause +
            " AND r.id >= ? " +
            " ORDER BY NAME ");
        s.setString(1, clazzName);
        s.setInt(2, minRunId);

        final ArrayList<String> columnNames = new ArrayList<String>(); 
        rs = s.executeQuery();
        while (rs.next())
        {
            columnNames.add(rs.getString(1));
        }

        // Emit columns.
        StringBuilder buf = new StringBuilder();
        buf.append("receiveJsonpData({\n");

        buf.append("\"cols\": [\n");
        
        buf.append("{\"label\": \"Run\", \"type\": \"string\"},\n");
        buf.append("{\"label\": \"Custom key\", \"type\": \"string\"},\n");
        buf.append("{\"label\": \"Timestamp\", \"type\": \"string\"}");

        for (int i = 0; i < columnNames.size(); i++)
        {
            buf.append(",\n");
            buf.append("{\"label\": \"");
            buf.append(Escape.jsonEscape(columnNames.get(i)));
            buf.append("\", \"type\": \"string\"} ");
        }
        buf.append("],\n");

        // Emit data.
        s = consumer.getConnection().prepareStatement(
            "SELECT RUN_ID, CUSTOM_KEY, TSTAMP, NAME, ROUND_AVG " + 
            "FROM TESTS t, RUNS r " + 
            "WHERE t.classname = ? " +
            " AND t.run_id = r.id " +
            methodsRestrictionClause +
            " AND r.id >= ? " +
            "ORDER BY r.id ASC, NAME ASC");
        s.setString(1, clazzName);
        s.setInt(2, minRunId);
        rs = s.executeQuery();

        /*
         * We need to emit a value for every column, possibly missing, so prepare a
         * full row.
         */
        final NumberFormat nf = NumberFormat.getInstance(Locale.ENGLISH);
        nf.setMaximumFractionDigits(3);
        nf.setGroupingUsed(false);

        final HashMap<String, StringHolder> byColumn = new HashMap<String, StringHolder>();
        final ArrayList<StringHolder> row = new ArrayList<StringHolder>();
        row.add(new StringHolder(null)); // run id
        row.add(new StringHolder(null)); // custom key
        row.add(new StringHolder(null)); // timestamp
        for (String name : columnNames)
        {
            StringHolder nv = new StringHolder(null);
            row.add(nv);
            byColumn.put(name, nv);
        }

        final int colRunId = getColumnIndex(rs, "RUN_ID");
        final int colName = getColumnIndex(rs, "NAME");
        final int colRoundAvg = getColumnIndex(rs, "ROUND_AVG");
        final int colCustomKey = getColumnIndex(rs, "CUSTOM_KEY");
        final int colTimestamp = getColumnIndex(rs, "TSTAMP");

        int previousRowId = -1;
        buf.append("\"rows\": [\n");
        while (rs.next())
        {
            final int rowId = rs.getInt(colRunId);

            if (rs.isFirst()) previousRowId = rowId;

            if (rowId != previousRowId)
            {
                emitRow(buf, row, false);
                previousRowId = rowId;
            }

            String name = rs.getString(colName);
            double avg = rs.getDouble(colRoundAvg);
            String customKey = rs.getString(colCustomKey);
            String timestamp = rs.getTimestamp(colTimestamp).toString();
            
            final String runName = Integer.toString(rowId);
            row.get(0).value = '"' + runName + '"';
            row.get(1).value = '"' + (customKey == null ? "[" + runName + "]" : customKey) + '"';
            row.get(2).value = '"' + (timestamp) + '"';

            final StringHolder nv = byColumn.get(name);
            if (nv == null) 
                throw new RuntimeException("Missing column: " + name);
            nv.value = nf.format(avg);

            if (rs.isLast())
                emitRow(buf, row, rs.isLast());
        }
        buf.append("]});\n");
        
        return buf.toString();
    }

    private void emitRow(StringBuilder buf, ArrayList<StringHolder> row, boolean last)
    {
        buf.append("{\"c\": [");
        for (int i = 0; i < row.size(); i++)
        {
            final StringHolder nv = row.get(i);
            buf.append("{\"v\": ");
            buf.append(nv.value);
            buf.append("}");
            if (i + 1 < row.size())
                buf.append(", ");
        }
        buf.append("]}");
        if (!last) buf.append(",");
        buf.append('\n');

        for (StringHolder nv : row)
            nv.value = null;
    }

    /**
     * Include a given method in the chart. 
     */
    public void includeMethod(String methodName)
    {
        methods.add(methodName);
    }

    /**
     * Update max history steps.
     */
    public void updateMaxRuns(int newMax)
    {
        this.maxRuns = Math.max(newMax, maxRuns);
    }

    /**
     * Update min/max fields.
     */
    public void updateMinMax(AxisRange r)
    {
        if (Double.isNaN(this.min))
            this.min = r.min();

        if (!Double.isNaN(r.min()))
        {
            this.min = Math.min(r.min(), this.min);
        }

        if (Double.isNaN(this.max))
            this.max = r.max();

        if (!Double.isNaN(r.max()))
        {
            this.max = Math.max(r.max(), this.max);
        }
    }
}
