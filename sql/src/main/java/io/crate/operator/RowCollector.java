package io.crate.operator;

public interface RowCollector<T> {

    /**
     * Called once before the first row
     *
     * @return false if no colletion is needed
     */
    public boolean startCollect();

    /**
     * Tells the collector to process the current row
     *
     * @return false if no more rows are needed
     */
    public boolean processRow();

    /**
     * Called once after the last row is processed.
     *
     * @return The result of the collector
     */
    public T finishCollect();

}
