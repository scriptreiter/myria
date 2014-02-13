package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myria.ReadableColumn;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.mutable.MutableColumn;

/**
 * @param <T> type of the objects in this column.
 * 
 */
public abstract class ColumnBuilder<T extends Comparable<?>> implements ReadableColumn, WritableColumn {

  @Override
  public ColumnBuilder<T> appendBoolean(final boolean value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendDateTime(final DateTime value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public boolean getBoolean(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public DateTime getDateTime(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public double getDouble(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public float getFloat(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public int getInt(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public long getLong(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public String getString(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendDouble(final double value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendFloat(final float value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendInt(final int value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendLong(final long value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public ColumnBuilder<T> appendString(final String value) throws BufferOverflowException {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Extracts the appropriate value from a JDBC ResultSet object and appends it to this column.
   * 
   * @param resultSet contains the results
   * @param jdbcIndex the position of the element to extract. 1-indexed.
   * @return this column builder.
   * @throws SQLException if there are JDBC errors.
   * @throws BufferOverflowException if the column is already full
   */
  public abstract ColumnBuilder<T> appendFromJdbc(ResultSet resultSet, int jdbcIndex) throws SQLException,
      BufferOverflowException;

  /**
   * Extracts the appropriate value from a SQLiteStatement object and appends it to this column.
   * 
   * @param statement contains the results
   * @return this column builder.
   * @param index the position of the element to extract. 0-indexed.
   * @throws SQLiteException if there are SQLite errors.
   * @throws BufferOverflowException if the column is already full
   */
  public abstract ColumnBuilder<T> appendFromSQLite(SQLiteStatement statement, int index) throws SQLiteException,
      BufferOverflowException;

  /**
   * @return a column with the contents built.
   */
  public abstract Column<T> build();

  /**
   * @return a mutable column with the contents built.
   */
  public abstract MutableColumn<T> buildMutable();

  /**
   * expand some size.
   * 
   * @param size to expand
   * @return this column builder.
   * @throws BufferOverflowException if expanding size exceeds the column capacity
   */
  public abstract ColumnBuilder<T> expand(int size) throws BufferOverflowException;

  /**
   * expand to full size.
   * 
   * @return this column builder.
   */
  public abstract ColumnBuilder<T> expandAll();

  /**
   * @return a new builder with the contents initialized by this builder. The two builders share no data.
   */
  public abstract ColumnBuilder<T> forkNewBuilder();

  /**
   * Replace the specified element.
   * 
   * @param value element to be inserted.
   * @param idx where to insert the element.
   * @return this column builder.
   * @throws IndexOutOfBoundsException if the idx exceeds the currently valid indices, i.e. the currently built size.
   */
  public abstract ColumnBuilder<T> replace(int idx, T value) throws IndexOutOfBoundsException;

}