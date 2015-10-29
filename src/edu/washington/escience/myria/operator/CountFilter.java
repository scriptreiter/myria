package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import jersey.repackaged.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.gs.collections.api.block.procedure.primitive.IntProcedure;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.IntArrayColumn;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Keeps distinct tuples with their counts and only emits a tuple at the first time when its count hits the threshold.
 * */
public final class CountFilter extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(CountFilter.class);

  /**
   * Indices to unique tuples.
   * */
  private transient IntObjectHashMap<IntArrayList> uniqueTupleIndices;

  /**
   * The buffer for storing unique tuples.
   * */
  private transient MutableTupleBuffer uniqueTuples = null;
  /**
   * The count of each unique tuple.
   * */
  private transient MutableTupleBuffer tupleCounts = null;

  /** threshold of the count. */
  public final int threshold;

  /**
   * @param threshold threshold
   */
  public CountFilter(final int threshold) {
    Preconditions.checkArgument(threshold >= 0, "threshold needs to be greater than or equal to 0");
    this.threshold = threshold;
  }

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
    tupleCounts = null;
  }

  /**
   * Do duplicate elimination for tb.
   * 
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch countFilter(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    System.out.println(tb);
    doCount.inputTB = tb;
    final List<? extends Column<?>> columns = tb.getDataColumns();
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = HashUtils.hashRow(tb, i);
      IntArrayList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      if (tupleIndexList == null) {
        tupleIndexList = new IntArrayList();
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
      }
      doCount.found = false;
      doCount.meet = false;
      doCount.sourceRow = i;
      tupleIndexList.forEach(doCount);
      if (!doCount.found) {
        for (int j = 0; j < columns.size(); ++j) {
          uniqueTuples.put(j, columns.get(j), i);
        }
        tupleCounts.put(0, new IntArrayColumn(new int[] { 1 }, 1), 0);
        tupleIndexList.add(nextIndex);
        if (threshold <= 1) {
          doCount.meet = true;
        }
      }
      if (!doCount.meet) {
        toRemove.set(i);
      }
    }
    System.out.println("return");
    System.out.println(tb.filterOut(toRemove));
    return tb.filterOut(toRemove);
  }

  @Override
  public Schema getSchema() {
    return getOp().getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    uniqueTupleIndices = new IntObjectHashMap<IntArrayList>();
    uniqueTuples = new MutableTupleBuffer(getSchema());
    tupleCounts =
        new MutableTupleBuffer(Schema.of(Arrays.asList(new Type[] { Type.INT_TYPE }), Arrays
            .asList(new String[] { "count" })));
    doCount = new CountProcedure();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = countFilter(tb);
    if (newtb.numTuples() > 0 || newtb.isEOI()) {
      return newtb;
    }
    return null;
  }

  @Override
  public List<TupleBatch> exportState() {
    return uniqueTuples.getAll();
  }

  /**
   * Traverse through the list of tuples and replace old values.
   * */
  private transient CountProcedure doCount;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class CountProcedure implements IntProcedure {

    /** row index of the tuple. */
    private int sourceRow;

    /** input TupleBatch. */
    private TupleBatch inputTB;

    /** if found a key. */
    private boolean found;

    /** if meet the threshold for the first time. */
    private boolean meet;

    @Override
    public void value(final int destRow) {
      if (TupleUtils.tupleEquals(inputTB, sourceRow, uniqueTuples, destRow)) {
        found = true;
        int oldcount = tupleCounts.getInt(0, destRow);
        if (oldcount < threshold) {
          tupleCounts.replace(0, destRow, new IntArrayColumn(new int[] { oldcount + 1 }, 1), 0);
          meet = (oldcount + 1 >= threshold);
        }
      }
    }

  };

  @Override
  public int numTuples() {
    if (uniqueTuples == null) {
      return 0;
    }
    return uniqueTuples.numTuples();
  }

  @Override
  public StreamingState newInstanceFromMyself() {
    return new CountFilter(threshold);
  }
}
