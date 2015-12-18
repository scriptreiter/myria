package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

import java.util.Map;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * A relation with a single column with values in a sequence. Values run from 0 to count - 1
 * 
 */
public final class Seq extends LeafOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The Schema of the tuples output by this operator. */
  private static final Schema SEQ_SCHEMA = Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList
      .of("SEQNUM"));

  /** A flag whether we have generated (and returned) tuples, yet. */
  private boolean been_generated;

  /** The count of tuples in this sequence. */
  private long count;

  /** A mapping of worker ids to their offset in the group */
  private Map<Integer, Integer> offsets;

  /**
   * Constructs a relation with exactly one row.
   */
  public Seq(@Nonnull final long count, Map<Integer, Integer> offsets) {
    this.count = count;
    this.offsets = offsets;
    this.been_generated = false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // Check if we have already generated and thus returned tuples
    if(this.been_generated) {
      return null;
    }

    this.been_generated = true;

    TupleBatchBuffer tbb  = new TupleBatchBuffer(SEQ_SCHEMA);

    // The offset of this worker
    int offset = this.offsets.get(this.getNodeID());

    // The start of the range (inclusive)
    long start = offset * this.count / this.offsets.size();

    // The end of the range (exclusive)
    long end  = (offset + 1) * this.count / this.offsets.size();

    for(long i = start; i < end; i++) {
      tbb.putInt(0, (int) i);
    }

    return tbb.popAny();
  }

  @Override
  protected Schema generateSchema() {
    return SEQ_SCHEMA;
  }
}
