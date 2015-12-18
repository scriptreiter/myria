package edu.washington.escience.myria.api.encoding;

import java.util.ArrayList;
import java.util.HashMap;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Seq;

public class SeqEncoding extends LeafOperatorEncoding<Seq> {

  @Required
  public Long count;

  @Override
  public Seq construct(ConstructArgs args) {
    // Get a list of the current alive workers
    ArrayList<Integer> workers = new ArrayList<Integer>(args.getServer().getAliveWorkers());

    // We initialize the hashmap with load factor 1, and size of the list
    // to prevent rehashing overhead
    HashMap<Integer, Integer> worker_offsets = new HashMap<Integer, Integer>(workers.size(), 1.0f);

    // Now we map the worker ids to their offsets for quicker retrieval
    // For small numbers of workers, this may be overkill, compared to indexOf
    for(int i = 0; i < workers.size(); i++) {
      worker_offsets.put(workers.get(i), i);
    }

    System.out.println("size3: " + workers.size());
    return new Seq(count, worker_offsets);
  }
}
