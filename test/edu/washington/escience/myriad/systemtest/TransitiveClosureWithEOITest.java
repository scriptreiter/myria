package edu.washington.escience.myriad.systemtest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myriad.util.TestUtils;

public class TransitiveClosureWithEOITest extends SystemTestBase {
  // change configuration here
  private final int MaxID = 100;
  private final int numTbl1Worker1 = 10000;
  private final int numTbl1Worker2 = 10000;

  @Override
  public Map<String, String> getMasterConfigurations() {
    HashMap<String, String> masterConfigurations = new HashMap<String, String>();
    masterConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "1");
    masterConfigurations.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "20001");
    return masterConfigurations;
  }

  @Override
  public Map<String, String> getWorkerConfigurations() {
    HashMap<String, String> workerConfigurations = new HashMap<String, String>();
    workerConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "1");
    workerConfigurations.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "20101");
    return workerConfigurations;
  }

  public boolean[][] allNodeTransitiveClosure(TupleBatchBuffer table1, Schema schema) {
    // a brute force check

    final Iterator<List<Column<?>>> tbs = table1.getAllAsRawColumn().iterator();
    boolean graph[][] = new boolean[MaxID][MaxID];
    while (tbs.hasNext()) {
      List<Column<?>> output = tbs.next();
      int numRow = output.get(0).size();
      for (int i = 0; i < numRow; i++) {
        int fr = Integer.parseInt(output.get(0).get(i).toString());
        int fe = Integer.parseInt(output.get(1).get(i).toString());
        graph[fr][fe] = true;
      }
    }

    while (true) {
      boolean update = false;
      for (int i = 0; i < MaxID; ++i) {
        for (int j = 0; j < MaxID; ++j) {
          for (int k = 0; k < MaxID; ++k) {
            if (graph[j][i] && graph[i][k] && !graph[j][k]) {
              graph[j][k] = true;
              update = true;
            }
          }
        }
      }
      if (!update) {
        break;
      }
    }
    return graph;

  }

  @Test
  public void transitiveClosure() throws Exception {
    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);
    RelationKey identityKey = RelationKey.of("test", "test", "identity");
    RelationKey testtableKey = RelationKey.of("test", "test", "testtable");

    // generate the graph
    long[] tbl1ID1Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID1Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    long[] tbl1ID2Worker1 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker1);
    long[] tbl1ID2Worker2 = TestUtils.randomLong(1, MaxID - 1, numTbl1Worker2);
    TupleBatchBuffer tbl1Worker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer tbl1Worker2 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < numTbl1Worker1; i++) {
      tbl1Worker1.put(0, tbl1ID1Worker1[i]);
      tbl1Worker1.put(1, tbl1ID2Worker1[i]);
    }
    for (int i = 0; i < numTbl1Worker2; i++) {
      tbl1Worker2.put(0, tbl1ID1Worker2[i]);
      tbl1Worker2.put(1, tbl1ID2Worker2[i]);
    }
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    table1.merge(tbl1Worker1);
    table1.merge(tbl1Worker2);

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    createTable(WORKER_ID[1], testtableKey, "follower long, followee long");
    TupleBatch tb = null;
    while ((tb = tbl1Worker1.popAny()) != null) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
    }
    while ((tb = tbl1Worker2.popAny()) != null) {
      insert(WORKER_ID[1], testtableKey, tableSchema, tb);
    }

    boolean[][] graph = allNodeTransitiveClosure(table1, tableSchema);
    // generate the correct answer in memory
    TupleBatchBuffer expectedTBB = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < MaxID; ++i) {
      for (int j = 0; j < MaxID; ++j) {
        if (graph[i][j]) {
          expectedTBB.put(0, (long) i);
          expectedTBB.put(1, (long) j);
          LOGGER.debug(i + "\t" + j);
        }
      }
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // generate the I matrix
    TupleBatchBuffer identity_worker1 = new TupleBatchBuffer(tableSchema);
    TupleBatchBuffer identity_worker2 = new TupleBatchBuffer(tableSchema);
    Random gen = new Random();
    for (long i = 1; i < MaxID; ++i) {
      if (gen.nextInt(1000) % 2 == 0) {
        identity_worker1.put(0, i);
        identity_worker1.put(1, i);
      } else {
        identity_worker2.put(0, i);
        identity_worker2.put(1, i);
      }
    }
    createTable(WORKER_ID[0], identityKey, "follower long, followee long");
    createTable(WORKER_ID[1], identityKey, "follower long, followee long");
    while ((tb = identity_worker1.popAny()) != null) {
      insert(WORKER_ID[0], identityKey, tableSchema, tb);
    }
    while ((tb = identity_worker2.popAny()) != null) {
      insert(WORKER_ID[1], identityKey, tableSchema, tb);
    }

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 = new SQLiteQueryScan("select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("select * from " + identityKey, tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayConsumer sendBack_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer sendBack_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2server_worker1 = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final LocalMultiwayConsumer send2server_worker2 = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final IDBInput idbinput_worker1 = new IDBInput(scan2, sendBack_worker1);
    final IDBInput idbinput_worker2 = new IDBInput(scan2, sendBack_worker2);

    final int numPartition = 2;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column
    ExchangePairID joinArray1ID, joinArray2ID;
    joinArray1ID = ExchangePairID.newID();
    joinArray2ID = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, joinArray1ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf1);
    final ShuffleProducer sp2_worker1 =
        new ShuffleProducer(idbinput_worker1, joinArray2ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleProducer sp2_worker2 =
        new ShuffleProducer(idbinput_worker2, joinArray2ID, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleConsumer sc1 =
        new ShuffleConsumer(sp1.getSchema(), joinArray1ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc2_worker1 =
        new ShuffleConsumer(sp2_worker1.getSchema(), joinArray2ID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc2_worker2 =
        new ShuffleConsumer(sp2_worker2.getSchema(), joinArray2ID, new int[] { WORKER_ID[0], WORKER_ID[1] });

    final LocalJoin join_worker1 =
        new LocalJoin(sc1, sc2_worker1, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    final LocalJoin join_worker2 =
        new LocalJoin(sc1, sc2_worker2, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 });
    ExchangePairID beforeDE = ExchangePairID.newID();
    final ShuffleProducer sp3_worker1 =
        new ShuffleProducer(join_worker1, beforeDE, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleProducer sp3_worker2 =
        new ShuffleProducer(join_worker2, beforeDE, new int[] { WORKER_ID[0], WORKER_ID[1] }, pf0);
    final ShuffleConsumer sc3_worker1 =
        new ShuffleConsumer(sp3_worker1.getSchema(), beforeDE, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final ShuffleConsumer sc3_worker2 =
        new ShuffleConsumer(sp3_worker2.getSchema(), beforeDE, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final DupElim dupelim_worker1 = new DupElim(sc3_worker1);
    final DupElim dupelim_worker2 = new DupElim(sc3_worker2);
    final LocalMultiwayProducer multiProducer_worker1 =
        new LocalMultiwayProducer(dupelim_worker1, new ExchangePairID[] { consumerID1, consumerID2 });
    final LocalMultiwayProducer multiProducer_worker2 =
        new LocalMultiwayProducer(dupelim_worker2, new ExchangePairID[] { consumerID1, consumerID2 });
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp_worker1 = new CollectProducer(send2server_worker1, serverReceiveID, MASTER_ID);
    final CollectProducer cp_worker2 = new CollectProducer(send2server_worker2, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0],
        new RootOperator[] { cp_worker1, multiProducer_worker1, sp1, sp2_worker1, sp3_worker1 });
    workerPlans.put(WORKER_ID[1],
        new RootOperator[] { cp_worker2, multiProducer_worker2, sp1, sp2_worker2, sp3_worker2 });

    final CollectConsumer serverCollect =
        new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0], WORKER_ID[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);
  }

  // @Test
  public void exhaustTest() throws Exception {

    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    // generate the graph
    long[] tbl1ID1Worker1 = new long[] { 0, 1, 2, 2 };
    long[] tbl1ID2Worker1 = new long[] { 1, 2, 3, 4 };
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < tbl1ID1Worker1.length; i++) {
      table1.put(0, tbl1ID1Worker1[i]);
      table1.put(1, tbl1ID2Worker1[i]);
    }

    TupleBatchBuffer seed = new TupleBatchBuffer(tableSchema);
    seed.put(0, 0l);
    seed.put(1, 0l);
    RelationKey seedKey = RelationKey.of("test", "test", "seed");
    RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], seedKey, "follower long, followee long");
    insert(WORKER_ID[0], seedKey, tableSchema, seed.popAny());

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    for (TupleBatch tb : table1.getAll()) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
    }

    boolean[][] graph = allNodeTransitiveClosure(table1, tableSchema);
    // transitive closure from 0
    TupleBatchBuffer expectedTBB = new TupleBatchBuffer(tableSchema);
    for (int j = 0; j < MaxID; ++j) {
      if (graph[0][j]) {
        expectedTBB.put(0, 0l);
        expectedTBB.put(1, (long) j);
        LOGGER.debug(0 + "\t" + j);
      }
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 = new SQLiteQueryScan("select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("select * from " + seedKey, tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayConsumer sendBack = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2server = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final IDBInput idbinput = new IDBInput(scan2, sendBack);

    final int numPartition = 1;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column
    ExchangePairID joinArray1ID, joinArray2ID;
    joinArray1ID = ExchangePairID.newID();
    joinArray2ID = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, joinArray1ID, new int[] { WORKER_ID[0] }, pf1);
    final ShuffleProducer sp2 = new ShuffleProducer(idbinput, joinArray2ID, new int[] { WORKER_ID[0] }, pf0);
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1.getSchema(), joinArray1ID, new int[] { WORKER_ID[0] });
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2.getSchema(), joinArray2ID, new int[] { WORKER_ID[0] });

    final LocalJoin join = new LocalJoin(sc1, sc2, new int[] { 0 }, new int[] { 1 });
    final Project proj = new Project(new Integer[] { 2, 1 }, join);
    ExchangePairID beforeDE = ExchangePairID.newID();
    final ShuffleProducer sp3 = new ShuffleProducer(proj, beforeDE, new int[] { WORKER_ID[0] }, pf0);
    final ShuffleConsumer sc3 = new ShuffleConsumer(sp3.getSchema(), beforeDE, new int[] { WORKER_ID[0] });
    final DupElim dupelim = new DupElim(sc3);
    final LocalMultiwayProducer multiProducer =
        new LocalMultiwayProducer(dupelim, new ExchangePairID[] { consumerID1, consumerID2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(send2server, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, multiProducer, sp1, sp2, sp3 });

    final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    for (int i = 0; i < 1000; i++) {
      final int j = i;

      server.submitQueryPlan(serverPlan, workerPlans).sync();
      TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
      TupleBatch tb = null;
      while (!receivedTupleBatches.isEmpty()) {
        tb = receivedTupleBatches.poll();
        if (tb != null) {
          tb.compactInto(actualResult);
        }
      }
      final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
      TestUtils.assertTupleBagEqual(expectedResult, resultBag);
      System.out.println("Query #" + j + " finished.");

    }
  }

  // @Test
  public void simpleSingleWorkerTransitiveClosure() throws Exception {
    // data generation
    final ImmutableList<Type> table1Types = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> table1ColumnNames = ImmutableList.of("follower", "followee");
    final Schema tableSchema = new Schema(table1Types, table1ColumnNames);

    // generate the graph
    long[] tbl1ID1Worker1 = new long[] { 0, 1, 2, 2 };
    long[] tbl1ID2Worker1 = new long[] { 1, 2, 3, 4 };
    TupleBatchBuffer table1 = new TupleBatchBuffer(tableSchema);
    for (int i = 0; i < tbl1ID1Worker1.length; i++) {
      table1.put(0, tbl1ID1Worker1[i]);
      table1.put(1, tbl1ID2Worker1[i]);
    }

    TupleBatchBuffer seed = new TupleBatchBuffer(tableSchema);
    seed.put(0, 0l);
    seed.put(1, 0l);
    RelationKey seedKey = RelationKey.of("test", "test", "identity");
    RelationKey testtableKey = RelationKey.of("test", "test", "testtable");
    createTable(WORKER_ID[0], seedKey, "follower long, followee long");
    insert(WORKER_ID[0], seedKey, tableSchema, seed.popAny());

    createTable(WORKER_ID[0], testtableKey, "follower long, followee long");
    for (TupleBatch tb : table1.getAll()) {
      insert(WORKER_ID[0], testtableKey, tableSchema, tb);
    }

    boolean[][] graph = allNodeTransitiveClosure(table1, tableSchema);
    // transitive closure from 0
    TupleBatchBuffer expectedTBB = new TupleBatchBuffer(tableSchema);
    for (int j = 0; j < MaxID; ++j) {
      if (graph[0][j]) {
        expectedTBB.put(0, 0l);
        expectedTBB.put(1, (long) j);
        LOGGER.debug(0 + "\t" + j);
      }
    }
    final HashMap<Tuple, Integer> expectedResult = TestUtils.tupleBatchToTupleBag(expectedTBB);

    // parallel query generation, duplicate db files
    final SQLiteQueryScan scan1 = new SQLiteQueryScan("select * from " + testtableKey, tableSchema);
    final SQLiteQueryScan scan2 = new SQLiteQueryScan("select * from " + seedKey, tableSchema);
    final ExchangePairID consumerID1 = ExchangePairID.newID();
    final ExchangePairID consumerID2 = ExchangePairID.newID();
    final LocalMultiwayConsumer sendBack = new LocalMultiwayConsumer(tableSchema, consumerID1);
    final LocalMultiwayConsumer send2server = new LocalMultiwayConsumer(tableSchema, consumerID2);
    final IDBInput idbinput = new IDBInput(scan2, sendBack);

    final int numPartition = 1;
    final PartitionFunction<String, Integer> pf0 = new SingleFieldHashPartitionFunction(numPartition);
    pf0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0); // partition by 1st column
    final PartitionFunction<String, Integer> pf1 = new SingleFieldHashPartitionFunction(numPartition);
    pf1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1); // partition by 2nd column
    ExchangePairID joinArray1ID, joinArray2ID;
    joinArray1ID = ExchangePairID.newID();
    joinArray2ID = ExchangePairID.newID();
    final ShuffleProducer sp1 = new ShuffleProducer(scan1, joinArray1ID, new int[] { WORKER_ID[0] }, pf1);
    final ShuffleProducer sp2 = new ShuffleProducer(idbinput, joinArray2ID, new int[] { WORKER_ID[0] }, pf0);
    final ShuffleConsumer sc1 = new ShuffleConsumer(sp1.getSchema(), joinArray1ID, new int[] { WORKER_ID[0] });
    final ShuffleConsumer sc2 = new ShuffleConsumer(sp2.getSchema(), joinArray2ID, new int[] { WORKER_ID[0] });

    final LocalJoin join = new LocalJoin(sc1, sc2, new int[] { 0 }, new int[] { 1 });
    final Project proj = new Project(new Integer[] { 2, 1 }, join);
    ExchangePairID beforeDE = ExchangePairID.newID();
    final ShuffleProducer sp3 = new ShuffleProducer(proj, beforeDE, new int[] { WORKER_ID[0] }, pf0);
    final ShuffleConsumer sc3 = new ShuffleConsumer(sp3.getSchema(), beforeDE, new int[] { WORKER_ID[0] });
    final DupElim dupelim = new DupElim(sc3);
    final LocalMultiwayProducer multiProducer =
        new LocalMultiwayProducer(dupelim, new ExchangePairID[] { consumerID1, consumerID2 });

    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp = new CollectProducer(send2server, serverReceiveID, MASTER_ID);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(WORKER_ID[0], new RootOperator[] { cp, multiProducer, sp1, sp2, sp3 });

    final CollectConsumer serverCollect = new CollectConsumer(tableSchema, serverReceiveID, new int[] { WORKER_ID[0] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);

  }
}
