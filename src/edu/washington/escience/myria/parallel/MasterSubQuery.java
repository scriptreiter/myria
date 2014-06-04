package edu.washington.escience.myria.parallel;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * A {@link LocalSubQuery} running at the Master. Currently, a {@link MasterSubQuery} can only have a single
 * {@link LocalFragment}.
 */
public class MasterSubQuery implements LocalSubQuery {

  /**
   * Record worker execution info.
   */
  private class WorkerExecutionInfo {
    /**
     * @param workerID owner worker id of the {@link LocalSubQuery}.
     * @param workerPlan the query plan of the {@link LocalSubQuery}.
     */
    WorkerExecutionInfo(final int workerID, final SubQueryPlan workerPlan) {
      this.workerPlan = workerPlan;
      workerReceiveSubQuery = new LocalSubQueryFuture(MasterSubQuery.this, false);
      workerReceiveSubQuery.addListener(new LocalSubQueryFutureListener() {

        @Override
        public void operationComplete(final LocalSubQueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowReceived.incrementAndGet();
          if (current >= total) {
            workerReceiveFuture.setSuccess();
          }
        }
      });
      workerCompleteQuery = new LocalSubQueryFuture(MasterSubQuery.this, false);
      workerCompleteQuery.addListener(new LocalSubQueryFutureListener() {

        @Override
        public void operationComplete(final LocalSubQueryFuture future) throws Exception {
          int total = workerExecutionInfo.size();
          int current = nowCompleted.incrementAndGet();
          if (!future.isSuccess()) {
            Throwable cause = future.getCause();
            if (!(cause instanceof QueryKilledException)) {
              // Only record non-killed exceptions
              if (ftMode.equals(FTMODE.none)) {
                failedWorkerLocalSubQueries.put(workerID, cause);
                // if any worker fails because of some exception, kill the query.
                kill();
                /* Record the reason for failure. */
                if (cause != null) {
                  message = Objects.firstNonNull(message, cause.toString());
                }
              } else if (ftMode.equals(FTMODE.abandon)) {
                LOGGER.debug("(Abandon) ignoring failed subquery future on subquery #{}", subQueryId);
                // do nothing
              } else if (ftMode.equals(FTMODE.rejoin)) {
                LOGGER.debug("(Rejoin) ignoring failed subquery future on subquery #{}", subQueryId);
                // do nothing
              }
            }
          }
          if (current >= total) {
            queryStatistics.markEnd();
            LOGGER.info("Query #{} executed for {}", subQueryId, DateTimeUtils
                .nanoElapseToHumanReadable(queryStatistics.getQueryExecutionElapse()));

            if (!killed && failedWorkerLocalSubQueries.isEmpty()) {
              queryExecutionFuture.setSuccess();
            } else {
              if (failedWorkerLocalSubQueries.isEmpty()) {
                // query gets killed.
                queryExecutionFuture.setFailure(new QueryKilledException());
              } else {
                DbException composedException =
                    new DbException("Query #" + future.getLocalSubQuery().getSubQueryId() + " failed.");
                for (Entry<Integer, Throwable> workerIDCause : failedWorkerLocalSubQueries.entrySet()) {
                  int failedWorkerID = workerIDCause.getKey();
                  Throwable cause = workerIDCause.getValue();
                  if (!(cause instanceof QueryKilledException)) {
                    // Only record non-killed exceptions
                    DbException workerException =
                        new DbException("Worker #" + failedWorkerID + " failed: " + cause.getMessage(), cause);
                    workerException.setStackTrace(cause.getStackTrace());
                    for (Throwable sup : cause.getSuppressed()) {
                      workerException.addSuppressed(sup);
                    }
                    composedException.addSuppressed(workerException);
                  }
                }
                queryExecutionFuture.setFailure(composedException);
              }
            }
          }
        }
      });
    }

    /**
     * The {@link SubQueryPlan} that's assigned to the master.
     */
    private final SubQueryPlan workerPlan;

    /**
     * The future denoting the status of {@link SubQuery} dispatching to the workers.
     */
    private final LocalSubQueryFuture workerReceiveSubQuery;

    /**
     * The future denoting the status of {@link SubQuery} execution on the worker.
     */
    private final LocalSubQueryFuture workerCompleteQuery;
  }

  /**
   * logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterSubQuery.class);

  /**
   * The {@link SubQuery} id.
   */
  private final SubQueryId subQueryId;

  /**
   * The actual physical plan for the master {@link LocalSubQuery}.
   */
  private volatile LocalFragment fragment;

  /***
   * Statistics of this {@link LocalSubQuery}.
   */
  private final ExecutionStatistics queryStatistics = new ExecutionStatistics();

  /**
   * The root operator of the {@link MasterSubQuery}.
   */
  private final RootOperator root;

  /**
   * The owner master.
   */
  private final Server master;

  /**
   * The FT mode.
   */
  private final FTMODE ftMode;

  /**
   * The profiling mode.
   */
  private final boolean profilingMode;

  /**
   * The priority.
   */
  private volatile int priority;

  /**
   * The data structure denoting the query dispatching/execution status of each worker.
   */
  private final ConcurrentHashMap<Integer, WorkerExecutionInfo> workerExecutionInfo;

  /**
   * the number of workers currently received the query.
   */
  private final AtomicInteger nowReceived = new AtomicInteger();

  /**
   * The number of workers currently completed the query.
   */
  private final AtomicInteger nowCompleted = new AtomicInteger();

  /**
   * The future object denoting the worker receive query plan operation.
   */
  private final LocalSubQueryFuture workerReceiveFuture = new LocalSubQueryFuture(this, false);

  /**
   * The future object denoting the query execution progress.
   */
  private final LocalSubQueryFuture queryExecutionFuture = new LocalSubQueryFuture(this, false);

  /**
   * Current alive worker set.
   */
  private final Set<Integer> missingWorkers;

  /**
   * record all failed {@link LocalSubQuery}s.
   */
  private final ConcurrentHashMap<Integer, Throwable> failedWorkerLocalSubQueries = new ConcurrentHashMap<>();

  /**
   * The future listener for processing the complete events of the execution of the master fragment.
   */
  private final LocalFragmentFutureListener fragmentExecutionListener = new LocalFragmentFutureListener() {

    @Override
    public void operationComplete(final LocalFragmentFuture future) throws Exception {
      if (future.isSuccess()) {
        if (root instanceof SinkRoot) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info(" Root fragment {} EOS. Num output tuple: {}", fragment, ((SinkRoot) root).getCount());
          }
        }
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setSuccess();
      } else {
        workerExecutionInfo.get(MyriaConstants.MASTER_ID).workerCompleteQuery.setFailure(future.getCause());
      }
    }

  };

  /**
   * Callback when a query plan is received by a worker.
   * 
   * @param workerID the workerID
   */
  final void queryReceivedByWorker(final int workerID) {
    WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    LOGGER.debug("Worker #{} received query#{}", workerID, subQueryId);
    if (wei.workerReceiveSubQuery.isSuccess()) {
      /* a recovery worker */
      master.getIPCConnectionPool().sendShortMessage(workerID, IPCUtils.startQueryTM(subQueryId));
      for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
        if (e.getKey() == workerID) {
          /* the new worker doesn't need to start recovery tasks */
          continue;
        }
        if (!e.getValue().workerCompleteQuery.isDone() && e.getKey() != MyriaConstants.MASTER_ID) {
          master.getIPCConnectionPool().sendShortMessage(e.getKey(), IPCUtils.recoverQueryTM(subQueryId, workerID));
        }
      }
    } else {
      wei.workerReceiveSubQuery.setSuccess();
    }
  }

  /**
   * @return worker plans.
   */
  final Map<Integer, SubQueryPlan> getWorkerPlans() {
    Map<Integer, SubQueryPlan> result = new HashMap<Integer, SubQueryPlan>(workerExecutionInfo.size());
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() != MyriaConstants.MASTER_ID) {
        result.put(e.getKey(), e.getValue().workerPlan);
      }
    }
    return result;
  }

  /**
   * @return query future for the worker receiving query action.
   */
  final LocalSubQueryFuture getWorkerReceiveFuture() {
    return workerReceiveFuture;
  }

  @Override
  public final LocalSubQueryFuture getExecutionFuture() {
    return queryExecutionFuture;
  }

  /**
   * @return my root operator.
   */
  final RootOperator getRootOperator() {
    return root;
  }

  /**
   * @return the set of workers get assigned to run the query.
   */
  final Set<Integer> getWorkerAssigned() {
    Set<Integer> s = new HashSet<Integer>(workerExecutionInfo.keySet());
    s.remove(MyriaConstants.MASTER_ID);
    return s;
  }

  /**
   * @return the set of workers who havn't finished their execution of the query.
   */
  final Set<Integer> getWorkersUnfinished() {
    Set<Integer> result = new HashSet<>();
    for (Entry<Integer, WorkerExecutionInfo> e : workerExecutionInfo.entrySet()) {
      if (e.getKey() == MyriaConstants.MASTER_ID) {
        continue;
      }
      LocalSubQueryFuture workerExecutionFuture = e.getValue().workerCompleteQuery;
      if (!workerExecutionFuture.isDone()) {
        result.add(e.getKey());
      }
    }
    return result;
  }

  /**
   * Callback when a worker completes its part of the query.
   * 
   * @param workerID the workerID
   */
  final void workerComplete(final int workerID) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (succeed) message from worker {} who is not assigned to query #{}", workerID,
          subQueryId);
      return;
    }
    LOGGER.debug("Received query complete (succeed) message from worker: {}", workerID);
    wei.workerCompleteQuery.setSuccess();
  }

  /**
   * Callback when a worker fails in executing its part of the query.
   * 
   * @param workerID the workerID
   * @param cause the cause of the failure
   */
  final void workerFail(final int workerID, final Throwable cause) {
    final WorkerExecutionInfo wei = workerExecutionInfo.get(workerID);
    if (wei == null) {
      LOGGER.warn("Got a QUERY_COMPLETE (fail) message from worker {} who is not assigned to query #{}", workerID,
          subQueryId);
      return;
    }

    LOGGER.info("Received query complete (fail) message from worker: {}, cause: {}", workerID, cause);
    if (ftMode.equals(FTMODE.rejoin) && cause.toString().endsWith("LostHeartbeatException")) {
      /* for rejoin, don't set it to be completed since this worker is expected to be launched again. */
      return;
    }
    wei.workerCompleteQuery.setFailure(cause);
  }

  /**
   * @param subQuery the {@link SubQuery} to be executed.
   * @param master the master on which the {@link SubQuery} is running.
   */
  public MasterSubQuery(final SubQuery subQuery, final Server master) {
    Preconditions.checkNotNull(subQuery, "subQuery");
    SubQueryPlan masterPlan = subQuery.getMasterPlan();
    Map<Integer, SubQueryPlan> workerPlans = subQuery.getWorkerPlans();
    root = masterPlan.getRootOps().get(0);
    subQueryId = Preconditions.checkNotNull(subQuery.getSubQueryId(), "subQueryId");
    this.master = master;
    profilingMode = masterPlan.isProfilingMode();
    ftMode = masterPlan.getFTMode();
    workerExecutionInfo = new ConcurrentHashMap<Integer, WorkerExecutionInfo>(workerPlans.size());

    for (Entry<Integer, SubQueryPlan> workerInfo : workerPlans.entrySet()) {
      workerExecutionInfo.put(workerInfo.getKey(), new WorkerExecutionInfo(workerInfo.getKey(), workerInfo.getValue()));
    }
    WorkerExecutionInfo masterPart = new WorkerExecutionInfo(MyriaConstants.MASTER_ID, masterPlan);
    workerExecutionInfo.put(MyriaConstants.MASTER_ID, masterPart);

    missingWorkers = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    fragment = new LocalFragment(MyriaConstants.MASTER_ID, this, root, master.getQueryExecutor());
    fragment.getExecutionFuture().addListener(fragmentExecutionListener);
    HashSet<Consumer> consumerSet = new HashSet<>();
    consumerSet.addAll(fragment.getInputChannels().values());

    for (final Consumer operator : consumerSet) {
      FlowControlBagInputBuffer<TupleBatch> inputBuffer =
          new FlowControlBagInputBuffer<TupleBatch>(this.master.getIPCConnectionPool(), operator
              .getInputChannelIDs(this.master.getIPCConnectionPool().getMyIPCID()), master.getInputBufferCapacity(),
              master.getInputBufferRecoverTrigger(), this.master.getIPCConnectionPool());
      operator.setInputBuffer(inputBuffer);
      inputBuffer.addListener(FlowControlBagInputBuffer.NEW_INPUT_DATA, new IPCEventListener() {

        @Override
        public void triggered(final IPCEvent event) {
          fragment.notifyNewInput();
        }
      });
    }
  }

  @Override
  public final SubQueryId getSubQueryId() {
    return subQueryId;
  }

  @Override
  public final int compareTo(final LocalSubQuery o) {
    if (o == null) {
      return -1;
    }
    return priority - o.getPriority();
  }

  @Override
  public final void setPriority(final int priority) {
    this.priority = priority;
  }

  @Override
  public final String toString() {
    return Joiner.on("").join(fragment, ", priority:", priority);
  }

  @Override
  public final void startExecution() {
    LOGGER.info("starting execution for query #{}", subQueryId);
    queryStatistics.markStart();
    fragment.start();
  }

  @Override
  public final int getPriority() {
    return priority;
  }

  @Override
  public final void kill() {
    if (killed) {
      return;
    }
    killed = true;
    fragment.kill();
    Set<Integer> workers = getWorkersUnfinished();
    ChannelFuture[] cfs = new ChannelFuture[workers.size()];
    int i = 0;
    DefaultChannelGroup cg = new DefaultChannelGroup();
    for (Integer workerID : workers) {
      cfs[i] = master.getIPCConnectionPool().sendShortMessage(workerID, IPCUtils.killQueryTM(getSubQueryId()));
      cg.add(cfs[i].getChannel());
      i++;
    }
    DefaultChannelGroupFuture f = new DefaultChannelGroupFuture(cg, Arrays.asList(cfs));
    f.awaitUninterruptibly();
    if (!f.isCompleteSuccess()) {
      LOGGER.error("Send kill query message to workers failed.");
    }
  }

  @Override
  public final void init() {
    ImmutableMap.Builder<String, Object> b = ImmutableMap.builder();
    LocalFragmentResourceManager resourceManager =
        new LocalFragmentResourceManager(master.getIPCConnectionPool(), fragment);
    fragment.init(resourceManager, b.putAll(master.getExecEnvVars()).build());
  }

  @Override
  public final ExecutionStatistics getExecutionStatistics() {
    return queryStatistics;
  }

  /**
   * If the query has been asked to get killed (the kill event may not have completed).
   */
  private volatile boolean killed = false;

  /**
   * Describes the cause of the query's death.
   */
  private volatile String message = null;

  /**
   * @return If the query has been asked to get killed (the kill event may not have completed).
   */
  public final boolean isKilled() {
    return killed;
  }

  @Override
  public FTMODE getFTMode() {
    return ftMode;
  }

  @Override
  public boolean isProfilingMode() {
    return profilingMode;
  }

  @Override
  public Set<Integer> getMissingWorkers() {
    return missingWorkers;
  }

  /**
   * when a REMOVE_WORKER message is received, give the execution {@link LocalFragment} another chance to decide if it
   * is ready to generate EOS/EOI.
   */
  public void triggerFragmentEosEoiCheck() {
    fragment.notifyNewInput();
  }

  /**
   * enable/disable output channels of the root(producer) of the {@link LocalFragment}.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   */
  public void updateProducerChannels(final int workerId, final boolean enable) {
    fragment.updateProducerChannels(workerId, enable);
  }

  /**
   * @return the message describing the cause of the query's death. Nullable.
   */
  public String getMessage() {
    return message;
  }
}