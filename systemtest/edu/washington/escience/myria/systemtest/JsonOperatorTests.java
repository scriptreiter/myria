package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.HttpURLConnection;
import java.nio.file.Paths;

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.CrossWithSingletonEncoding;
import edu.washington.escience.myria.api.encoding.DbInsertEncoding;
import edu.washington.escience.myria.api.encoding.EmptyRelationEncoding;
import edu.washington.escience.myria.api.encoding.FileScanEncoding;
import edu.washington.escience.myria.api.encoding.FlatteningApplyEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.api.encoding.SingletonEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.CounterExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.SplitExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * System tests of operators using plans submitted via JSON. Tests both the API encoding of the operator AND the
 * serializability of the operator.
 */
public class JsonOperatorTests extends SystemTestBase {

  @Test
  public void crossWithSingletonTest() throws Exception {
    SingletonEncoding singleton = new SingletonEncoding();
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    CrossWithSingletonEncoding cross = new CrossWithSingletonEncoding();
    DbInsertEncoding insert = new DbInsertEncoding();

    RelationKey outputRelation = RelationKey.of("test", "crosswithsingleton", "empty");
    singleton.opId = 0;
    empty.opId = 1;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    cross.opId = 2;
    cross.argChild1 = empty.opId;
    cross.argChild2 = singleton.opId;
    insert.opId = 3;
    insert.argChild = cross.opId;
    insert.relationKey = outputRelation;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(singleton, empty, cross, insert);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "CrossWithSingleton test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
  }

  @Test
  public void flatteningApplyTest() throws Exception {
    File currentDir = new File(".");
    DataSource source =
        new FileSource(Paths.get(currentDir.getAbsolutePath(), "testdata", "filescan", "three_col_mixed_types.txt")
            .toString());

    Schema schema =
        Schema.ofFields("int", Type.INT_TYPE, "delimited_string", Type.STRING_TYPE, "boolean", Type.BOOLEAN_TYPE);
    FileScanEncoding fs = new FileScanEncoding();
    fs.source = source;
    fs.schema = schema;
    fs.delimiter = ',';
    fs.quote = null;
    fs.escape = null;
    fs.skip = null;
    fs.opId = 0;
    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
    ExpressionOperator countColIdx = new VariableExpression(0);
    ExpressionOperator counter = new CounterExpression(countColIdx);
    expressions.add(new Expression("counter", counter));
    ExpressionOperator splitColIdx = new VariableExpression(1);
    ExpressionOperator regex = new ConstantExpression(":");
    ExpressionOperator split = new SplitExpression(splitColIdx, regex);
    expressions.add(new Expression("split", split));
    FlatteningApplyEncoding flatApply = new FlatteningApplyEncoding();
    flatApply.emitExpressions = expressions.build();
    flatApply.columnsToKeep = ImmutableList.of(2);
    flatApply.argChild = fs.opId;
    flatApply.opId = 1;
    RelationKey outputRelation = RelationKey.of("test", "flatteningApply", "output");
    DbInsertEncoding insert = new DbInsertEncoding();
    insert.opId = 2;
    insert.argChild = flatApply.opId;
    insert.relationKey = outputRelation;
    insert.argOverwriteTable = true;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(fs, flatApply, insert);
    // HACK: if this test runs with multiple workers then each will duplicate the FileScan input relation and send
    // the coordinator duplicate results which will be concatenated in the output.
    int minWorkerID = Ints.min(workerIDs);
    frag.overrideWorkers = ImmutableList.of(minWorkerID);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "FlatteningApply test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);

    String data =
        JsonAPIUtils.download("localhost", masterDaemonPort, outputRelation.getUserName(), outputRelation
            .getProgramName(), outputRelation.getRelationName(), "json");;
    String expectedData =
        "[{\"boolean\":true,\"counter\":0,\"split\":\"a\"},{\"boolean\":true,\"counter\":0,\"split\":\"b\"},{\"boolean\":false,\"counter\":0,\"split\":\"c\"},{\"boolean\":false,\"counter\":0,\"split\":\"d\"},{\"boolean\":false,\"counter\":1,\"split\":\"c\"},{\"boolean\":false,\"counter\":1,\"split\":\"d\"}]";
    assertEquals(expectedData, data);
  }
}
