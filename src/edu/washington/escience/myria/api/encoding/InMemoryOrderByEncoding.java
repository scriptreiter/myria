package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class InMemoryOrderByEncoding extends OperatorEncoding<InMemoryOrderBy> {

  public String argChild;
  public int[] argSortColumns;
  public boolean[] argAscending;
  private static final List<String> requiredArguments = ImmutableList.of("argChild", "argSortColumns", "argAscending");

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public InMemoryOrderBy construct(Server server) throws MyriaApiException {
    return new InMemoryOrderBy(null, argSortColumns, argAscending);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }

  @Override
  protected void validateExtra() {

    if (argSortColumns.length != argAscending.length) {
      throw new MyriaApiException(Status.BAD_REQUEST, "sort columns number should be equal to ascending orders number!");
    }
  }

}