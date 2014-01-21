package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Take the {@link Math.sin} of the operand.
 */
public class SinExpression extends UnaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private SinExpression() {
    super();
  }

  /**
   * Take the {@link Math.sin} of the operand.
   * 
   * @param operand the operand.
   */
  public SinExpression(final ExpressionOperator operand) {
    super(operand);
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    checkAndReturnDefaultNumericType(schema, stateSchema);
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return getFunctionCallUnaryString("Math.sin", schema, stateSchema);
  }
}