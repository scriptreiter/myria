package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.ExpressionEncoding;
import edu.washington.escience.myria.expression.AbsExpression;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.PowExpression;
import edu.washington.escience.myria.expression.SqrtExpression;
import edu.washington.escience.myria.expression.TimesExpression;
import edu.washington.escience.myria.expression.ToUpperCaseExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.TupleSource;

public class ApplyTest {

  private final int NUM_TUPLES = 2 * TupleBatch.BATCH_SIZE;

  @Test
  public void testApply() throws DbException {
    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.INT_TYPE, Type.STRING_TYPE), ImmutableList.of(
            "a", "b", "c", "d"));
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < NUM_TUPLES; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
      tbb.put(1, i + 1);
      tbb.put(2, (int) i);
      tbb.put(3, "Foo" + i);
    }
    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();

    ExpressionOperator vara = new VariableExpression(0);
    ExpressionOperator varb = new VariableExpression(1);
    ExpressionOperator varc = new VariableExpression(2);
    ExpressionOperator vard = new VariableExpression(3);

    {
      // Expression: Math.sqrt(a);

      ExpressionOperator squareRoot = new SqrtExpression(vara);

      ExpressionEncoding exprEnc = new ExpressionEncoding("first", squareRoot);

      expressions.add(exprEnc.construct());
    }

    {
      // Expression: (b+c) * (b-c)

      ExpressionOperator plus = new PlusExpression(varb, varc);
      ExpressionOperator minus = new MinusExpression(varb, varc);

      ExpressionOperator times = new TimesExpression(plus, minus);

      ExpressionEncoding exprEnc = new ExpressionEncoding("second", times);
      expressions.add(exprEnc.construct());
    }

    {
      // Expression: Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2))

      ExpressionOperator two = new ConstantExpression(Type.INT_TYPE, "2");
      ExpressionOperator pow1 = new PowExpression(vara, two);
      ExpressionOperator pow2 = new PowExpression(varb, two);

      ExpressionOperator plus = new PlusExpression(pow1, pow2);

      ExpressionOperator sqrt = new SqrtExpression(plus);

      ExpressionEncoding exprEnc = new ExpressionEncoding("third", sqrt);
      expressions.add(exprEnc.construct());
    }

    {
      // Expression: d.toUpperCase()

      ExpressionOperator upper = new ToUpperCaseExpression(vard);

      ExpressionEncoding exprEnc = new ExpressionEncoding("fourth", upper);
      expressions.add(exprEnc.construct());
    }

    {
      // Expression: Math.abs(b-a)

      ExpressionOperator abs = new AbsExpression(new MinusExpression(varb, vara));

      ExpressionEncoding exprEnc = new ExpressionEncoding("fifth", abs);
      expressions.add(exprEnc.construct());
    }

    Apply apply = new Apply(new TupleSource(tbb), expressions.build());

    apply.open(null);
    TupleBatch result;
    int resultSize = 0;
    while (!apply.eos()) {
      result = apply.nextReady();
      if (result != null) {
        assertEquals(5, result.getSchema().numColumns());
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(0));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(1));
        assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(2));
        assertEquals(Type.STRING_TYPE, result.getSchema().getColumnType(3));
        assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(4));

        assertEquals("first", result.getSchema().getColumnName(0));
        assertEquals("second", result.getSchema().getColumnName(1));
        assertEquals("third", result.getSchema().getColumnName(2));
        assertEquals("fourth", result.getSchema().getColumnName(3));
        assertEquals("fifth", result.getSchema().getColumnName(4));
        for (int curI = 0; curI < result.numTuples(); curI++) {
          long i = curI + resultSize;
          assertEquals(i, result.getDouble(0, curI), 0.0000001);
          long a = (long) Math.pow(i, 2);
          long b = i + 1;
          int c = (int) i;
          String d = ("Foo" + i).toUpperCase();
          assertEquals((b + c) * (b - c), result.getLong(1, curI));
          assertEquals(Math.sqrt(Math.pow(a, 2) + Math.pow(b, 2)), result.getDouble(2, curI), 0.0000001);
          assertEquals(d, result.getString(3, curI));
          assertEquals(Math.abs(b - a), result.getLong(4, curI));
        }
        resultSize += result.numTuples();
      }
    }
    assertEquals(NUM_TUPLES, resultSize);
    apply.close();
  }
}
