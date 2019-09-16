package io.cdap.plugin.mfs.expression;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Class description here.
 */
public class ELTest {

  @Ignore
  @Test
  public void testExpression() throws Exception {
    EL el = new EL(new EL.DefaultFunctions());
    el.compile("`/path/window/${Sex}`");
    ELContext context = new ELContext();
    context.add("Sex", "female");
    ELResult execute = el.execute(context);
    Assert.assertEquals(true, execute.getBoolean());
  }
}