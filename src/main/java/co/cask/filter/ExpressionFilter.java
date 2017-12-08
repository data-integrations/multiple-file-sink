package co.cask.filter;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.expression.EL;
import co.cask.expression.ELContext;
import co.cask.expression.ELException;
import co.cask.expression.ELResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Class description here.
 */
public class ExpressionFilter extends RecordFilter {
  public static final Logger LOG = LoggerFactory.getLogger(ExpressionFilter.class);
  public static final String EXPRESSION_CONFIG = "filter.expression";
  private String expression;
  private EL el;
  private long errors = 0;

  public ExpressionFilter(String expression) {
    this.expression = expression;
  }

  @Override
  public void configure() throws IllegalArgumentException {
    el = new EL(new EL.DefaultFunctions());
    try {
      el.compile(expression);
    } catch (ELException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public boolean filter(StructuredRecord record) {
    ELContext context = new ELContext();
    for(Map.Entry<String, String> value : getArguments().entrySet()) {
      context.add(value.getKey(), value.getValue());
    }
    List<Schema.Field> fields = record.getSchema().getFields();
    for (Schema.Field field : fields) {
      Schema.Type type = field.getSchema().getType();
      if (type.isSimpleType() || field.getSchema().isNullableSimple()) {
        context.add(field.getName(), record.get(field.getName()));
      }
    }
    try {
      ELResult result = el.execute(context);
      return result.getBoolean();
    } catch (ELException e) {
      errors++;
      if (errors % 100 == 0) {
        LOG.info(String.format(
          "%d records have been skipped because the expression '%s' is having an issue.", errors, e.getMessage()
        ));
      }
    }
    return false;
  }
}


