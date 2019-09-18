package io.cdap.plugin.mfs;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class MultipleFileSinkConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_SCHEMA = Schema.recordOf(
    "schema", Schema.Field.of("ID", Schema.nullableOf(Schema.of(Schema.Type.LONG))));

  private static final MultipleFileConfig VALID_CONFIG = new MultipleFileConfig(
      "myfile",
        "/tmp/data",
        "Text",
        "CSV",
        "ID > 0",
        "Expression"
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidOutputType() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setType("nonExistentType")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_OUTPUT_TYPE);
  }

  @Test
  public void testInvalidOutputFormat() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFormat("nonExistentFormat")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_OUTPUT_FORMAT);
  }

  @Test
  public void testInvalidFiltersType() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFiltersType("nonExistentFiltersType")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_FILTERS_TYPE);
  }

  @Test
  public void testNoFiltersForExpressionType() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFilters(null)
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_FILTERS);
  }

  @Test
  public void testHasFiltersForNoneType() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFiltersType("None")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_FILTERS);
  }

  @Test
  public void testFilterExpressionInvalid() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFilters("x >> 0")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_FILTERS);
  }

  @Test
  public void testFilterExpressionVariableNotFromSchema() {
    MultipleFileConfig config = MultipleFileConfig.builder(VALID_CONFIG)
      .setFilters("nonExistentField = 0")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, MultipleFileConfig.PROPERTY_FILTERS);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
