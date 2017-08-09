package co.cask.hydrator.plugin.model;

public class OutputFileSet {
  private String compressionCodec;
  private String datasetName;
  private String datasetTargetPaths;
  private String expression;
  private String filesetProperties;
  private Schema schema;

  public String getCompressionCodec() {
    return compressionCodec;
  }

  public void setCompressionCodec(String compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }

  public String getDatasetTargetPaths() {
    return datasetTargetPaths;
  }

  public void setDatasetTargetPaths(String datasetTargetPaths) {
    this.datasetTargetPaths = datasetTargetPaths;
  }

  public String getExpression() {
    return expression;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public String getFilesetProperties() {
    return filesetProperties;
  }

  public void setFilesetProperties(String filesetProperties) {
    this.filesetProperties = filesetProperties;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"compressionCodec\": \"" + compressionCodec +"\",\n" );
    sb.append("\"datasetName\": \"" + datasetName + "\",\n");
    sb.append("\"datasetTargetPaths\":\"" + datasetTargetPaths + "\",\n");
    sb.append("\"expression\":\"" + expression + "\",\n");
    sb.append("\"filesetProperties\":\"" + filesetProperties + "\",\n");
    sb.append("\"schema\":\"" + schema.toString() + ",\n");
    return sb.toString();
  }
}
