package co.cask.hydrator.plugin.model;

import java.util.Arrays;
import java.util.List;

public class Schema {
  private List<Field> fields;

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }
  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"fields\":"+ Arrays.toString(fields.toArray())+"\n");
    sb.append("},\n");
    return sb.toString();
  }
}


