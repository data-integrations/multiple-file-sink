package co.cask.hydrator.plugin.model;

import java.util.Arrays;
import java.util.List;

public class Schema {

  private String type;
  private String name;

  private List<Field> fields;

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

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
    sb.append("\"type\": \"" + type +"\",\n" );
    sb.append("\"name\": \"" + name +"\",\n" );
    sb.append("\"fields\":"+ Arrays.toString(fields.toArray())+"\n");
    sb.append("}\n");
    return sb.toString();
  }
}
