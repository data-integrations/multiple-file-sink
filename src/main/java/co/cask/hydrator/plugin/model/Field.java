package co.cask.hydrator.plugin.model;


public class Field {

  private String name;
  private String type;
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\": \"" + name +"\",\n" );
    sb.append("\"type\": \"" + type +"\",\n" );
    sb.append("},\n");
    return sb.toString();
  }
}
