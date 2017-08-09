package co.cask.hydrator.plugin.model;

public class Field {

  private String name;

  private String type;

  public String getName(){
    return this.name;
  }

  public void setName(String name){
    this.name = name;
  }

  public String getType(){
    return this.type;
  }

  public void setType(String name){
    this.type = name;
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
