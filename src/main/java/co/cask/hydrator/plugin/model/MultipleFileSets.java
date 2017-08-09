package co.cask.hydrator.plugin.model;

import java.util.Arrays;
import java.util.List;

public class MultipleFileSets {

  private String name;
  private String type;
  private List<OutputFileSet> outputFileSets;

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

  public List<OutputFileSet> getOutputFileSets() {
    return outputFileSets;
  }

  public void setOutputFileSets(List<OutputFileSet> outputFileSets) {
    this.outputFileSets = outputFileSets;
  }

  @Override
  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("\"name\": \"" + name +"\",\n" );
    sb.append("\"type\":" + "\"" + type + "\",\n");
    sb.append("\"outputFileSets\":" + Arrays.toString(outputFileSets.toArray()) + "\n");
    sb.append("}\n");
    return sb.toString();
  }
}
