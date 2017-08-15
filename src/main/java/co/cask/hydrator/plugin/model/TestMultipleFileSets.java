package co.cask.hydrator.plugin.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestMultipleFileSets {
  public static void main(String[] args) throws IOException {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String fileData = new String(Files.readAllBytes
      (Paths.get("./src/main/java/co/cask/hydrator/plugin/model/multipleFileSets.txt")));

    MultipleFileSets multipleFileSets= gson.fromJson(fileData, MultipleFileSets.class);
    System.out.println(multipleFileSets.getOutputFileSets().get(0).getSchema().toString());

  }
}
