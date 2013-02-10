package edu.umn.cs.spatialHadoop.operations;


public class ProgramDriver extends org.apache.hadoop.util.ProgramDriver {
  public void addClass(String name, Class<?> mainClass, String description, String ... args)
      throws Throwable {
    if (args.length > 0) {
      description += "\nArguments";
      for (String arg : args) {
        description += "\n";
        description += arg;
      }
    }
    description += "\n--------";
    super.addClass(name, mainClass, description);
  }
}
