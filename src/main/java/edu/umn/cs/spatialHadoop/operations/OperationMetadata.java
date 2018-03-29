package edu.umn.cs.spatialHadoop.operations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation describes the metadata of an operation that can be accessed
 * from command line.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface OperationMetadata {
  /**The short name used to access this operation from command line*/
  String shortName();

  /**A description of this operation*/
  String description();
}
