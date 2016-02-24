/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop;

/**
 * Estimates a given value by taking a random sample from sample space until
 * the confidence interval of the calculated value is acceptable
 * @author Ahmed Eldawy
 *
 */
public class Estimator<Y> {
  private static final int N_MIN = 10;
  
  /**
   * An interface implemented by the user to provide a source of values to
   * be used in the estimate
   * @author Ahmed Eldawy
   *
   */
  public static interface RandomSample {
    double next();
  }
  
  /**
   * An interface implemented by the user to provide a calculated value given
   * an estimate for the random variable X.
   * @author Ahmed Eldawy
   *
   * @param <Y> The type of objects to sample
   */
  public static interface UserFunction<Y> {
    Y calculate(double x);
  }
  
  /**
   * Given a confidence interval for the calculated value, determine whether
   * this is good enough or we need to sample more data.
   * @author Ahmed Eldawy
   *
   * @param <Y> The type of objects to sample
   */
  public static interface QualityControl<Y> {
    boolean isAcceptable(Y y1, Y y2);
  }
  
  /**
   * Stores a range of calculated values
   * @author Ahmed Eldawy
   *
   * @param <Y> The type of objects to sample
   */
  public static class Range<Y> {
    public Y limit1, limit2;
    
    public Range(Y limit1, Y limit2) {
      this.limit1 = limit1;
      this.limit2 = limit2;
    }
  }
  
  /**
   * A value from [0, 1] to determine how large is the confidence interval
   * needed. A value closer to zero gives a narrow interval. For example,
   * setting this value to 0.05 returns a 95% confidence interval.
   */
  protected double alpha;
  
  /**
   * The percentile of the normal distribution corresponding to conifdence
   */
  protected double z;
  
  /**A source of values coming from a random sample*/
  protected RandomSample randomSample;
  
  /**The user provided function that calculates the value the user wants*/
  protected UserFunction<Y> userFunction;
  
  /**Determines whether the current interval for the target value is good*/
  protected QualityControl<Y> qualityControl;
  
  public Estimator(double alpha) {
    this.alpha = alpha;
    // z = qnorm(1 - this.alpha / 2)
//    z = 1.959964; // This corresponds to 95% confidence (alpha = 0.05)
    z = 2.575829; // This corresponds to 99% confidence (alpha = 0.05)
  }

  public double getAlpha() {
    return alpha;
  }

  public void setConfidence(float confidence) {
    this.alpha = confidence;
  }

  public void setRandomSample(RandomSample randomSample) {
    this.randomSample = randomSample;
  }

  public void setUserFunction(UserFunction<Y> userFunction) {
    this.userFunction = userFunction;
  }

  public void setQualityControl(QualityControl<Y> qualityControl) {
    this.qualityControl = qualityControl;
  }
  
  public Range<Y> getEstimate() {
    double sum_x = 0.0;
    double sum_x2 = 0.0;
    int n = 0;

    while (n < N_MIN) {
      double x = randomSample.next();
      sum_x += x;
      sum_x2 += x*x;
      n++;
    }
    
    Y y1, y2;
    
    do {
      double x = randomSample.next();
      sum_x += x;
      sum_x2 += x*x;
      
      n++;
      
      double x_bar = sum_x / n;
      double s2 = (n * sum_x2 - sum_x * sum_x) / n / (n-1);
      double x_lb = x_bar - z * Math.sqrt(s2/n);
      double x_ub = x_bar + z * Math.sqrt(s2/n);
      
      y1 = userFunction.calculate(x_lb);
      y2 = userFunction.calculate(x_ub);
    } while (!qualityControl.isAcceptable(y1, y2));
    
    return new Range<Y>(y1, y2);
  }
  
  public static void main(String[] args) {
    Estimator<Integer> lineEstimator = new Estimator<Integer>(0.05);
    lineEstimator.setRandomSample(new RandomSample() {
      
      @Override
      public double next() {
        return Math.random() * 5 + 20;
      }
    });
    
    lineEstimator.setUserFunction(new UserFunction<Integer>() {
      
      @Override
      public Integer calculate(double x) {
        return (int)(1000000000 / x);
      }
    });
    
    lineEstimator.setQualityControl(new QualityControl<Integer>() {
      
      @Override
      public boolean isAcceptable(Integer y1, Integer y2) {
        return (double)Math.abs(y2 - y1) / Math.min(y1, y2) < 0.01;
      }
    });
    
    Estimator.Range<Integer> line_count = lineEstimator.getEstimate();
    System.out.println("Lines: "+line_count);
  }
}
