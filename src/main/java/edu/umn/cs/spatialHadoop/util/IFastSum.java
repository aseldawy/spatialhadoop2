/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.util;

/**
 * A Java implementation of the iFastSum algorithm as it appears in:
 * Yong-Kang Zhu and Wayne B. Hayes, "Correct Rounding and a Hybrid Approach
 * to Exact Floating-point Summation", SIAM J. Sci. Comput. 31 (4) pp 2981-3001, 2009
 * The code is ported from the original C++ application accompanied with the
 * paper above.
 *
 * @author Ahmed Eldawy
 */
public class IFastSum {

  /** Number of bits in the mantissa */
  private static int MANTISSA_BITS = 52;

  /** Mask for the exponent part */
  private static long EXP_MASK = 0x7FF0000000000000L;

  /** the number of exponents for IEEE754 double */
  protected static final int N_EXPONENT = 2048;

  /** the length of the accumulators, i.e., 2 X N_EXPONENT */
  protected static final int N2_EXPONENT = 2 * N_EXPONENT;

  /** number of bits in a split mantissa */
  protected static final int HALF_MANTISSA = 26;

  /** Masks the high part of the mantissa */
  protected static final long HIGH_MANTISSA_MASK = 0x000FFFFF00000000L;

  /** Masks the low part of the mantissa */
  protected static final long LOW_MANTISSA_MASK = 0x00000000FFFFFFFFL;

  /** Max number of split mantissas that can be summed without error */
  protected static final long MAX_N = 1L << HALF_MANTISSA; // 2^HALF_MANTISSA
  protected static final long MAX_N_AFTER_SWAP = MAX_N - N2_EXPONENT;

  /** a global used by iFastSum */
  public static int r_c;

  public IFastSum() {
  }

  static protected void AddTwo(double[] vals) {
    double t = vals[0] + vals[1];
    vals[1] = (Double.doubleToRawLongBits(vals[0]) & EXP_MASK) < (Double.doubleToRawLongBits(vals[1]) & EXP_MASK) ?
        ((vals[1] - t) + vals[0]) : ((vals[0] - t) + vals[1]);
    vals[0] = t;
  }

  /** Return true if not correctly rounded; false otherwise */
  static protected boolean Round3(double s0, double s1, double s2) {
    // To check "s1 is half-ulp", s1!=0 and all the mantissa bits=0 only work
    // for normalized numbers. But if s1 is de-normalized, then according to
    // the two places where Round3 gets called, \hat s_2 must be zero, which
    // means s0 is correctly rounded.
    long is1 = Double.doubleToRawLongBits(s1);
    return (s1 != .0 && (HIGH_MANTISSA_MASK & is1) == 0 && (LOW_MANTISSA_MASK & is1) == 0 && s1 * s2 > 0);
  }

  public static double iFastSum(double[] num_list, int size) {
    if (size < 1)
      return .0;
    double s = 0, s_t, s1, s2, e1, e2;
    int count; // next position in num_list to store error
    int c_n = size; // current number of summands
    long max = 0; // the max exponent of s_t
    int i;
    double t, e_m;
    double half_ulp = .0; // half an ulp of s

    double EPS = 1.0;
    // EPS.exponents -= 53
    long iEPS = Double.doubleToRawLongBits(EPS);
    long exp = iEPS & EXP_MASK - (53 << MANTISSA_BITS); // exp - 53
    EPS = Double.longBitsToDouble((iEPS & ~EXP_MASK) | exp);

    double ev_d = .0;
    ev_d = Double.longBitsToDouble(Double.doubleToRawLongBits(ev_d) & 0x7FFFFFFFFFFFFFFFL);

    for (i = 1; i <= size; i++) {
      // AddTwo, inline
      t = s + num_list[i];
      num_list[i] = (Double.doubleToRawLongBits(s) & EXP_MASK) < (Double.doubleToRawLongBits(num_list[i]) & EXP_MASK) ?
          (num_list[i] - t) + s : (s - t) + num_list[i];
      s = t;
    }

    // Temporary array for AddTwo
    double[] temp = new double[2];

    while (true) {
      count = 1;
      s_t = .0;
      max = 0;
      for (i = 1; i <= c_n; i++) {
        // AddTwo, inline
        t = s_t + num_list[i];
        num_list[count] =
            (Double.doubleToRawLongBits(s_t) & EXP_MASK) <
                (Double.doubleToRawLongBits(num_list[i]) & EXP_MASK) ?
                (num_list[i] - t) + s_t : (s_t - t) + num_list[i];
        s_t = t;

        if (num_list[count] != 0) {
          count++;
          long s_t_exp = Double.doubleToRawLongBits(s_t) & EXP_MASK;
          if (max < s_t_exp)
            max = s_t_exp;
        }
      }

      // compute e_m, the estimated global error
      if (max > 0) // neither minimum exponent nor de-normalized
      {
        // ev_d.exponent = max;
        long iev_d = Double.doubleToRawLongBits(ev_d);
        iev_d = (iev_d & ~EXP_MASK) | max;
        ev_d = Double.longBitsToDouble(iev_d);
        ev_d *= EPS;
        e_m = ev_d * (count - 1);
      } else
        e_m = .0;

      // AddTwo(s, s_t)
      temp[0] = s;
      temp[1] = s_t;
      AddTwo(temp);
      s = temp[0];
      s_t = temp[1];

      num_list[count] = s_t;
      c_n = count;

      // compute HalfUlp(s)
      if ((Double.doubleToRawLongBits(s) & EXP_MASK) > 0) {
        // half_ulp.exponent = s.exponent
        long ihalf_ulp = Double.doubleToRawLongBits(half_ulp);
        ihalf_ulp = (ihalf_ulp & ~EXP_MASK) | (Double.doubleToRawLongBits(s) & EXP_MASK);
        half_ulp = Double.longBitsToDouble(ihalf_ulp);
        half_ulp *= EPS;
      } else
        half_ulp = .0;

      if (e_m < half_ulp || e_m == .0) {
        if (r_c > 0)
          return s;
        s1 = s2 = s_t;
        e1 = e_m;
        e2 = -e_m;
        // AddTwo(s1, e1);
        temp[0] = s1;
        temp[1] = e1;
        AddTwo(temp);
        s1 = temp[0];
        e1 = temp[1];
        // AddTwo(s2, e2);
        temp[0] = s2;
        temp[1] = e2;
        AddTwo(temp);
        s2 = temp[0];
        e2 = temp[1];

        if (s + s1 != s || s + s2 != s || Round3(s, s1, e1) || Round3(s, s2, e2)) {
          r_c = 1;
          double ss1 = iFastSum(num_list, c_n);
          // AddTwo(s, s1);
          temp[0] = s;
          temp[1] = ss1;
          AddTwo(temp);
          s = temp[0];
          ss1 = temp[1];
          double ss2 = iFastSum(num_list, c_n);
          r_c = 0;
          if (Round3(s, ss1, ss2)) {
            //s1->mantissa |= 0x1;
            // The magnify function
            s1 = Double.longBitsToDouble(Double.doubleToRawLongBits(s1) | 0x1);
            s += ss1;
          }
        }
        return s;
      }
    }
  }

}
