package edu.umn.cs.spatialHadoop.util;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.net.NetUtils;

public class JspHelper extends org.apache.hadoop.hdfs.server.namenode.JspHelper {
  public static String jobTrackUrl(String requestUrl, Configuration conf, RunningJob job) {
    // Create a link to the status of the running job
    String trackerAddress = conf.get("mapred.job.tracker.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(trackerAddress);
    int cutoff = requestUrl.indexOf('/', requestUrl.lastIndexOf(':'));
    requestUrl = requestUrl.substring(0, cutoff);
    InetSocketAddress requestSocAddr = NetUtils.createSocketAddr(requestUrl);
    String address = "http://"+requestSocAddr.getHostName()+":"+infoSocAddr.getPort()+
      "/jobdetails.jsp?jobid="+job.getID()+"&amp;refresh=30";
    return address;
  }
}
