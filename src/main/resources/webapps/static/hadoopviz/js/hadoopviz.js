/**
 Copyright (c) 2015 by Regents of the University of Minnesota.
 All rights reserved. This program and the accompanying materials
 are made available under the terms of the Apache License, Version 2.0 which 
 accompanies this distribution and is available at
 http://www.opensource.org/licenses/apache2.0.php.
 */
$(function() {

  // List the contents of a directory
  function listFiles(path) {
    var url = '/LISTSTATUS.cgi';
    url += '?path='+encodeURIComponent(path);
    jQuery.get(url, function(data) {
      if (jQuery("#current-path").val() != path)
        jQuery("#current-path").val(path);

      fileStatuses = data["FileStatuses"];
      
      // Display the content of the current directory
      dust.render("file-list-template", fileStatuses, function(err, out) {
        jQuery("#listed-files").html(out);
      });
      
      // If it is a dataset, further visualize its contents
      if (data["image"] != null) {
        // There is an image, display it
      }
      if (data["master"] != null) {
        // There is a master file, display it
        var masterFilePath = path+"/"+data["master"];
        var masterURL = "/hdfs"+masterFilePath;
        jQuery.ajax({
          url: masterURL,
          method: "GET",
          dataType: "text",
          processData: false
        }).success(function(data) {
          var masterJSON = convertCSVToJSON(data);
          dust.render("global-index-template", masterJSON, function(err, out) {
            jQuery("#global-index").html(out);
          });
        });
      }
    }).error(function(xhr, data) {alert(data);});
  }

  dust.loadSource(dust.compile($('#file-list-template').html(), 'file-list-template'));
  dust.loadSource(dust.compile($('#global-index-template').html(), 'global-index-template'));
  
  function updateFromHash() {
    var path = window.location.hash.slice(1);
    if (path.length == 0)
      path = '/';

    listFiles(path);
  }
  
  function humanReadableSize(size) {
    var units = [ "", "KB", "MB", "GB", "TB", "PB" ];
    var unit = 0;
    while (unit < units.length && size > 1024) {
      size /= 1024;
      unit++;
    }
    return size.toFixed(2) + " "+ units[unit];
  }
  
  function formatDate(date) {
    date = new Date(date);
    var year = date.getFullYear();
    var month = date.getMonth() + 1;
    var day = date.getDate();
    var hours = date.getHours();
    var minutes = date.getMinutes();
    return year+"-"+month+"-"+day+" "+hours+":"+minutes;
  }
  
  dust.helpers.formatDateHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(formatDate(parseInt(value)));
  };

  dust.helpers.formatSizeHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(humanReadableSize(parseInt(value)));
  };
  
  function convertCSVToJSON(csv) {
    var lines = csv.split("\n");
    var json_lines = [];
    var minx = Infinity, miny = Infinity,
        maxx = -Infinity, maxy = -Infinity;
    for (var i = 0; i < lines.length; i++) {
      var parts = lines[i].split(',');
      if (parts.length == 8) {
        var this_line = json_lines[i] = {
          cell_id: parseInt(parts[0]),
          x1: parseFloat(parts[1]),
          y1: parseFloat(parts[2]),
          x2: parseFloat(parts[3]),
          y2: parseFloat(parts[4]),
          numRecords: parseInt(parts[5]),
          size: parseInt(parts[6]),
          filename: parts[7]
        };
        if (this_line["x1"] < minx)
          minx = this_line["x1"];
        if (this_line["y1"] < miny)
          miny = this_line["y1"];
        if (this_line["x2"] > maxx)
          maxx = this_line["x2"];
        if (this_line["y2"] > maxy)
          maxy = this_line["y2"];
      }
    }
    return {
      Partitions: json_lines,
      minx: minx, miny: miny,
      maxx: maxx, maxy: maxy
    };
  }
  
  jQuery(window).bind('hashchange', updateFromHash);
  updateFromHash();
});
