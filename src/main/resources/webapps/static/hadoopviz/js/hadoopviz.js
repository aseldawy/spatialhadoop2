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
      var window_hash = "#"+path;
      if (window.location.hash != window_hash)
        window.location.hash = window_hash;

      fileStatuses = data["FileStatuses"];
      
      // Display the content of the current directory
      dust.render("file-list-template", fileStatuses, function(err, out) {
        jQuery("#listed-files").html(out);
      });
      
      // If it is a dataset, further visualize its contents
      if (data["MasterPath"] != null) {
        // There is a master file, display it
        var masterFilePath = path+"/"+data["MasterPath"];
        var masterURL = "/hdfs"+masterFilePath;
        jQuery.ajax({
          url: masterURL,
          method: "GET",
          dataType: "text",
          processData: false
        }).success(function(gindex_csv) {
          var gindex_json = convertCSVToJSON(gindex_csv);
          if (data["ImagePath"] != null)
            gindex_json["ImagePath"] = path+"/"+data["ImagePath"];
          else
            gindex_json["BaseDir"] = path;
          gindex_json["Shape"] = data["Shape"];
          dust.render("global-index-template", gindex_json, function(err, out) {
            jQuery("#global-index").html(out);
            jQuery(".toggle-chekbox").change(checkboxToggle);
          });
          jQuery("#frm-visualize").submit(ajaxVisualize);
        });
      } else {
        if (data["ImagePath"] != null) {
          // There is an image, display it
          var gindex_json = {
            ImagePath: path+"/"+data["ImagePath"],
            ImageWidth: jQuery("#global-index").width(),
            ImageHeight: jQuery(window).height()
          };
          dust.render("global-index-template", gindex_json, function(err, out) {
            jQuery("#global-index").html(out);
            registerCheckboxes();
          });
        } else {
          jQuery("#global-index").html("");
        }        
      }
    }).error(function(xhr, data) {alert(data);});
  }

  dust.loadSource(dust.compile($('#file-list-template').html(), 'file-list-template'));
  dust.loadSource(dust.compile($('#global-index-template').html(), 'global-index-template'));
  
  /**
   * Update the contents of the current file from the hash list.
   */
  function updateFromHash() {
    var path = window.location.hash.slice(1);
    if (path.length == 0)
      path = '/';

    listFiles(path);
  }
  
  /**
   * Takes the size in bytes and returns a human readable string.
   */
  function humanReadableSize(size) {
    var units = [ "", "KB", "MB", "GB", "TB", "PB" ];
    var unit = 0;
    while (unit < units.length && size > 1024) {
      size /= 1024;
      unit++;
    }
    return size.toFixed(2) + " "+ units[unit];
  }
  
  /**
   * Formats the date to appear in the file list as it does with HDFS command
   */
  function formatDate(date) {
    date = new Date(date);
    var year = date.getFullYear();
    var month = date.getMonth() + 1;
    var day = date.getDate();
    var hours = date.getHours();
    var minutes = date.getMinutes();
    return year+"-"+month+"-"+day+" "+hours+":"+minutes;
  }
  
  /**
   * Adds a Dust helper that formats the date
   */
  dust.helpers.formatDateHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(formatDate(parseInt(value)));
  };

  /**
   * Adds a Dust helper that formats the size in human readable form
   */
  dust.helpers.formatSizeHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(humanReadableSize(parseInt(value)));
  };
  
  /**
   * Convert the global index from CSV to JSON format to feed to Dust.
   */
  function convertCSVToJSON(csv) {
    var screen_width = jQuery("#global-index").width();
    var screen_height = jQuery(window).height();
    
    // Convert the data to JSON and calculate the MBR of the input
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
    
    // Scale all partitions to fill the screen
    // Notice that we could use SVG scale functionality, however, it scales
    // the stroke-width as well which we do not wish to scale.
    // Scaling the stroke-width might cause inputs with very large domain
    // to produce rectangles with very thin strokes, and similarly, inputs
    // with very small domain to produce rectangles with very thick strokes.
    if (((maxx-minx) / (maxy-miny)) > (screen_width/screen_height)) {
      // Keep width fixed and change height
      screen_height = screen_width * (maxy-miny) / (maxx-minx);
    } else {
      // Keep height fixed and change width
      screen_width = screen_height * (maxx-minx) / (maxy-miny);
    }
    for (var i = 0; i < json_lines.length; i++) {
      var json_line = json_lines[i];
      // Scale x-dimension to be in the range [0, screen_width)
      json_line["x1"] = (json_line["x1"] - minx) * screen_width / (maxx-minx);
      json_line["x2"] = (json_line["x2"] - minx) * screen_width / (maxx-minx);
      // Scale y-dimension to be in the range [0, screen_height)
      // Also flip the records vertically to match the generated image
      var y1 = (json_line["y1"] - miny) * screen_height / (maxy-miny);
      var y2 = (json_line["y2"] - miny) * screen_height / (maxy-miny);
      json_line["y2"] = screen_height - y1;
      json_line["y1"] = screen_height - y2;
    }
    
    return {
      Partitions: json_lines,
      ImageWidth: screen_width, ImageHeight: screen_height
    };
  }
  
  function checkboxToggle() {
    var checkbox = jQuery(this);
    var target = jQuery("#"+checkbox.attr("data-target"));
      if (checkbox.is(":checked"))
        target.show();
      else
        target.hide();
  }
  
  // Register address change of the URL
  jQuery(window).bind('hashchange', updateFromHash);
  
  jQuery("form#navigate").submit( function(e) {
    listFiles(jQuery("#current-path").val());
    e.preventDefault();
  });
  
  function ajaxVisualize(e) {
    var form = jQuery(this);
    var url = form.attr("action");
    var form_data = form.serialize();
    jQuery.ajax({
      url: url,
      method: form.attr("method"),
      data: form_data
    }).success(function(data) {
      window.location = data["TrackURL"];
    });
    e.preventDefault();
  }

  // Initially, use the URL to initially list the files
  updateFromHash();
});
