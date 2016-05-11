/**
 Copyright (c) 2015 by Regents of the University of Minnesota.
 All rights reserved. This program and the accompanying materials
 are made available under the terms of the Apache License, Version 2.0 which 
 accompanies this distribution and is available at
 http://www.opensource.org/licenses/apache2.0.php.
 */
$(function() {

  // List the contents of a directory
  function listFiles(path, data) {
    if (jQuery("#current-path").val() != path)
      jQuery("#current-path").val(path);
    if (path.endsWith("/"))
      path = path.substring(0, path.length - 1);
    fileStatuses = data["FileStatuses"];
    // Add BaseDir and ParentDir to the JSON information to use in the page
    fileStatuses["BaseDir"] = path;
    var lastPathSeparator = path.lastIndexOf('/');
    if (lastPathSeparator != -1) {
      var parentDir = path.substring(0, lastPathSeparator+1);
      fileStatuses["ParentDir"] = parentDir;
    }
    // Display the content of the current directory
    dust.render("file-list-template", fileStatuses, function(err, out) {
      jQuery("#listed-files").html(out);
    });
  }
  
  // Displays the contents of a file
  function showContents(path, offset, length) {
    // First, retrieve file status to know its length
    var open_url = "/webhdfs/v1"+path+"?op=OPEN&offset="+offset+"&length="+length;
    window.location.href = open_url;
/*    jQuery.ajax({
      url: open_url,
      method: 'GET',
      processData: false
    }).always(function(jqXHR, textStatus, errorThrown) {alert(jqXHR.statusText); });*/
  }
  
  // Display the contents of a path.
  // If it is a directory, list the contained files and subdirectories.
  // If it is a file, list its contents (up-to some limit)
  function display(path) {
    var url = '/webhdfs/v1'+path+'?op=LISTSTATUS';
    jQuery.get(url, function(data) {
      // Check whether it is a file or a directory
      var list = data["FileStatuses"]["FileStatus"];
      if (list.length == 1 && list[0]["pathSuffix"] == "") {
        // It is a file, display its contents
        showContents(path, 0, list[0]["length"]);
      } else {
        // It is a directory, list the contents
        listFiles(path, data);
      }
    });
  }

  dust.loadSource(dust.compile($('#file-list-template').html(), 'file-list-template'));
  
  function updateFromHash() {
    var path = window.location.hash.slice(1);
    if (path.length == 0)
      path = '/';

    display(path);
  }
  
  jQuery(window).bind('hashchange', updateFromHash);
  updateFromHash();
});
