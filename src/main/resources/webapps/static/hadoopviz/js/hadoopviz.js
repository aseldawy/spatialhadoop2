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
    }).error(function(xhr, data) {alert(data);});
  }

  dust.loadSource(dust.compile($('#file-list-template').html(), 'file-list-template'));
  
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
    return new Date(date).toLocaleFormat('%Y-%M-%d %H:%m');
  }
  
  dust.helpers.formatDateHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(formatDate(parseInt(value)));
  };

  dust.helpers.formatSizeHelper = function (chunk, context, bodies, params) {
    var value = dust.helpers.tap(params.value, chunk, context);
    return chunk.write(humanReadableSize(parseInt(value)));
  };
  
  jQuery(window).bind('hashchange', updateFromHash);
  updateFromHash();
});
