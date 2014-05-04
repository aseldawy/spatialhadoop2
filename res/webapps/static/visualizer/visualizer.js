$(document).ready(function() {
  function plotSelected() {
    var fselector = $("#file-selector");
    var selected = fselector.find(":selected");
    if (selected.size() == 0) {
      $('#preview-img').html('no preview available');
    } else {
      var files = selected.map( function(index, obj) {
        return obj.value;
      });
      $.ajax({
        url: "/plotter.jsp",
        data: {
          files: jQuery.makeArray(files).join(","),
          vflip: true,
          partitions: $("#partitions").is(':checked')
        },
        success: function(response) {
          $('#preview-img').html(response);
        }, error: function(xhr, status) {
          alert('err: ' + status);
          $('#preview-img').html('no preview available');
        }
    });
    }
  }

  $("#file-selector").change(plotSelected);
  $("#file-selector").dblclick(function() {
    var fselector = $("#file-selector");
    var selected = fselector.find(":selected");
    if (selected.size() > 0) {
      window.location.href = "visualizer.jsp?dir="+selected.val();
    }
  });

  $("#partitions").change(plotSelected);
  // Initialize preview area
  plotSelected();
  
  // ------------ Range query --------
  $("#range-query-button").click( function() {
    var files = $("#file-selector").find(":selected");
    if (files.length != 1) {
      alert("Please select one file for range query");
      return;
    }
    $("#range-query-dialog input[name='input']").val($(files[0]).val());
    rangeQueryUpdateQuery();
    $("#range-query-dialog").show();
  });
  
  $("#range-query-dialog input[type='reset']").click( function() {
    $("#range-query-dialog").hide();
  });
  
  $("#range-query-dialog form").submit( function(event) {
    var form = $(this);

    $.ajax({
      url: "/rangequery.jsp",
      data: form.serialize(),
      complete: function() {
        $("#range-query-dialog").hide();
      }, success: function(response) {
        $('#preview-img').html(response);
      }, error: function(xhr) {
        $('#preview-img').html(xhr.responseText);
      }
    });
    
    event.preventDefault();
  });
  
  function rangeQueryUpdateQuery() {
    var query = "input = LOAD '%input%' AS (geom: Geometry);\n\
result = FILTER input BY Overlap(geom, Rectangle(%query_rect%));\n\
STORE result INTO '%output%';";
    var input = $("#range-query-dialog input[name='input']").val();
    query = query.replace("%input%", input);
    var query_rect = $("#range-query-dialog input[name='xmin']").val() + ",";
    query_rect += $("#range-query-dialog input[name='ymin']").val() + ",";
    query_rect += $("#range-query-dialog input[name='xmax']").val() + ",";
    query_rect += $("#range-query-dialog input[name='ymax']").val();
    query = query.replace("%query_rect%", query_rect);
    var output = $("#range-query-dialog input[name='output']").val();
    query = query.replace("%output%", output);
    $("#range-query-dialog textarea").text(query);
  }
  
  $("#range-query-dialog form").change(rangeQueryUpdateQuery);

  // ------------ End of range query ------  
  
  
  // ------------ knn --------
  $("#knn-button").click( function() {
    var files = $("#file-selector").find(":selected");
    if (files.length != 1) {
      alert("Please select one file for knn");
      return;
    }
    $("#knn-dialog input[name='input']").val($(files[0]).val());
    knnUpdateQuery();
    $("#knn-dialog").show();
  });
  
  $("#knn-dialog input[type='reset']").click( function() {
    $("#knn-dialog").hide();
  });
  
  $("#knn-dialog form").submit( function(event) {
    var form = $(this);

    $.ajax({
      url: "/knn.jsp",
      data: form.serialize(),
      complete: function() {
        $("#knn-dialog").hide();
      }, success: function(response) {
        $('#preview-img').html(response);
      }, error: function(xhr) {
        $('#preview-img').html(xhr.responseText);
      }
    });
    
    event.preventDefault();
  });
  
  function knnUpdateQuery() {
    var query = "input = LOAD '%input%' AS (geom: Geometry);\n\
result = KNN %input% WITH_K=%k% USING Distance(geom, Point(%query_point%));\n\
STORE result INTO '%output%';";
    var input = $("#knn-dialog input[name='input']").val();
    query = query.replace("%input%", input);
    var query_point = $("#knn-dialog input[name='x']").val() + ",";
    query_point += $("#knn-dialog input[name='y']").val();
    query = query.replace("%query_point%", query_point);
    var k = $("#knn-dialog input[name='k']").val();
    query = query.replace("%k%", k);
    var output = $("#knn-dialog input[name='output']").val();
    query = query.replace("%output%", output);
    $("#knn-dialog textarea").text(query);
  }
  
  $("#knn-dialog form").change(knnUpdateQuery);

  // ------------ End of knn ------  
  
  
  
  // ------------ Spatial Join -------
  $("#spatial-join-button").click( function() {
    var files = $("#file-selector").find(":selected");
    if (files.length != 2) {
      alert("Please select two files for spatial join");
      return;
    }
    $("#spatial-join-dialog input[name='input1']").val($(files[0]).val());
    $("#spatial-join-dialog input[name='input2']").val($(files[1]).val());
    spatialJoinUpdateQuery();
    $("#spatial-join-dialog").show();
  });
  
  $("#spatial-join-dialog input[type='reset']").click( function() {
    $("#spatial-join-dialog").hide();
  });

  $("#spatial-join-dialog form").submit( function(event) {
    var form = $(this);

    $.ajax({
      url: "/spatialjoin.jsp",
      data: form.serialize(),
      complete: function() {
        $("#spatial-join-dialog").hide();
      }, success: function(response) {
        $('#preview-img').html(response);
      }, error: function(xhr) {
        $('#preview-img').html(xhr.responseText);
      }
    });
    
    event.preventDefault();
  });
  
  function spatialJoinUpdateQuery() {
    var query = "input1 = LOAD '%input1%' AS (geom: Geometry);\n\
input2 = LOAD '%input2%' AS (geom: Geometry);\n\
result = JOIN input1 BY geom\n\
  input2 BY geom PREDICATE = %predicate%;\n\
STORE result INTO '%output%';";
    var input1 = $("#spatial-join-dialog input[name='input1']").val();
    query = query.replace("%input1%", input1);
    var input2 = $("#spatial-join-dialog input[name='input2']").val();
    query = query.replace("%input2%", input2);
    var predicate = $("#spatial-join-dialog select[name='predicate']").val();;
    query = query.replace("%predicate%", predicate);
    var output = $("#spatial-join-dialog input[name='output']").val();
    query = query.replace("%output%", output);
    $("#spatial-join-dialog textarea").text(query);
  }
  
  $("#spatial-join-dialog form").change(spatialJoinUpdateQuery);
  // ------ End of spatial join -------
  
});
