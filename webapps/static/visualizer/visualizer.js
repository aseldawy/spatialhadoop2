$(document).ready(function() {
  $("#file-selector").change( function() {
    var $this = $(this);
    var selected = $this.find(":selected");
    var files = selected.map( function(index, obj) {
      return obj.value;
    });
    
    $.ajax({
      url: "/plotter.jsp",
      data: {files: jQuery.makeArray(files).join(",")},
      success: function(response) {
        $('#preview-img').html(response);
      }, error: function(xhr, status) {
        alert('err: ' + status);
      }
    });
  });
  
  $("#spatial-join-button").click( function() {
    var files = $("#file-selector").find(":selected");
    if (files.length != 2) {
      alert("Please select two files for spatial join");
      return;
    }
    $("#spatial-join-dialog input[name='input1']").val($(files[0]).text());
    $("#spatial-join-dialog input[name='input2']").val($(files[1]).text());
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
});
