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
});
