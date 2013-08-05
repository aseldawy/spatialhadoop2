$(document).ready(function() {
  $("#file-selector").change( function() {
    var $this = $(this);
    var selected = $this.find(":selected");
    var files = selected.map( function(index, obj) {
      return obj.value;
    });
    $.get("/plotter.jsp", {
      data: files[0],
      success: function(response) {
        alert(response);
      }, error: function(xhr, status) {
        alert(status);
      }
    });
  });
});
