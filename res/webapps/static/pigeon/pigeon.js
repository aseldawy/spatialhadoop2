$(document).ready(function() {
  function headSelected() {
    var fselector = $("#file-selector");
    var selected = fselector.find(":selected");
    if (selected.size() == 0) {
      $('#preview-head').html('not available');
    } else {
      $.ajax({
        url: "/head.jsp",
        method: "GET",
        data: {
          file: selected.val(),
          lines: 10,
        },
        success: function(response) {
          $('#preview-head').html(response.trim());
        }, error: function(xhr, status) {
          $('#preview-head').html('not available');
          alert('err: ' + status);
        }
    });
    }
    
  }

  $("#file-selector").change(headSelected);
  
  // For initial case
  headSelected();
});
