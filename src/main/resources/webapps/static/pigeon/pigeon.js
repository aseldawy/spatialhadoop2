$(document).ready(function() {
  function headSelected() {
    var fselector = $("#file-selector");
    var selected = fselector.find(":selected");
    if (selected.size() == 0) {
      $('#preview-head').html('not available');
    } else {
      $.ajax({
        type: "GET",
        url: "/head.jsp",
        data: {
          file: selected.val(),
          lines: 10,
        },
        success: function(response) {
          $('#preview-head').html(response.trim());
        }, error: function(xhr, error) {
          $('#preview-head').html('not available');
          //alert(xhr.responseText);
        }
    });
    }
  }

  $("#file-selector").change(headSelected);
  $("#file-selector").dblclick(function() {
    var fselector = $("#file-selector");
    var selected = fselector.find(":selected");
    if (selected.size() > 0) {
      window.location.href = "pigeon.jsp?dir="+selected.val();
    }
  });

  // For initial case
  if (!run_script)
    headSelected();
  
  function previewScript() {
    var scriptSelector = $("#pigeon-scripts");
    var selected = scriptSelector.find(":selected");
    if (selected.size() == 0) {
      $('#preview-head').html('not available');
    } else {
      $('#script-name').val(selected.text());
    
      // Retrieve script body
      $.ajax({
        type: "GET",
        url: "/preview-script.jsp",
        data: {
          id: selected.val(),
          part: "body"
        },
        success: function(response) {
          $('#script-body').val(response.trim());
        }, error: function(xhr, error) {
          $('#script-body').val('not available');
          alert(xhr.responseText);
        }
      });
      
      // Retrieve script log
      $.ajax({
        type: "GET",
        url: "/preview-script.jsp",
        data: {
          id: selected.val(),
          part: "log"
        },
        success: function(response) {
          $('#preview-head').html(response.trim());
        }, error: function(xhr, error) {
          $('#preview-head').html(xhr.responseText);
        }
      });
    }
 
  }
  
  $('#pigeon-scripts').change(previewScript);
});
