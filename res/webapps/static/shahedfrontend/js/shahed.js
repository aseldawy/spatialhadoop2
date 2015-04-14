// The geocoder used to parse user-input addresses
var geocoder;
// The Google Maps object created in this page
var map;
// The draggable rectangle on the map
var rectangle;
var rectangleIsDragged = false;

function Fly2Destinaiton() {
  var address = document.getElementById("fly-to").value;
  geocoder.geocode({
    'address' : address
  }, function(results, status) {
    if (status == google.maps.GeocoderStatus.OK) {
      var result = results[0];
      var fromatted_address = result.formatted_address;
      $("#fly-to").val(fromatted_address);
      var bounds = result.geometry.bounds;
      map.fitBounds(bounds);
      MoveRectangle(bounds);
    } else {
      alert("Geocode was not successful for the following reason: " + status);
    }
  });
}

// Create the draggable rectangle for the first time and initialize listeners
function CreateRectangle() {
  var bounds = new google.maps.LatLngBounds(
     new google.maps.LatLng(0, 0),
     new google.maps.LatLng(1, 1));
  rectangle = new google.maps.Rectangle({
    bounds : bounds,
    fillOpacity : 0.2,
    strokeOpacity : 0.8,
    draggable : true,
    editable : true
  });
  google.maps.event.addListener(rectangle, 'mousedown', function() {rectangleIsDragged = true;});
  google.maps.event.addListener(rectangle, 'mouseup', function() {rectangleIsDragged = false;});
  if ($("#results-panel").length > 0)
    google.maps.event.addListener(rectangle, 'bounds_changed', aggregateQuery);
}

// Move the draggable rectangle to a specific location on the map           
function MoveRectangle(bounds) {
  rectangle.setMap(map); // Ensure it is visible if not
  rectangle.setBounds(bounds);
}

// TODO This variable should be thread-safe
var processingRequest = false;

// Process the request by submitting it to the backend server
function aggregateQuery(){
  if (rectangleIsDragged)
    return;
  if (processingRequest)
    return; // Another request already in progress
  if ($("#fromDatePicker").val().length == 0 || $("#toDatePicker").val().length == 0) {
    alert('Please specify start and end date');
    return;
  }
  if (rectangle == null) {
    alert("Please specify a rectangle");
    return;
  }
  processingRequest = true;
  var fromDate = document.getElementById("fromDatePicker").value;
  var toDate = document.getElementById("toDatePicker").value;
  var ne = rectangle.getBounds().getNorthEast();
  var sw = rectangle.getBounds().getSouthWest();
  requestURL = "cgi-bin/aggregate_query.cgi?"
                + "min_lat=" + sw.lat() + "&min_lon=" + sw.lng()
                + "&max_lat=" + ne.lat() + "&max_lon=" + ne.lng()
                + "&fromDate=" + fromDate
                + "&toDate=" + toDate;
  jQuery.ajax(requestURL, {success : function(data) {
    min = ((parseInt(data.results.min)/50) - 273.15) * 1.8000 + 32.00;
    $("#min").val(min);
    max = ((parseInt(data.results.max)/50) - 273.15) * 1.8000 + 32.00;
    $("#max").val(max);
    sum = data.results.sum;
    count = parseInt(data.results.count);
    average = ((sum/count / 50.0)-273.15)*1.8+32;
    $("#avg").val(average);
    timeInMillis = parseInt(data.stats.totaltime);
    $("#time").val(timeInMillis);
    $("#partitions").val(data.stats["num-of-temporal-partitions"]);
    $("#files").val(data.stats["num-of-trees"]);
  }, complete: function() {
    processingRequest = false;
  }});
}

function generateImage() {
  if ($("#fromDatePicker").val().length == 0) {
    alert('Please specify start date');
    return;
  }
  if (rectangle == null) {
    alert("Please specify a rectangle");
    return;
  }
  var username = $("#userName").val();
  var email = $("#email").val();
  var emailRegexp = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  if (!emailRegexp.test(email)) {
    alert("Please enter a valid email address to email you the generated files");
    return;
  }
  var fromDate = $("#fromDatePicker").val();
  var ne = rectangle.getBounds().getNorthEast();
  var sw = rectangle.getBounds().getSouthWest();
  var selectedDatasetOption = $("#datasets option:selected");
  var dataset = selectedDatasetOption.attr("id");
  var datasetURL = selectedDatasetOption.val();
  requestURL = "cgi-bin/generate_image.cgi?"
                + "min_lat=" + sw.lat() + "&min_lon=" + sw.lng()
                + "&max_lat=" + ne.lat() + "&max_lon=" + ne.lng()
                + "&fromDate=" + fromDate
                + "&toDate=" + fromDate // For images, from and to date are the same
                + "&dataset=" + dataset
                + "&dataset_url=" + datasetURL
                + "&user_name=" + username
                + "&email=" + email;
  // Send using Ajax
  jQuery.ajax(requestURL, {success: function(response) {
    alert(response);
  }});
}
    
function generateVideo() {
  if ($("#fromDatePicker").val().length == 0 || $("#toDatePicker").val().length == 0) {
    alert('Please specify start and end date');
    return;
  }
  if (rectangle == null) {
    alert("Please specify a rectangle");
    return;
  }
  var username = $("#userName").val();
  var email = $("#email").val();
  var emailRegexp = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  if (!emailRegexp.test(email)) {
    alert("Please enter a valid email address to email you the generated files");
    return;
  }
  var fromDate = $("#fromDatePicker").val();
  var toDate = $("#toDatePicker").val();
  var ne = rectangle.getBounds().getNorthEast();
  var sw = rectangle.getBounds().getSouthWest();
  var selectedDatasetOption = $("#datasets option:selected");
  var dataset = selectedDatasetOption.attr("id");
  var datasetURL = selectedDatasetOption.val();
  var requestURL = "cgi-bin/generate_image.cgi?"
                + "min_lat=" + sw.lat() + "&min_lon=" + sw.lng()
                + "&max_lat=" + ne.lat() + "&max_lon=" + ne.lng()
                + "&fromDate=" + fromDate
                + "&toDate=" + toDate
                + "&dataset=" + dataset
                + "&dataset_url=" + datasetURL
                + "&user_name=" + username
                + "&email=" + email;
  // Send using Ajax
  jQuery.ajax(requestURL, {success: function(response) {
    alert(response);
  }});
}
    
function formatDate(date) {
  var year = date.getFullYear();
  var month = (date.getMonth() + 1).toString();
  if (month.length == 1)
    month = "0" + month;
  var day = (date.getDate()).toString();
  if (day.length == 1)
    day = "0" + day;
  return month+"/"+day+"/"+year;         
}

var slider;  
var MillisPerDay = 1000 * 60 * 60 * 24; // Milliseconds in one day
var day0 = new Date(2012, 0, 1); // Minimum day supported

var waitImageURL = 'images/wait.gif';
var sessionoutRedirectURL = 'aggregate_query.html';
$(function () {
  // Assign handler to the "fly-to" input
  $('#fly-to').keypress(function(e) {
    if (e.keyCode == 13) {
      Fly2Destinaiton();
    }
  });

  // Initialize global Ajax handler to show the wait icon
  jQuery(document).ajaxStart( function() {
    jQuery("#modal").show();
    jQuery("#fade").show();
  }). ajaxComplete(function() {
    jQuery("#modal").hide();
    jQuery("#fade").hide();
  });
  
  // Initialize date picker for fromDate and toDate
  $("#toDatePicker").datepicker({
    dateFormat : 'mm/dd/yy',
    changeYear : true,
    changeMonth : true,
    yearRange : "2012:2014",
  }).change( function() {
    var day0 = new Date(2012, 0, 1);
    var value1 = (Date.parse($("#fromDatePicker").val()) - day0.getTime()) / MillisPerDay;
    var value2 = (Date.parse($("#toDatePicker").val()) - day0.getTime()) / MillisPerDay;
    slider.slider('values', [value1, value2]);
  });
  
  $("#fromDatePicker").datepicker({
    dateFormat : 'mm/dd/yy',
    changeYear : true,
    changeMonth : true,
    yearRange : "2012:2014",
  }).change( function() {
    var day0 = new Date(2012, 0, 1);
    var value1 = (Date.parse($("#fromDatePicker").val()) - day0.getTime()) / MillisPerDay;
    var value2 = (Date.parse($("#toDatePicker").val()) - day0.getTime()) / MillisPerDay;
    slider.slider('values', [value1, value2]);
  });
  slider = $("#date-range-selector").slider( {
    range: true,
    min: 0,
    max: 365*4+1,
    values: [ 0, 15 ],
    slide: function( event, ui ) {
      var value1 = ui.values[0];
      var value2 = ui.values[1];
      var fromDate = new Date(day0.getTime() + value1 * MillisPerDay);
      var toDate = new Date(day0.getTime() + value2 * MillisPerDay);
      $("#fromDatePicker").val(formatDate(fromDate));
      $("#toDatePicker").val(formatDate(toDate));
    }
  });
  
  // Assign event handler for the image generation button
  $("#GenerateImage").click(generateImage);
  $("#GenerateVideo").click(generateVideo);
  
  // Initialize Google Map
  var element = document.getElementById("map");

  var mapTypeIds = [];
  for (var type in google.maps.MapTypeId) {
    mapTypeIds.push(google.maps.MapTypeId[type]);
  }
  
  map = new google.maps.Map(element, {
    center : new google.maps.LatLng(39.502506, -98.356131),
    zoom : 5,
    mapTypeId : google.maps.MapTypeId.ROADMAP,
    mapTypeControlOptions : {
      mapTypeIds : mapTypeIds
    }
  });
  
  google.maps.event.addListener(map, 'click', function(event) {
    // Move the selection rectangle in the clicked location
    lat = event.latLng.lat();
    lng = event.latLng.lng();
  
    var ne = map.getBounds().getNorthEast();
    var sw = map.getBounds().getSouthWest();
    var width = (ne.lng() - sw.lng()) / 10.0;
    var height = (ne.lat() - sw.lat()) / 10.0;
    bounds = new google.maps.LatLngBounds(
      new google.maps.LatLng(lat - height, lng - width),
      new google.maps.LatLng(lat + height, lng + width));
    
    MoveRectangle(bounds);
  });
  
  // Create and initialize the draggable rectangle on the map
  CreateRectangle();
  
  // Initialize the geocoder
  geocoder = new google.maps.Geocoder();
  
  // Add a new map type which shows an overlay of heat map
  // Copied from navigation.js and disabled for now
  /*var refreshFunc = function(event) {
    $("#bdaytime").val("12-"+$("#time-travel").val()+"-2013");
    // Refresh the map
    window.map.mapTypes.set("NASA", new google.maps.ImageMapType({
      getTileUrl: function(coord, zoom) {
         var size = Math.pow(2, zoom);
         // Update x to wrap-around the earth
         var x = (coord.x % size + size) % size;
         var selectedDataset = $("#datasets").find(":selected").val();
         var selectedDate = "2013.12."+pad($("#time-travel").val(), 2);
         //return "http://tile.openstreetmap.org/" + zoom + "/" + coord.x + "/" + coord.y + ".png";
         return selectedDataset+selectedDate+"/tile_"+zoom + "_" + x + "-" + coord.y + ".png";
      },
      tileSize: new google.maps.Size(256, 256),
      name: "NASA",
      maxZoom: 9
    }));
  };
  $("#datasets").change(refreshFunc);
  $("#time-travel").change(refreshFunc);*/
});

