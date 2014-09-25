$(document).ready(function() {
  $("#bdaytime").val("12-"+$("#time-travel").val()+"-2013");
  var refreshFunc = function(event) {
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
  $("#time-travel").change(refreshFunc);
});
