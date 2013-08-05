<%@ page
  contentType="text/html; charset=UTF-8"
%>

<%! private static final long serialVersionUID = 1L;
%>

<html>
<head>
<%= "zzzzzzzzz" %>
<style type="text/css">
.toolbar div { float: left; width: 32px; height: 32px; border: solid 1px black; }
.operations div { float: left; height: 32px; border: solid 1px black; }
#dialog {
  position: absolute;
  left: 300px;
  top: 150px;
  z-index: 100;
  background-color: lightgrey;
  border: solid 2px black;
  width: 350px;
  height: 260px;}
#dialog .title {
  width: 100%;
  height: 32px;
  background-color: lightblue;
  text-align: center;
  border-bottom: solid 1px black;
}
#dialog .label {
  width: 120px;
  float: left;
  text-align: center;
}
</style>
<title>Spatial Hadoop Visualizer</title>
</head>
<body>
</body>
<div id="filenames" style="float: left;">
  <img width="100" src="/static/shadoop_logo.png"/>
  <div class="toolbar" style="width: 100%;">
    <div style="background: url('/static/add.png')" title="Add file"></div>
    <div style="background: url('/static/delete.png')" title="Delete file"></div>
  </div>
  <select name="filename" multiple="multiple" style="height: 300px;">
    <option selected="selected">area_water</option>
    <option>linear_water</option>
    <option selected="selected">road_edges</option>
    <option>parks</option>
    <option>buildings</option>
    <option>pois</option>
 </select>
</div>
<div id="image" style="float: left;">
  <image height="80"/>
  <div class="operations" style="width: 100%;">
    <input type="button" value="Range Query"></input>
    <input type="button" value="kNN"></input>
    <input type="button" value="Spatial Join"></input>
  </div>
  <img src="/static/roads_rivers.png"/>
</div>
<div id="dialog">
  <div class="title">Spatial Join</div>
  <form>
    <span class="label"><label for="output">Output filename</label></span>
    <input name="output" type="text" value="/roads_rivers"/><br/>
    <span class="label"><label for="predicate">Join predicate</label></span>
    <select name="predicate">
      <option selected="selected">overlap</option>
      <option>touches</option>
      <option>contains</option>
    </select>
    <textarea cols="40" rows="8">
area_water = LOAD 'area_water' AS (id: int, area: polygon);
road_edges = LOAD 'road_edges' AS (id: int, edge: line);
result     = JOIN area_water BY area
  road_edges BY edge PREDICATE = overlap;
STORE result INTO '/roads_rivers';
    </textarea>
    <input type="submit" value="Submit"/><input type="reset" value="Cancel"/>
  </form>
</div>
</html>