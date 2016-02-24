if(!d3.chart) d3.chart = {};

d3.chart.brush = function() {
  var g;
  var data;
  var width = 600;
  var height = 30;
  var dispatch = d3.dispatch(chart, "filter");

  function chart(container) {
    g = container;
    
    var extent = d3.extent(data, function(d) {
      return d.data;
    })

    var scale = d3.time.scale()
      .domain(extent)
      .range([0, width])

    var brush = d3.svg.brush()
    brush.x(scale)
    brush(g)
    g.selectAll("rect").attr("height", height)
    g.selectAll(".background")
      .style({fill: "#4B9E9E", visibility: "visible"})
    g.selectAll(".extent")
      .style({fill: "#78C5C5", visibility: "visible"})
    g.selectAll(".resize rect")
      .style({fill: "#276C86", visibility: "visible"})

    var rects = g.selectAll("rect.events")
    .data(data)
    rects.enter()
    .append("rect").classed("events", true)
    rects.attr({
      x: function(d) { return scale(d.data.created);},
      y: 0,
      width: 1,
      height: height
    }).style("pointer-events", "none")

    rects.exit().remove()

    brush.on("brushend", function() {
      var ext = brush.extent()
      var filtered = data.filter(function(d) {
        return (d.data.created > ext[0] && d.data.created < ext[1])
      })
      g.selectAll("rect.events")
      .style("stroke", "")
      
      g.selectAll("rect.events")
      .data(filtered, function(d) { return d.data.id })
      .style({
        stroke: "#fff"
      })

      //emit filtered data
      dispatch.filter(filtered)
    })

    var axis = d3.svg.axis()
    .scale(scale)
    .orient("bottom")
    .tickValues([new Date(extent[0]), new Date(extent[0] + (extent[1] - extent[0])/2) , new Date(extent[1])])
    .tickFormat(d3.time.format("%x %H:%M"))

    var agroup = g.append("g")
    agroup.attr("transform", "translate(" + [0, height] + ")")
    axis(agroup)
    agroup.selectAll("path")
      .style({ fill: "none", stroke: "#000"})
    agroup.selectAll("line")
      .style({ stroke: "#000"})
  }

  chart.highlight = function(data) {
    var rects = g.selectAll("rect.events")
    .style("stroke", "")
    .style("stroke-width", "")

    rects.data(data, function(d) { return d.data.id })
    .style("stroke", "orange")
    .style("stroke-width", 3)
  }

  chart.data = function(value) {
    if(!arguments.length) return data;
    data = value;
    return chart;
  }
  chart.width = function(value) {
    if(!arguments.length) return width;
    width = value;
    return chart;
  }
  chart.height = function(value) {
    if(!arguments.length) return height;
    height = value;
    return chart;
  }

  return d3.rebind(chart, dispatch, "on");
}