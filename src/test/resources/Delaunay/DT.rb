require 'rasem'

if ARGV.length == 0
  raise "Must provide input file"
end

input_file = ARGV[0]

points_file = "#{input_file}.points"
triangles_file = "#{input_file}.triangles"
unsafe_sites_file = "#{input_file}.unsafe_sites"

points = File.read(points_file).each_line.map do |line|
  line.split(",").map(&:to_f)
end

triangles = File.read(triangles_file).each_line.map do |line|
  line.split("\t").map{|part| part.split(",").map(&:to_f)}
end

unsafe_sites = File.read(unsafe_sites_file).each_line.map {|l| l.to_i == 1} if File.exists?(unsafe_sites_file)

all_xs = points.map{|p| p[0]}
all_ys = points.map{|p| p[1]}

point_size = 4

width = 100
height = 100

img = Rasem::SVGImage.new(:width=>(width+2*point_size), :height=>(height+2*point_size)) do
	@mbr = [all_xs.min, all_ys.min, all_xs.max, all_ys.max]
	@scale = 100 / [(@mbr[2] - @mbr[0]) , (@mbr[3] - @mbr[1])].max
	@point_size = 12
	def project_point(pt)
		[@point_size+(pt[0] - @mbr[0]) * @scale, @point_size+(pt[1] - @mbr[1])*@scale]
	end

	points.each_with_index do |point, index|
	  projected_point = project_point(point)
	  if unsafe_sites
		if unsafe_sites[index]
			# An unsafe site, draw as a circle
			circle projected_point[0], projected_point[1], point_size/2, :fill => :none
		else
			# A safe site, draw as a square
			rectangle projected_point[0] - point_size/2, projected_point[1] - point_size/2,
						point_size, point_size, :fill => :none
		end
	  else
		circle projected_point[0], projected_point[1], point_size / 2
	  end
	  text(projected_point[0], projected_point[1]+@point_size) { raw index.to_s }
	end

	for triangle in triangles do
	  polygon *(triangle.map{|pt| project_point(pt)}.flatten), :fill => :none
	end
end

File.open("#{input_file}.svg", "w") {|f| img.write(f)}

