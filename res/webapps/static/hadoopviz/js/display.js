function display() {
	var outputPath = $('input[name="output"]:checked').val();
	var requestURL = "cgi-bin/fetch_result.cgi?path=" + outputPath;
	jQuery.ajax(requestURL, {success: function(response) {
		document.getElementById("result").innerHTML = response;
	}})
}

$(function () {
	var requestURL = "cgi-bin/generate_output_list.cgi";
	jQuery.ajax(requestURL, {success: function(response) {
		var index;
		var text = "";
		for	(index = 0; index < response.length; index++) {
			text += "<input type=radio name=output value=" + response[index] + ">" + response[index] + "<br>";
		}
	    document.getElementById("output").innerHTML = text;
	}});
})