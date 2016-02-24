function display() {
	var outputPath = $('input[name="output"]:checked').val();
	var requestURL = "cgi-bin/fetch_result.cgi?path=" + outputPath;
	var requestInfoURL = "cgi-bin/get_output_info.cgi?path=" + outputPath;
	jQuery.ajax(requestInfoURL, {success: function(response) {
		document.getElementById("statistics").innerHTML = "<h3 class=\"dataset_title\"> Statistics </h3> <b>Input Size: </b>" 
			+ response.inputSize + ".<br>" +
		"<b>Intermediate Size: </b>" + response.intermediateSize + ".<br>" +
		"<b>Intermediate Groups: </b>" + response.intermediateGroup+ ".<br>" +
		"<b>Job ID: </b>" + response.jobID+ ".<br>" +
		"<b>Job URL: </b><a href='"+response.jobURL+"'>" + response.jobURL+ "</a>.<br>" +
		;
	}});
	jQuery.ajax(requestURL, {success: function(response) {
		document.getElementById("result").innerHTML = response;
	}});
}

function refresh() {
	var requestURL = "cgi-bin/generate_output_list.cgi";
	jQuery.ajax(requestURL, {success: function(response) {
		var index;
		var text = "";
		for	(index = 0; index < response.length; index++) {
			text += "<input type=radio name=output value=" + response[index] + ">" + response[index] + "<br>";
		}
	    document.getElementById("output").innerHTML = text;
	}});
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