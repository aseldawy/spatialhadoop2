function multiSetting() {
	$('input:radio[name="options_partitioning"]')[0].checked = false;
	$('input:radio[name="options_partitioning"]')[1].checked = false;
	$('input:radio[name="options_partitioning"]')[2].checked = false;
	$(".options_content_column_partitioning_1").hide();
	$(".options_content_column_partitioning_1").hide();
	$(".options_content_column_partitioning_1").hide();
	
	$('input:radio[name="options_partitioning"]')[3].checked = true;
	$(".options_content_column_partitioning_2").show();
	$(".options_content_column_partitioning_2").show();
	$(".options_content_column_partitioning_2").show();
	
	$('input[name="width"]').val("256");
	$('input[name="height"]').val("256");
	
	$('input[name="min_zoom"]').prop('disabled', false);
	$('input[name="min_zoom"]').val("0");
	$('input[name="max_zoom"]').prop('disabled', false);
	$('input[name="max_zoom"]').val("6");
}

function singleSetting() {
	$('input:radio[name="options_partitioning"]')[3].checked = false;
	$('input:radio[name="options_partitioning"]')[4].checked = false;
	$('input:radio[name="options_partitioning"]')[5].checked = false;
	$(".options_content_column_partitioning_2").hide();
	$(".options_content_column_partitioning_2").hide();
	$(".options_content_column_partitioning_2").hide();
	
	$('input:radio[name="options_partitioning"]')[0].checked = true;
	$(".options_content_column_partitioning_1").show();
	$(".options_content_column_partitioning_1").show();
	$(".options_content_column_partitioning_1").show();
	
	$('input[name="width"]').val("2000");
	$('input[name="height"]').val("1000");
	
	$('input[name="min_zoom"]').prop('disabled', true);
	$('input[name="min_zoom"]').val("");
	$('input[name="max_zoom"]').prop('disabled', true);
	$('input[name="max_zoom"]').val("");
}

function hdfSetting() {
	$('input[name="smooth"]').prop('disabled', false);
}

function regularSetting() {
	$('input[name="smooth"]').prop('checked', false);
	$('input[name="smooth"]').prop('disabled', true);
}

function visualize() {
	$(".job_id").empty();
	var plotType = $('input[name="options_plot_type"]:checked').val();
	var vizType = $('input[name="options_type"]:checked').val();
	var dataset = $('input[name="dataset"]:checked').val();
	var noMerge = $('input[name="merge"]:checked').length > 0;

	if(!dataset || !vizType || !plotType) {
		alert("Please select a dataset, visualization type, and plot type.");
		return;
	}
	var width = $('input[name="width"]').val();
	var height = $('input[name="height"]').val();
	
	var partition = $('input[name="options_partitioning"]:checked').val();
	var requestURL = "cgi-bin/visualize.cgi?"
		+ "dataset=" + dataset + "&width=" + width
		+ "&height=" + height + "&partition=" + partition
		+ "&viztype=" + vizType + "&plottype=" + plotType;
	
	if(vizType == "multi_level") {
		var min_zoom = $('input[name="min_zoom"]').val();
		var max_zoom = $('input[name="max_zoom"]').val();
		if(min_zoom && max_zoom) {
			if(max_zoom < min_zoom) {
				alert("max_zoom cannot be lower than min_zoom");
				return;
			}
		}
		if(!min_zoom) {
			min_zoom = "null";
		}
		if(!max_zoom) {
			max_zoom = "null";
		}
		requestURL += "&min_zoom=" + min_zoom + "&max_zoom=" + max_zoom;
	}
	if(noMerge) {
		requestURL += "&no-merge=true";
	} else {
		requestURL += "&no-merge=false";
	}
	if(plotType == "hdfplot") {
		var smooth = $('input[name="smooth"]:checked').length > 0;
		if(smooth) {
			requestURL += "&recover=read";
		} else {
			requestURL += "&recover=none";
		}
	}
	jQuery.ajax(requestURL, {success: function(response) {
		document.getElementById("job_id").innerHTML = "&nbsp Hadoop Job ID: " + response.job + "<br> " +
				"&nbsp Output Path: " + response.output+ "<br>" +
				"&nbsp Job URL: <a target=\"_blank\" href=\"" + response.url + "\">" + response.url + "</a>";
	}});
}
 
$(function () {
	var requestURL = "cgi-bin/generate_dataset.cgi";
	jQuery.ajax(requestURL, {success: function(response) {
		var index;
		var text = "";
		for	(index = 0; index < response.length; index++) {
			text += "<input type=radio name=dataset value=" + response[index] + ">" + response[index] + "<br>";
		}
	    document.getElementById("dataset").innerHTML = text;
	}});
})