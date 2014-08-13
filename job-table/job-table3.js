//Displays loading gif anytime ajax event occurs
$body = $("body");

$(document).on({
        ajaxStart: function() { $body.addClass("loading");    },
	    ajaxStop: function() { $body.removeClass("loading"); }
    });

/* The following is javascript for getting data from a wsgi and displaying it correctly */

// Gets parameters from url address
function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

// Retrieve data from url and execute dataHandler on success
function getJobs() {
    var hours = getParameterByName('hours');

    // Default is 48 hours
    if(hours == "")
        hours = 48;

    var urlAddress = 'http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/job-table.wsgi?hours='.concat(hours);

    jQuery.ajax({
            url: urlAddress,
                dataType: 'jsonp',
                success: dataHandler,
                error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
        });
}

function dataHandler() {
    $(document).ready(function() {
	    $('#myTable').DataTable({
		    "ajax": "http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/job-table.wsgi?hours=12",
			"columns": [
				    { "data": "user" },
				    { "data": "projects.0.project" },
				    { "data": "projects.0.sites.0.site" },
				    { "data": "projects.0.sites.0.jobs" },
				    { "data": "projects.0.sites.0.walltime" },
				    { "data": "projects.0.sites.0.cputime" },
				    { "data": "projects.0.sites.0.efficiency" }
				    ]
			});
	});
}
