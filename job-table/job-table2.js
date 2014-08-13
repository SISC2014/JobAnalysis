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
                success: totalSummary,
                error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
	});
}

function cellText(cell, text) {
    var newText = document.createTextNode(text);
    cell.appendChild(newText);
    
    if(text == "") {
	//cell.setAttribute("rowspan", 1);
	//cell.className += " collapsible";
    }
}

function rowText(row, user, project, site, jobs, walltime, cputime, efficiency) {
    //change color -- row.style.backgroundcolor = '#123456';
    cellText(row.insertCell(0), "");
    cellText(row.insertCell(1), user);
    cellText(row.insertCell(2), project);
    cellText(row.insertCell(3), site);
    cellText(row.insertCell(4), jobs);
    cellText(row.insertCell(5), walltime);
    cellText(row.insertCell(6), cputime);
    cellText(row.insertCell(7), efficiency);
}

function totalSummary(data) {
    dataGlobal = data;
 
    var tableRef = document.getElementById('dataTable');
    // tbodyRef - for inserting rows into tbody instead of the table itself
    var tbodyRef = tableRef.getElementsByTagName('tbody')[0];
    var rows = tableRef.rows.length;
    var cols = tableRef.rows[0].cells.length;

    var users = Object.keys(data[0]);
    for(var u = 0; u < users.length; u++) {
	var user = users[u];
	var projects = Object.keys(data[0][user]);

	for(var p = 0; p < projects.length; p++) {
	    var project = projects[p];

	    //ignore this for now
	    if(project == "User Total")
		continue;

	    var projectData = data[0][user][project]["Project Total"];
	    var newRow = tbodyRef.insertRow(-1);
	    rowText(newRow, user, project, "Project Total", projectData.jobs, projectData.walltime, projectData.cputime, projectData.efficiency);

	    //Don't do anything with individual sites (yet) because this is executive summary
	}
    }
    var newRow = tbodyRef.insertRow(-1);
    rowText(newRow, 'u', 'p', 's', 'j', 'w', 'c', 'e');
    newRow.className += " expand-child";

    $(document).ready(function() 
		      { 
			  $(tableRef)
			      //.collapsible("td.collapsible", {
			      //      collapse: true
			      //	  })
			      .tablesorter({
				      // Don't sort by first column
				      headers: {0: {sorter: false}}
				  });
		      });
}

function userTable() {
    // First get users from text box
    var users = document.getElementById('users-input').value;
    users = users.split(",");

    if(users == 'all')
	users = Object.keys(dataGlobal[0]);
    
    var tableRef = document.getElementById('dataTable');
    var tbodyRef = tableRef.getElementsByTagName('tbody')[0];
    var rows = tableRef.rows.length;
    var cols = tableRef.rows[0].cells.length;

    // Remove all rows in tbody
    $(tbodyRef).empty();

    // Construct table
    for(var u = 0; u < users.length; u++) {
	var user = users[u].trim();

	var projects = Object.keys(dataGlobal[0][user]);
	
	for(var p = 0; p < projects.length; p++) {
	    var project = projects[p];

	    //ignore this for now
            if(project == "User Total")
                continue;

	    // Build project total row first
            var projectData = dataGlobal[0][user][project]["Project Total"];
            var newRow = tbodyRef.insertRow(-1);
            rowText(newRow, user, project, "Project Total", projectData.jobs, projectData.walltime, projectData.cputime, projectData.efficiency);

	    var sites = Object.keys(dataGlobal[0][user][project]);

	    for(var s = 0; s < sites.length; s++) {
		site = sites[s];
		if(site == "Project Total")
		    continue;

		var siteData = dataGlobal[0][user][project][site];
		newRow = tbodyRef.insertRow(-1);
		rowText(newRow, " ", " ", site, siteData.jobs, siteData.walltime, siteData.cputime, siteData.efficiency);
	    }
	}
    }
    sorttable.makeSortable(tableRef);
}