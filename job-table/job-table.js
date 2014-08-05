//Displays loading gif anytime ajax event occurs
$body = $("body");

$(document).on({
        ajaxStart: function() { $body.addClass("loading");    },
            ajaxStop: function() { $body.removeClass("loading"); }
    });

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results == null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function getJobs() {
    var hours = getParameterByName('hours');
    var users = getParameterByName('users');
    var urlAddress = 'http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/job-table.wsgi?hours='.concat(hours).concat(';users=').concat(users);
    //document.getElementById('test').innerHTML = urlAddress;

    jQuery.ajax({
                url: urlAddress,
                dataType: 'jsonp',
                success: executiveSummary,
                error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
                });
}

function cellText(cell, text) {
    var newText = document.createTextNode(text);
    cell.appendChild(newText);
}

function rowText(row, user, project, site, jobs, walltime, cputime, efficiency) {
    //change color -- row.style.backgroundcolor = '#123456';
    cellText(row.insertCell(0), user, 0);
    cellText(row.insertCell(1), project, 1);
    cellText(row.insertCell(2), site, 2);
    cellText(row.insertCell(3), jobs, 3);
    cellText(row.insertCell(4), walltime, 4);
    cellText(row.insertCell(5), cputime, 5);
    cellText(row.insertCell(6), efficiency, 6);
}

function executiveSummary(data) {
    var tableRef = document.getElementById('dataTable');
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
	    var newRow = tableRef.insertRow(-1);
	    rowText(newRow, user, project, "Project Total", projectData.jobs, projectData.walltime, projectData.cputime, projectData.efficiency);

	    //Don't do anything with individual sites (yet) because this is executive summary
	}
    }
    sorttable.makeSortable(tableRef); //make table sortable
}

/*
function scope(scopeData) {
    var user, project, site;
    var totJobs, curJobs;
    var totWall, curWall;
    var totCpu, curCpu;
    var totEff, curEff;

    function cellText(cell, text, type) {
        var newText = document.createTextNode(text);
        cell.appendChild(newText);
        var key;
        var key2;

        switch(type) {
        case 0: // User
            key = user;
            if(text == ' ')
                key2 = 'b';
            else
                key2 = 'a';

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 1: // Project
            key = project;
            if(text == ' ')
                key2 = 'b';
            else
                key2 = 'a';

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 2: // Site
            key = user;
            if(text == 'Project Total')
                key2 = ' ';
            else
                key2 = site;

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 3: // Number of Jobs
            key = totJobs+1;
            key2 = curJobs;

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 4: // Wall Time
            key = totWall;
            key2 = curWall;

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 5: // CPU Time
            key = totCpu;
            key2 = curCpu;

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        case 6: // CPU Efficiency
            key = totEff;
            if(site == 'Project Total')
                key2 = 200;
            else
                key2 = curEff;

            cell.setAttribute('sorttable_customkey', key);
            cell.setAttribute('sorttable_customkey2', key2);
            break;
        }
    }

    function rowText(row, localUser, project, site, jobs, walltime, cputime, efficiency) {
        //change color -- row.style.backgroundcolor = '#123456';
        cellText(row.insertCell(0), localUser, 0);
        cellText(row.insertCell(1), project, 1);
        cellText(row.insertCell(2), site, 2);
        cellText(row.insertCell(3), jobs, 3);
        cellText(row.insertCell(4), walltime, 4);
        cellText(row.insertCell(5), cputime, 5);
        cellText(row.insertCell(6), efficiency, 6);
    }

    function processData(data) {
        var tableRef = document.getElementById('dataTable');
        var rows = tableRef.rows.length;
        var cols = tableRef.rows[0].cells.length;

        var users = Object.keys(data[0]);

        for(var u = 0; u < users.length; u++) {
            user = users[u];
            var projects = Object.keys(data[0][user]);

            // add try statement for user total

            for(var p = 0; p < projects.length; p++) {
                project = projects[p];
                var sites = Object.keys(data[0][user][project]);

                // Build project total row first
                var projectData = data[0][user][project]["Project Total"];
                totJobs = curJobs = projectData.jobs;
                totWall = parseFloat(projectData.walltime);
                totCpu = parseFloat(projectData.cputime);
                totEff = parseFloat(projectData.efficiency);

                var newRow = tableRef.insertRow(-1);
                rowText(newRow, user, project, "Project Total", totJobs, totWall, totCpu, totEff);

                for(var s = 0; s < sites.length; s++) {
                    site = sites[s];
                    if(site == "Project Total")
                        continue;

                    var siteData = data[0][user][project][site];
                    curJobs = siteData.jobs;
                    curWall = parseFloat(siteData.walltime);
		    curCpu = parseFloat(siteData.cputime);
		    curEff = parseFloat(siteData.efficiency);

                    newRow = tableRef.insertRow(-1);
                    rowText(newRow, " ", " ", site, curJobs, curWall, curCpu, curEff);
                }
            }
        }
        sorttable.makeSortable(tableRef); //make table sortable
    }
    processData(scopeData);
}
*/