function getJobs() {
    jQuery.ajax({
                url: 'http://web-dev.ci-connect.net/~erikhalperin/job-table/job-table.wsgi?hours=48000',
                dataType: 'jsonp',
                success: processData,
                error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
                });
}

function cellText(cell, text) {
    var newText = document.createTextNode(text);
    cell.appendChild(newText);
}

function rowText(row, user, project, site, jobs, walltime, cputime, efficiency) {
    //change color -- row.style.backgroundcolor = '#123456';
    cellText(row.insertCell(0), user);
    cellText(row.insertCell(1), project);
    cellText(row.insertCell(2), site);
    cellText(row.insertCell(3), jobs);
    cellText(row.insertCell(4), walltime);
    cellText(row.insertCell(5), cputime);
    cellText(row.insertCell(6), efficiency);
}

function getName(user) {
    var len = user.length;
}

function processData(data) {
    //document.getElementById('test').innerHTML = Object.keys(data);

    var tableRef = document.getElementById('dataTable');
    var rows = tableRef.rows.length;
    var cols = tableRef.rows[0].cells.length;

    var users = Object.keys(data[0]);

    //testing
    //var ds = users[0]
    //document.getElementById('test').innerHTML = data[0][ds];

    for(var u = 0; u < users.length; u++) {
        var user = users[u];
        var projects = Object.keys(data[0][user]);

        for(var p = 0; p < projects.length; p++) {
            var project = projects[p];
            var sites = Object.keys(data[0][user][project]);

            for(var s = 0; s < sites.length; s++) {
                var newRow = tableRef.insertRow(-1);
                var site = sites[s];
                var data2 = data[0][user][project][site];

                if(s == 0) {
                    //fill user cell and project cell
                    if(p == 0)
                        rowText(newRow, user, project, site, data2.jobs, data2.walltime, data2.cputime, data2.efficiency);
                    //fill only project cell
                    else
                        rowText(newRow, "", project, site, data2.jobs, data2.walltime, data2.cputime, data2.efficiency);
                }
                else {
                    //don't fill user or project cell
                    rowText(newRow, "", "", sites[s], data2.jobs, data2.walltime, data2.cputime, data2.efficiency);
                }
            }
        }
    }
    /*
    for(var r = 1; r < data.length + rows; r++) {
        var newRow = tableRef.insertRow(r);
        //change color -- newRow.style.backgroundColor = '#dope';
        for(var c = 0; c < cols; c++) {
            var newCell = newRow.insertCell(c);
            //var link = data[r-1].url; TODO
            switch(c) {
            case 0:
                var user = data[r-1].user;
                cellText(newCell, user);
                break;
            case 1:
                var proj = data[r-1].user;
                cellText(newCell, user);
                break;
            case 2:
                var site = data[r-1].user;
                cellText(newCell, user);
                break;
            case 3:
                var numJobs = data[r-1].jobs;
                cellText(newCell, numJobs);
                break;
            case 4:
                var wallTime = data[r-1].walltime;
                cellText(newCell, wallTime);
                break;
            case 5:
                var cpuTime = data[r-1].cputime;
                cellText(newCell, cpuTime);
                break;
            case 6:
                var efficiency = data[r-1].efficiency;
                cellText(newCell, efficiency);
                break;
            }
        }
    }
    */
    //sorttable.makeSortable(tableRef); //make table sortable
}
