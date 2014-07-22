function getJobs() {
    jQuery.ajax({
		url: 'http://web-dev.ci-connect.net/~erikhalperin/job-table/job-table.wsgi',
		dataType: 'jsonp',
		success: processData,
		error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
		});
}

function cellText(cell, text) {
    var newText = document.createTextNode(text);
    cell.appendChild(newText);
}

function test(data) {
    document.getElementById("test").innerHTML = data[0].user;
}

function processData(data) {
    var tableRef = document.getElementById('dataTable');
    var rows = tableRef.rows.length;
    var cols = tableRef.rows[0].cells.length;

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
    sorttable.makeSortable(tableRef); //make table sortable
}