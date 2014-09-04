function splitquery(url) {
	var offset = url.indexOf('?');
	if (offset == -1)
		return [url, '']
	left = url.substr(0, offset);
	right = url.substr(offset);
	return [left, right];
}

function splitpath(path) {
	var offset = path.lastIndexOf('/');
	if (offset == -1)
		return [path, ''];
	return [path.substr(0, offset+1), path.substr(offset)];
}


var tmp = splitquery('' + window.location.href);
var homeURL = tmp[0] + '?';
var tmp = splitpath(homeURL);
var baseURL = tmp[0];

//Spinner handler
$(function() {
	// Prevent spinner from showing negative numbers
        var spinner = $("#spinner").spinner({
		spin: function(event, ui) {
		    if(ui.value < 0) {
			$(this).spinner("value", 0);
			return false;
		    }
		}
	    });

	var title = getParameterByName('title');
	// If title parameter is specified, keep it
	if (title != null)
	    title = 'title=' + title + '&'

	// Go button
        $("#go").click(function() {
		window.location.href = homeURL + title + 'hours=' + spinner.spinner("value");
	});
});

// Gets parameters from url address
function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)");
    var results = regex.exec(location.search);
    if (results == null)
        return null;
    return decodeURIComponent(results[1].replace(/\+/g, " "));
}

// Taken from http://stackoverflow.com/questions/11841486/datatables-drill-down-rows-with-nested-independent-table
function fnFormatDetails(tableId, html) {
    var sOut = "<table id=\"summaryTable_" + tableId + "\">";
    sOut += html;
    sOut += "</table>";
    return sOut;
}

// Draws table
function dataHandler() {
    var iTableCounter = 1;
    var oTable, oInnerTable, detailsTableHtml;

    var hours = getParameterByName('hours') || "";
    var site = getParameterByName('site') || "";
    var summaryUrl = baseURL + "summary.wsgi?hours=" + hours + '&site=' + site;

    // Limit to a single search key?
    var autosearch = getParameterByName('search');

    $(document).ready(function() {
	    detailsTableHtml = $('#childTable').html();

	    // Add +/- column to table
	    var nCloneTh = document.createElement('th');
	    var nCloneTd = document.createElement('td');
	    nCloneTd.innerHTML = '<img src="http://i.imgur.com/SD7Dz.png">';
	    nCloneTh.className = 'center';

	    $('#summaryTable thead tr').each(function () {
		    this.insertBefore(nCloneTh, this.childNodes[0]);
		});

	    $('#summaryTable tbody tr').each(function () {
		    this.insertBefore(nCloneTd.cloneNode(true), this.childNodes[0]);
		});

	    // Initialize DataTables for the 'details' table
	    var oTable = $('#summaryTable').dataTable({
		    "bJQueryUI": true,
		    "ajax": summaryUrl,
		    "bPaginate": false,
		    "aoColumns": [
	                          {
		                      "mDataProp": null,
		                      "sClass": "control center",
		                      "sDefaultContent": '<img src="http://i.imgur.com/SD7Dz.png">',
				      "bSortable": false
	                          },
	                          { "data": "user" },
	                          { "data": "project" },
	                          { "data": "jobs" },
	                          { "data": "percentcompleted" },
	                          { "data": "walltime" },
	                          { "data": "cputime" },
	                          { "data": "efficiency" },
	                          { "data": "username", "visible": false }, // In order to submit username to wsgi in child
				 ],
		    "footerCallback": function ( row, data, start, end, display ) {
			var api = this.api(), data;
			
			for(var i=3;i<7;i++) {
			    if(i == 4)
				continue;
			    // Total jobs, wall time, and cpu time columns
			    data = api.column(i).data();
			    total = data.length ?
				data.reduce( function (a, b) {
				    return parseFloat(a) + parseFloat(b);
				}) :
				0;
			    // Update footer
			    $(api.column(i).footer()).html(Math.round(total));
			}
		    }
		});
	    
	    // Add listener event for opening and closing details
	    $(document).on('click', '#summaryTable tbody td img', function () {
		    var nTr = $(this).parents('tr')[0];
		    var nTds = this;
		    var username = oTable.fnGetData(nTr, 8);
		    var urlAddr = baseURL + 'single-user.wsgi?hours=' + hours + ';user=' + username;

		    if(oTable.fnIsOpen(nTr)) {
			// Close row
			this.src = "http://i.imgur.com/SD7Dz.png";
			oTable.fnClose(nTr);
		    }
		    else {
			// Open row
			var rowIndex = oTable.fnGetPosition( $(nTds).closest('tr')[0] );
			
			this.src = "http://i.imgur.com/d4ICC.png";
			oTable.fnOpen(nTr, fnFormatDetails(iTableCounter, detailsTableHtml), 'details');

			oInnerTable = $("#summaryTable_" + iTableCounter).dataTable({
				"bJQueryUI": true,
				"bFilter": false,
				"ajax": urlAddr,
				"aoColumns": [
			                      { "data": "site" },
			                      { "data": "jobs" },
			                      { "data": "percentcompleted" },
		              	              { "data": "walltime" },
			                      { "data": "cputime" },
			                      { "data": "efficiency" }
					     ],
				"bPaginate": false
			    });
			iTableCounter = iTableCounter + 1;
		    } 
		});

        // populate the search
        if (autosearch) {
            $('#summaryTable_filter label input').val(autosearch);
            $('#summaryTable_filter label input').keyup();
         }
	});
}


// Change title and header if title is specified in url
var title = getParameterByName('title');
if (title != null) {
    $('title').text(title);
    $('#pagetitle').text(title);
}

// Hide table header?
var decor = getParameterByName('decoration');
if (decor == 'plain') {
    $('.table_header').hide();
}
