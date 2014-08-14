//Displays loading gif anytime ajax event occurs
/*$body = $("body");

$(document).on({
        ajaxStart: function() { $body.addClass("loading");    },
	    ajaxStop: function() { $body.removeClass("loading"); }
	    }); */

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

// Taken from http://stackoverflow.com/questions/11841486/datatables-drill-down-rows-with-nested-independent-table
function fnFormatDetails(tableId, html) {
    var sOut = "<table id=\"summaryTable_" + tableId + "\">";
    sOut += html;
    sOut += "</table>";
    return sOut;
}

function dataHandler() {
    var iTableCounter = 1;
    var oTable, oInnerTable, detailsTableHtml;

    var hours = getParameterByName('hours');
    var summaryUrl = "http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/summary.wsgi?hours=" + hours;

    // Change title and header if title is specified in url
    var title = getParameterByName('title');
    if(title != "") {
	$('title').text(title);
	$('#headId').text(title);
    }

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

	    // Initialize DataTables with no sorting on the 'details' column (yet)
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
	                          { "data": "walltime" },
	                          { "data": "cputime" },
	                          { "data": "efficiency" },
	                          { "data": "username", "visible": false } // In order to submit username to wsgi in child
				 ]
		});

	    // Add listener event for opening and closing details
	    $(document).on('click', '#summaryTable tbody td img', function () {
		    var nTr = $(this).parents('tr')[0];
		    var nTds = this;
		    var username = oTable.fnGetData(nTr, 7);
		    var urlAddr = 'http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/single-user.wsgi?hours=' + hours + ';user=' + username;

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
		              	              { "data": "walltime" },
			                      { "data": "cputime" },
			                      { "data": "efficiency" }
					     ],
				"bPaginate": false
			    });
			iTableCounter = iTableCounter + 1;
		    } 
		    });
	});
}