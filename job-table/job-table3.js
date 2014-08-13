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
function dataHandler() {
    var iTableCounter = 1;
    var oTable, oInnerTable, detailsTableHtml;

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
		    "ajax": "http://web-dev.ci-connect.net/~erikhalperin/JobAnalysis/job-table/summary.wsgi?hours=24",
		    "bPaginate": false,
		    "aoColumns": [
	                          {
		                      "mDataProp": null,
		                      "sClass": "control center",
		                      "sDefaultContent": '<img src="http://i.imgur.com/SD7Dz.png">'
	                          },
	                          { "data": "user" },
	                          { "data": "project" },
	                          { "data": "jobs" },
	                          { "data": "walltime" },
	                          { "data": "cputime" },
	                          { "data": "efficiency" }
				],
		});

	    // Add listener event for opening and closing details
	    $(document).on('click', '#summaryTable tbody td img', function () {
		    var nTr = $(this).parents('tr')[0];
		    var nTds = this;
		    
		    if(oTable.fnIsOpen(nTr)) {
			// Close row
			this.src = "http://i.imgur.com/SD7Dz.png";
			oTable.fnClose(nTr);
		    }
		    else {
			// Open row
			var rowIndex = oTable.fnGetPosition( $(nTds).closest('tr')[0] );
			// var detailsRowData = newRowData [rowIndex].details;

			this.src = "http://i.imgur.com/d4ICC.png";
			//oTable.fnOpen(nTr, fnFormatDetails(iTableCounter, detailsTableHtml), 'details');

			oInnerTable = $("#exampleTable_" + iTableCounter).dataTable({
				"bJQueryUI": true,
				"bFilter": false,
				// data
				"bSort": true,
				"Columns": [
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