function getJobs() {
    var urlAddress = ''

    jQuery.ajax({
	    url: urlAddress,
                dataType: 'jsonp',
                success: processData,
                error: function(jqXHR, textStatus, errort){ console.log(textStatus, errort); console.log(jqXHR); }
	});
}

var map = new Datamap({
	element: document.getElementById('container'),
	scope: 'usa',
	geographyConfig: {
	    popupOnHover: false,
	    highlightOnHover: false
	},

	fills: {
	    defaultFill: 'green',
	    bubble: 'blue',
	}
    });

function processData(data) {
	 
    function addradius(record) {
	record.radius = 5 * Math.log(record.walltime) / Math.log(10);
	return record;
    }
	 
    function addFillKey(record) {
	record.fillKey = 'bubble';
	return record;
    }
    
    for (record in data) {
	addradius(data[record]);
	addFillKey(data[record]);
    }
    
    map.bubbles(data, {
	    popupTemplate: function (geo, data) { 
		return ['<div class="hoverinfo">'+
			'Job Location: ' +  data.site,
			'<br/>CPU Time: ' +  data.walltime + ' hours',
			'</div>'].join('');
	    }    
	})
}