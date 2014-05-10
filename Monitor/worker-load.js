var nJobs = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
var memUse = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
var cpuDataSets = [new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()];
var memDataSets = [new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()];
var seriesOptions = [
  { strokeStyle: 'rgba(255, 0, 0, 1)', fillStyle: 'rgba(255, 0, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 255, 0, 1)', fillStyle: 'rgba(0, 255, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 0, 255, 1)', fillStyle: 'rgba(0, 0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 255, 0, 1)', fillStyle: 'rgba(255, 255, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 0, 255, 1)', fillStyle: 'rgba(255, 0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 255, 255, 1)', fillStyle: 'rgba(0, 255, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 255, 255, 1)', fillStyle: 'rgba(255, 255, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 0, 0, 1)', fillStyle: 'rgba(128, 0, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 128, 0, 1)', fillStyle: 'rgba(0, 128, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 0, 128, 1)', fillStyle: 'rgba(0, 0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 128, 0, 1)', fillStyle: 'rgba(128, 128, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 0, 128, 1)', fillStyle: 'rgba(128, 0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 128, 128, 1)', fillStyle: 'rgba(0, 128, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 128, 128, 1)', fillStyle: 'rgba(128, 128, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 255, 0, 1)', fillStyle: 'rgba(128, 255, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 128, 0, 1)', fillStyle: 'rgba(255, 128, 0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 0, 255, 1)', fillStyle: 'rgba(128, 0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 0, 128, 1)', fillStyle: 'rgba(255, 0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 128, 255, 1)', fillStyle: 'rgba(0, 128, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(0, 255, 128, 1)', fillStyle: 'rgba(0, 255, 128, 0.1)', lineWidth: 3 }
];

setInterval(refreshTimeline, 1000);
setInterval(function() {
  for (var i = 0; i < cpuDataSets.length; i++) {
    cpuDataSets[i].append(new Date().getTime(), nJobs[i]);
    memDataSets[i].append(new Date().getTime(), memUse[i]);
  }
}, 1000);

function createTimeline() {
  var cpuChart = new SmoothieChart({ millisPerPixel: 100, grid: { strokeStyle: '#555555', lineWidth: 1, millisPerLine: 5000, verticalSections: 4 }, timestampFormatter:SmoothieChart.timeFormatter});
  var memChart = new SmoothieChart({ millisPerPixel: 100, grid: { strokeStyle: '#555555', lineWidth: 1, millisPerLine: 5000, verticalSections: 4 }, timestampFormatter:SmoothieChart.timeFormatter});
  for (var i = 0; i < cpuDataSets.length; i++) {
    cpuChart.addTimeSeries(cpuDataSets[i], seriesOptions[i]);
    memChart.addTimeSeries(memDataSets[i], seriesOptions[i]);
  }
  cpuChart.streamTo(document.getElementById("cpuchart"), 0);
  memChart.streamTo(document.getElementById("memchart"), 0);
}

function refreshTimeline() {
  if ("WebSocket" in window) {
    //alert("WebSocket is supported by your Browser!");
    // Let us open a web socket
    var ws = new WebSocket("ws://vision24.csail.mit.edu:12345/chart");
    ws.onopen = function() {
       // Web Socket is connected, send data using send()
       ws.send("Message to send");
    };
    ws.onmessage = function (evt) { 
      var received_msg = evt.data;
      //alert("Message is received..." + received_msg);
      var reader = new FileReader();
      reader.onload = function() {
        //alert("blob:" + reader.result);
        var myArray = reader.result.split(" ");
        for (var i = 0; i < 40; i += 2) {
          nJobs[i/2] = parseInt(myArray[i], 10);
          memUse[i/2] = parseInt(myArray[i+1], 10);
        }
      };
      reader.readAsText(received_msg);
    };
    ws.onclose = function() { 
      // websocket is closed.
      //alert("Connection is closed..."); 
    };
  }
  else {
    // The browser doesn't support WebSocket
    alert("WebSocket NOT supported by your Browser!");
  }
}

