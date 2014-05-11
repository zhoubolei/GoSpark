var nJobs = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
var memUse = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
var cpuDataSets = [new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()];
var memDataSets = [new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries(), new TimeSeries()];
var seriesOptions = [
  { strokeStyle: 'rgba(255,   0,   0, 1)', fillStyle: 'rgba(255,   0,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 255,   0, 1)', fillStyle: 'rgba(  0, 255,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0,   0, 255, 1)', fillStyle: 'rgba(  0,   0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 255,   0, 1)', fillStyle: 'rgba(255, 255,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255,   0, 255, 1)', fillStyle: 'rgba(255,   0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 255, 255, 1)', fillStyle: 'rgba(  0, 255, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 255, 255, 1)', fillStyle: 'rgba(255, 255, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128,   0,   0, 1)', fillStyle: 'rgba(128,   0,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 128,   0, 1)', fillStyle: 'rgba(  0, 128,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0,   0, 128, 1)', fillStyle: 'rgba(  0,   0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 128,   0, 1)', fillStyle: 'rgba(128, 128,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128,   0, 128, 1)', fillStyle: 'rgba(128,   0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 128, 128, 1)', fillStyle: 'rgba(  0, 128, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 128, 128, 1)', fillStyle: 'rgba(128, 128, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128, 255,   0, 1)', fillStyle: 'rgba(128, 255,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255, 128,   0, 1)', fillStyle: 'rgba(255, 128,   0, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(128,   0, 255, 1)', fillStyle: 'rgba(128,   0, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(255,   0, 128, 1)', fillStyle: 'rgba(255,   0, 128, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 128, 255, 1)', fillStyle: 'rgba(  0, 128, 255, 0.1)', lineWidth: 3 },
  { strokeStyle: 'rgba(  0, 255, 128, 1)', fillStyle: 'rgba(  0, 255, 128, 0.1)', lineWidth: 3 }
];

setInterval(refreshTimeline, 1000);
setInterval(refreshWorkerNames, 5000);

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
       ws.send("stats");
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

function refreshWorkerNames() {
  if ("WebSocket" in window) {
    //alert("WebSocket is supported by your Browser!");
    // Let us open a web socket
    var ws = new WebSocket("ws://vision24.csail.mit.edu:12345/chart");
    ws.onopen = function() {
       // Web Socket is connected, send data using send()
       ws.send("names");
    };
    ws.onmessage = function (evt) { 
      var received_msg = evt.data;
      //alert("Message is received..." + received_msg);
      var reader = new FileReader();
      reader.onload = function() {
        //alert("blob:" + reader.result);
        var myArray = reader.result.split(",");
        document.getElementById("n00").innerHTML = "<font color=\"#ff0000\">" + myArray[0] + "</font>";
        document.getElementById("n01").innerHTML = "<font color=\"#00ff00\">" + myArray[1] + "</font>";
        document.getElementById("n02").innerHTML = "<font color=\"#0000ff\">" + myArray[2] + "</font>";
        document.getElementById("n03").innerHTML = "<font color=\"#ffff00\">" + myArray[3] + "</font>";
        document.getElementById("n04").innerHTML = "<font color=\"#ff00ff\">" + myArray[4] + "</font>";
        document.getElementById("n05").innerHTML = "<font color=\"#00ffff\">" + myArray[5] + "</font>";
        document.getElementById("n06").innerHTML = "<font color=\"#ffffff\">" + myArray[6] + "</font>";
        document.getElementById("n07").innerHTML = "<font color=\"#800000\">" + myArray[7] + "</font>";
        document.getElementById("n08").innerHTML = "<font color=\"#008000\">" + myArray[8] + "</font>";
        document.getElementById("n09").innerHTML = "<font color=\"#000080\">" + myArray[9] + "</font>";
        document.getElementById("n10").innerHTML = "<font color=\"#808000\">" + myArray[10] + "</font>";
        document.getElementById("n11").innerHTML = "<font color=\"#800080\">" + myArray[11] + "</font>";
        document.getElementById("n12").innerHTML = "<font color=\"#008080\">" + myArray[12] + "</font>";
        document.getElementById("n13").innerHTML = "<font color=\"#808080\">" + myArray[13] + "</font>";
        document.getElementById("n14").innerHTML = "<font color=\"#80ff00\">" + myArray[14] + "</font>";
        document.getElementById("n15").innerHTML = "<font color=\"#ff8000\">" + myArray[15] + "</font>";
        document.getElementById("n16").innerHTML = "<font color=\"#8000ff\">" + myArray[16] + "</font>";
        document.getElementById("n17").innerHTML = "<font color=\"#ff0080\">" + myArray[17] + "</font>";
        document.getElementById("n18").innerHTML = "<font color=\"#0080ff\">" + myArray[18] + "</font>";
        document.getElementById("n19").innerHTML = "<font color=\"#00ff80\">" + myArray[19] + "</font>";
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
