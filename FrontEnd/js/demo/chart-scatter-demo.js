// Set new default font family and font color to mimic Bootstrap's default styling
Chart.defaults.global.defaultFontFamily = 'Nunito', '-apple-system,system-ui,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif';
Chart.defaults.global.defaultFontColor = '#858796';

// Scatter Chart Example
var ctx = document.getElementById("myScatterChart");
var myScatterChart = new Chart(ctx, {
  type: 'scatter',
  data: {
    datasets: [{
      label: "Queries",
      borderColor: "rgba(78, 115, 223, 1)",
      backgroundColor: "rgba(78, 115, 223, 0.5)",
      pointRadius: 3,
      pointBackgroundColor: "rgba(78, 115, 223, 1)",
      pointBorderColor: "rgba(78, 115, 223, 1)",
      pointHoverRadius: 3,
      pointHoverBackgroundColor: "rgba(78, 115, 223, 1)",
      pointHoverBorderColor: "rgba(78, 115, 223, 1)",
      data: [{
        x: 10,
        y: 20
      }, {
        x: 15,
        y: 10
      }, // Add your data points here
      ],
    }],
  },
  options: {
    scales: {
      xAxes: [{
        scaleLabel: {
          display: true,
          labelString: 'Runtime (seconds)'
        }
      }],
      yAxes: [{
        scaleLabel: {
          display: true,
          labelString: 'Memory (MB)'
        }
      }]
    },
    tooltips: {
      callbacks: {
        label: function(tooltipItem, data) {
          var label = data.datasets[tooltipItem.datasetIndex].label || '';
          return label + ': (' + tooltipItem.xLabel + ', ' + tooltipItem.yLabel + ')';
        }
      }
    }
  }
});
