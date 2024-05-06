/** @type {HTMLCanvasElement} */
const canvas = document.getElementById("canvas")

/** @type {CanvasRenderingContext2D} */
let ctx = canvas.getContext("2d");

let width = canvas.width;
let height = canvas.height;

function toCanvasCoord(evt) {
  return [0.5 * width * (evt.x + 1), 0.5 * height * (evt.y + 1)]
}

function isInsideCircle(evt) {
  return Math.pow(evt.x, 2) + Math.pow(evt.y, 2) < 1;
}

function updateCanvas(evt) {
  ctx.beginPath()
  let [x, y ] = toCanvasCoord(evt)
  if (isInsideCircle(evt)) {
    ctx.strokeStyle = "red"
  } else {
    ctx.strokeStyle = "black"
  }
  ctx.ellipse(x, y, 1, 1, 0, 0, 2 * Math.PI);
  ctx.stroke()
}

const opts = {
  title: "Pi estimation",
  width: 500,
  height: 600,
  pxAlign: false,
  scales: {
    y: {
      //	auto: false,
      range: [0, 4],
    }, x: {
      time: false
    },
    Abs: {
      auto: true
    },
    "%": {
      distr: 3
    }
  },
  axes: [
    {
      space: 50,
    },
    {scale: "Abs"},
    {scale: "%", side: 1, values: (u, vals, space) => vals.map(v => new Intl.NumberFormat('no-NO', {notation: "engineering"}).format(v)),
    size: 100}
  ],
  series: [
    {},
    {
      label: "Sine",
      stroke: "red",
      scale: "Abs",
      spanGaps: true,
    },
    {
      label: "Error",
      stroke: "blue",
      scale: "%",
      spanGaps: true,
    },
  ],
};

let data= [[], [], []];
let u = new uPlot(opts, data, document.getElementById("graph"));

function updatePlot() {
//  let data = [[1], [Math.random()], [Math.random()]]
  u.setData(data)
  requestAnimationFrame(updatePlot)
}

updatePlot()

let lastData = {};

const socket = new WebSocket("ws://localhost:8081/ws/");
socket.addEventListener("message", (event) => {
  let evt = JSON.parse(event.data);

  if (evt.topic === "pi-estimation") {
    console.log(data)
    let val = evt.payload;
    let lastIdxVal = data[0].at(-1) + 1 || -1;
    data[0].push(lastIdxVal + 1);
    data[1].push(val)
    let lastVal = data[2].at(-1)
    data[2].push(lastVal)
    console.log("est", val)

    lastData.estimation = val;
  } else if (evt.topic === "pi-error") {
    let val = evt.payload;
    let lastIdxVal = data[0].at(-1) + 1 || -1;
    data[0].push(lastIdxVal + 1);
    let lastVal = data[1].at(-1);
    data[1].push(lastVal)
    data[2].push(val)
    lastData.error = val;
    console.log("err", val)
  } else if (evt.topic === "randoms") {
    updateCanvas(evt.payload)
  }
  document.getElementById("current-val").innerText = `Current estimation: ${lastData.estimation}. Error: ${lastData.error}`
});
