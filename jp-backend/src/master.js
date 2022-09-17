const app = require('./app').default;
import { slice, orderBy, random, flatten, keys, uniq } from 'lodash';
import EventEmitter from 'events';
import apicache from 'apicache';
import uuid from 'uuid/v4';

let cache = apicache.middleware;

/**
 * Emits the request to all workers and returns a promise arr with
 * their responses.
 * @param workers
 * @param requestName
 * @param requestData
 * @param requestId
 */
const promisifyWorkerResponses = (workers, requestName, requestData, requestId) =>
  workers.map(
    (worker) =>
      new Promise((res, rej) => {
        worker.responseEmitter.once(requestId, (msg) => {
          if (msg) {
            return res(msg);
          } return rej(404);
        });
        worker.send({ [requestName]: requestData, requestId });
      })
  );

/**
 * Attaches an eventWrapper to the worker processes.
 * The eventWrapper parses the event sent from the worker and
 * emits a separate event which bears the name equal to the requestId.
 * @param worker
 */
const attachEventWrapper = (worker) => {
  worker.responses = {};
  worker.responseEmitter = new EventEmitter();
  worker.on('message', (event) => {
    if (event.requestId) {
      worker.responseEmitter.emit(event.requestId, event.response);
    }
  });
};

/*
 * @param cluster
 * @param workers
 * @param tripsIndex
 * @returns {Promise<unknown>}
 */
const createWorker = (cluster, workers, tripsIndex) => new Promise((res, rej) => {
  const worker = cluster.fork();
  worker.on('error', (error, signal) => {
    /**
     * This will handle IPC_CHANNEL_CLOSED and similar errors when a worker dies but additional
     * requests are fired and somehow routed to it before it is revived. We should probably
     * be logging these errors somewhere besides the console.
     * TODO log errors
     */
    console.error(`Error emitted by worker ${worker.process.pid}`);
  });
  worker.once('message', (msg) => {
    if (msg['delegatedTrips']) {
      worker['delegatedTrips'] = msg['delegatedTrips'];
      worker['tripsIndex'] = tripsIndex;
      attachEventWrapper(worker);
      workers.push(worker);
      return res(worker);
    } else {
      return rej(false);
    }
  });
  worker.send({ tripsIndex });
});

export default (cluster) => {
  const port = 3001;
  const numWorkers = require('os').cpus().length;
  console.warn(`Num of cpu cores to occupy: ${numWorkers}`);
  let workers = [];
  const setRoutes = () => {
    app.get('/', (req, res) => res.sendStatus(200));
    app.get('/trips', (req, res) => {
      const requestId = uuid();
      const worker = workers[random(0, workers.length - 1)];
      // Set up a listener for resulting trips from that worker
      worker.responseEmitter.once(requestId, (workerResponse) => {
        if (workerResponse) {
          return res.send(workerResponse);
        } else {
          return res.send(404);
        }
      });
      // Request random trips from the worker
      worker.send({ random: true, requestId });
    });
    app.get(`/trips/:tripId`, cache('1 day'), (req, res) => {
      const tripId = req.params.tripId;
      if (!isNaN(tripId)) {
        const promises = promisifyWorkerResponses(workers, 'singleTripById', Number(tripId), uuid());
        return Promise.all(promises.map(p => Promise.resolve(p)
          .then(
            val => ({ status: 1, value: val }),
            err => ({ status: 0, reason: err })
          )))
          .then(promiseResult => {
              const okPromise = promiseResult.find(workerResult => workerResult.status);
              const trip = okPromise ? okPromise.value : false;
              return trip ? res.send(trip) : res.sendStatus(404);
            }
          )
      }
      return res.sendStatus(404);
    });
    app.post('/trips', (req, res) => {
      if (req.body) {
        const promises = promisifyWorkerResponses(workers, 'tripsByPrefs', req.body, uuid());
        return Promise.all(promises).then((trips) =>
          res.send(slice(orderBy(flatten(trips), ['score']), 0, 10))
        );
      }
    });
    app.get('/countries/:countryCode', cache('1 day') ,(req, res) => {
      const worker = workers[random(0, workers.length - 1)];
      const requestId = uuid();
      worker.responseEmitter.once(requestId, (msg) => {
        return msg && msg['boundingBox'] ? res.send(msg) : res.sendStatus(404);
      });
      const { countryCode } = req.params;
      worker.send({ countryCode, requestId });
    });
    app.get('/placesToSee', cache('1 day'), (req, res) => {
      const promises = promisifyWorkerResponses(workers, 'placesToSee', {}, uuid());
      return Promise.all(promises).then((places) => {
          return res.send({
            placesToSee: uniq(flatten(places))
          });
        }
      );
    });
    app.get('/activities', cache('1 day'), (req, res) => {
      const promises = promisifyWorkerResponses(workers, 'activities', {}, uuid());
      return Promise.all(promises).then((activities) => {
          return res.send({
            activities: uniq(flatten(activities))
          });
        }
      );
    });
    app.listen(port, () => {
      console.warn(`Jubel SQLite backend started on port ${port}`);
    });
  };

  Promise.all([...Array(numWorkers).keys()].map(i => createWorker(cluster, workers, i))).then(setRoutes());

  cluster.on('online', (worker) => {
    console.log('Worker ' + worker.process.pid + ' spinning up..');
  });
  cluster.on('exit', (worker, code, signal) => {
    console.error(`Worker ${worker.process.pid} died with code: ${code} & signal ${signal}, restarting.`);
    const indexOfDeadWorker = workers.findIndex(w => w.process.pid === worker.process.pid);
    workers.splice(indexOfDeadWorker, 1);
    const deadWorkerTrips = worker.tripsIndex;
    createWorker(cluster, workers, deadWorkerTrips)
      .then(newWorker =>
        console.log(`Restarted worker for trips ${newWorker['delegatedTrips'][0]} - ${newWorker['delegatedTrips'][1]}`)
      )
  });
}
