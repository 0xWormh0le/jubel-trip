import cluster from 'cluster';
import setupMaster from './master';
import setupWorker from './worker';

/**
 * Spawn a master node which handles server logic (accepting requests).
 * It passes the request's object to each worker node and awaits their results before
 * returning the result to the client.
 */

if (cluster.isMaster) {
  setupMaster(cluster)
} else {
  setupWorker();
}
