import TripMatch from "./TripMatch";
import { slice, sampleSize, keys, camelCase, chunk, split, uniq } from 'lodash';
const sqlite3 = require('sqlite3').verbose();

export default () => {
  const db = new sqlite3.Database('./jubel.db');
  /**
   * Worker node code below
   * @type {number}
   */
  const self = process.pid;
  const MIN_MATCH_THRESHOLD = 1.0;
  const HARD_MATCH_THRESHOLD = 0.7;
  let trips = [];

  const getTrips = (req) =>
    trips.reduce((tripList, trip) => {
      const tripMatch = new TripMatch(trip, req);
      return tripMatch.score <= MIN_MATCH_THRESHOLD
        ? tripList.push(Object.assign(trip, { score: tripMatch.score })) &&
        tripList
        : tripList;
    }, []) || trips;

  process.on('message', (msg) => {
    const tripsIndex = msg['tripsIndex'];
    if (tripsIndex || tripsIndex === 0) {
      // Sets up the worker
      trips = require(`../data/trips/trips${tripsIndex}`);
      const firstId = trips[0]['id'];
      const lastId = trips[trips.length - 1]['id'];
      // notify parent that trips are set & ready to process
      console.warn(
      `✳️ ${self} started processing. Handling trips ${firstId} - ${lastId}`);
      return process.send({
        worker: self,
        delegatedTrips: [firstId, lastId]
      });
    }
    if (msg['random']) {
      const { requestId } = msg;
      // Fetches 10 random trips
      const random = sampleSize(trips, 10);
      return process.send({ response: random, requestId });
    }
    if (msg['tripsByPrefs']) {
      const { tripsByPrefs, requestId } = msg;
      const newTrips = getTrips(tripsByPrefs);
      return process.send({ response: newTrips, requestId });
    }
    if (msg['singleTripById']) {
      const { singleTripById, requestId } = msg;
      const requestedTrip = trips.find(t => t.id === singleTripById);
      return process.send({ response: requestedTrip, requestId });
    }
    if (msg['countryCode']) {
      const { countryCode, requestId } = msg;
      db.get('SELECT * from countries where country_code = ?', [countryCode], (err, row) => {
        const ccRow = keys(row).reduce((acc, k) => {
          if (k === 'bounding_box') {
            acc[camelCase(k)] = chunk(split(row[k], ","), 2);
          } else {
            acc[camelCase(k)] = row[k];
          }
          return acc;
        }, {});
        return process.send({ requestId, response: ccRow })
      })
    }
    if (msg['placesToSee']) {
      const { requestId } = msg;
      /**
       * Should reduce all of the trips' places into an array of (unique) values;
       */
      const locations = trips.reduce((acc, trip) => {
        acc.push(
          ...trip.destinations.map(d => d.name)
        );
        acc.push(...trip.continents);
        acc.push(...trip.countries);
        return acc;
      }, []);
      return process.send({ requestId, response: locations });
    }
    if (msg['activities']) {
      const { requestId } = msg;
      const activities = trips.reduce((acc, trip) => {
        return [...acc, ...trip.activities];
      }, []);
      return process.send({ requestId, response: activities });
    }

    /**
     * Functionality API
     */
    if (msg['kaboom']) {
      console.warn(`Worker ${self} acquired a termination message.`);
      process.disconnect()
    }
  });
}
