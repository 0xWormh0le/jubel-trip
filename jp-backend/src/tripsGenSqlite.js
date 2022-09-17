const sqlite3 = require('sqlite3').verbose();
const { startCase, uniq, capitalize, lowerCase, keys, chunk, sortBy, concat, flatten } = require('lodash');
const util = require('util');
const fs = require('fs');

const db = new sqlite3.Database('./jubel.db');

const jubelBaseImageUrl = 'https://jubelproduction.s3.amazonaws.com/uploads';

const tsKeys = ['oc', 'an', 'cn', 'cu', 'pp'];
const shKeys = ['crime', 'food', 'infrastructure', 'language'];
const acKeys = ['hostel', 'budget', 'mid_range', 'luxe', 'ultra'];
const acCostFactors = {
  'hostel': 0.25,
  'budget': 0.5,
  'mid_range': 0.8,
  'luxe': 1.3,
  'ultra': 2.5
};
const calcAccommodationPrice = (estBaseCost, accommodationName) => {
  const factor = acCostFactors[accommodationName] || 0;
  return Math.round(estBaseCost * factor);
};

let resultingObject = {};

/**
 * This script is intended to be executed prior to starting the Node cluster.
 * Each worker should be assigned a subset of the data.
 * @returns {Promise<unknown>}
 */

const tripsGen = () =>
  new Promise((res, rej) => {
    let promises = [];
    for (const q of Object.keys(queries)) {
      promises.push(
        new Promise((resolve, reject) => {
          db.all(queries[q], (err, rows) => {
            if (err) {
              console.error(`Error processing table ${q}: ${err}`);
              reject(false);
            }
            resultingObject[q] = rows;
            resolve(true);
          });
        })
      );
    }
    Promise.all(promises).then((r) => res(true));
  });

const formTrips = async () => {
  const activities = resultingObject['activities'];
  const computedActivities = resultingObject['activitiesByDestinations'].reduce(
    (acc, da) => {
      acc[da['destination_id']] = da['activities']
        .split(',')
        .map((activity) => activities.find((a) => a.id === activity).name);
      return acc;
    },
    {}
  );
  const computedCountries = resultingObject['countries'].map((c) => ({
    ...c,
    continent: resultingObject['continents'].find(
      (cont) => cont['id'] === c['continent_id']
    ),
  }));
  const itineraries = resultingObject['destinationsByTrip'].reduce(
    (acc, iti) => {
      const travelStyles = { oc: 0, an: 0, cn: 0, cu: 0, pp: 0 };
      const shocks = { crime: 0, food: 0, language: 0, infrastructure: 0 };
      const accommodations = {
        hostel: 0,
        budget: 0,
        mid_range: 0,
        luxe: 0,
        ultra: 0,
      };
      let duration = 0;
      let estimatedCost = 0;
      const images = [];
      const route = [];
      const summary = [];
      let lastCountry = '';
      const destinations = iti['destinations']
        .split(',')
        .map((a) => a.split(':'))
        .sort((a, b) => parseInt(a[0]) - parseInt(b[0]))
        .map((a) => a[1])
        .map((destId) => resultingObject['destinations'].find(
            (destination) => destination['id'] === parseInt(destId)
          )
        )
        .map((d) => {
          let {
            geoname_id,
            natural_scenery,
            development_level,
            latitude,
            longitude,
            country_id,
            days_min,
            days_max,
            airport,
            seasons,
          } = d;
          natural_scenery = natural_scenery
            .split('{')
            .pop()
            .split('}')[0]
            .split(',');
          const name = capitalize(d.name);
          const destImages = resultingObject['images']
            .filter((img) => img['destination_id'] === d.id)
            .reduce((acc, img) => {
              const mainImage = {
                src: `${jubelBaseImageUrl}/destination/main_image/${d.id}/${img.main_image}`,
                thumbnail: `${jubelBaseImageUrl}/destination/main_image/${d.id}/card_${img.main_image}`,
                caption: d.name,
              };
              acc.push(mainImage);
              images.push(mainImage);
              img['other_images'].split(',').forEach((otherImg) => {
                const [imgId, imgSrc]  = otherImg.split('^');
                acc.push({
                  src: `${jubelBaseImageUrl}/photo/image/${imgId}/${imgSrc}`,
                  thumbnail: `${jubelBaseImageUrl}/photo/image/${imgId}/thumb_${imgSrc}`,
                  caption: d.name,
                })
                }
              );
              return acc;
            }, []);
          const ts = {
            oc: d.styles_oc,
            an: d.styles_an,
            cn: d.styles_cn,
            cu: d.styles_cu,
            pp: d.styles_pp,
          };
          tsKeys.forEach((k) => (travelStyles[k] += ts[k]));
          const sh = {
            crime: d.shocks_crime,
            food: d.shocks_food,
            infrastructure: d.shocks_infrastructure,
            language: d.shocks_language,
          };
          shKeys.forEach((k) => (shocks[k] += sh[k]));
          const country = computedCountries.find((c) => c.id == country_id);
          let cost = days_min * Math.round(Math.random() * 200 + 50);
          db.all(`SELECT * FROM cost_estimates WHERE country="${lowerCase(country.name)}"`, (err, rows) => {
            const destMatch = rows.find(c => c.name === lowerCase(name));
            if (destMatch) {
              cost = destMatch['per_diem'] * days_min;
            } else if (rows.length >= 1) {
              const other = rows.find(c => c.name === 'other');
              if (other) {
                cost = other['per_diem'] * days_min;
              } else {
                cost = rows[0]['per_diem'] * days_min;
              }
            }
          });
          const ac = {
            hostel: d['accommodation_hostel'],
            budget: d['accommodation_budget'],
            mid_range: d['accommodation_mid_range'],
            luxe: d['accommodation_luxe'],
            ultra: d['accommodation_ultra'],
          };
          acKeys.forEach((k) => (accommodations[k] += (ac[k] === 'true' || ac[k] === true) ? 1 : 0));
          const conventionality = d.familiarity;
          const start = duration;
          const days = days_min; // TODO ???
          duration += parseInt(days);
          estimatedCost += cost;
          const continent = resultingObject['continents'].find(
            (cont) => cont.id == country['continent_id']
          );
          const dest = {
            id: d.id,
            gid: geoname_id,
            name,
            accommodation: ac,
            continentCode: continent.code,
            continentName: startCase(continent.name),
            countryName: startCase(country.name),
            countryCode: country.code,
            latitude: latitude,
            longitude: longitude,
            duration: `P${days_min}D`,
            estimatedCost: cost,
            description: d.description,
            shocks: sh,
            travelStyles: ts,
            seasons: JSON.parse(seasons),
            conventionality,
            airport,
            activities: computedActivities[d.id],
            naturalScenery: natural_scenery.map(startCase),
            developmentLevel: startCase(development_level),
            images: destImages,
          };
          const elements = [];
          if (d.country_name !== lastCountry) {
            const transportElement = {
              type: 'transport',
              transport: 'flight',
              name: dest.name,
              icon: 'transport-flight',
              duration: `PT${Math.round(60 + Math.random() * 15 * 60)}M`,
            };
            summary.push(transportElement);
            elements.push(transportElement, {
              type: 'destination-country',
              name: dest.countryName,
              code: dest.countryCode,
              description: 'Country profile description',
            });
          }
          lastCountry = d.country_name;
          if (elements.length === 0) {
            const transportElement = {
              type: 'transport',
              transport: 'car',
              name: dest.name,
              icon: 'transport-car',
              duration: `PT${Math.round(30 + Math.random() * 500)}M`,
            };
            elements.push(transportElement);
            summary.push(transportElement);
          }
          const destElement = {
            type: 'destination-city',
            icon: 'map-marker',
            ...dest,
          };
          elements.push(destElement);
          summary.push(destElement);
          route.push({
            start,
            days,
            elements,
          });
          return dest;
        });
      /**
       * Finds the trip/itinerary object from the initial trips query
       */
      const itinerary = resultingObject['trips'].find(
        (j) => j['id'] == iti['itinerary_id']
      );
      const { id, title, description } = itinerary;
      const returnFlight = {
        type: 'transport',
        transport: 'flight',
        name: 'Home',
        icon: 'transport-flight',
        duration: `PT${Math.round(60 + Math.random() * 15 * 60)}M`,
      };
      route.push({
        start: duration,
        days: 1,
        elements: [returnFlight],
      });
      summary.push(returnFlight);
      const numDestinations = destinations.length;
      tsKeys.forEach(
        (k) => (travelStyles[k] = Math.round(travelStyles[k] / numDestinations))
      );
      shKeys.forEach(
        (k) => (shocks[k] = Math.round(shocks[k] / numDestinations))
      );
      acKeys.forEach(
        (k) => (accommodations[k] = accommodations[k] / numDestinations > 0.5)
      );
      const countries = uniq(destinations.map((d) => d.countryName)).sort();
      const continents = uniq(destinations.map((d) => d.continentName)).sort();
      const activities = uniq(
        destinations.reduce((a, d) => [...a, ...(d.activities || [])], [])
      ).sort();
      const naturalScenery = uniq(
        destinations.reduce((a, d) => [...a, ...(d.naturalScenery || [])], [])
      ).sort();
      const developmentLevel = uniq(
        destinations.reduce((a, d) => [...a, d.developmentLevel], [])
      ).sort();
      estimatedCost = Math.round(estimatedCost + ((estimatedCost / duration) * destinations.length));
      const accommodationCosts = keys(accommodations)
        .filter(k => accommodations[k])
        .reduce((acc, accommodationType) => {
          acc[accommodationType] = calcAccommodationPrice(estimatedCost, accommodationType);
          return acc;
        }, {});
      acc.push({
        id,
        name: title,
        description,
        accommodation: accommodationCosts,
        duration: `P${duration}D`,
        estimatedCost,
        countries,
        continents,
        destinations,
        activities,
        naturalScenery,
        developmentLevel,
        travelStyles,
        shocks,
        images,
        route,
        summary,
      });
      return acc;
    },
    []
  );
  const numWorkers = require('os').cpus().length;
  const tripListSize = itineraries.length;
  const numOfTripsPerWorker = Math.floor(tripListSize / numWorkers);
  const sorted = sortBy(itineraries, 'id');
  const chunked = chunk(sorted, numOfTripsPerWorker);
  const length = chunked.length;
  // Concat the last two arrays
  const lastTwo = [chunked[length - 2], chunked[length - 1]];
  chunked.splice(length - 2, 2, flatten(lastTwo));
  chunked.forEach((chunk, index) => {
    if (!fs.existsSync('data/trips')){
      fs.mkdirSync('data/trips', { recursive: true });
    }
    fs.writeFile(`data/trips/trips${index}.js`,
      `const trips = ${JSON.stringify(chunk, {
        showHidden: false,
        depth: null,
        compact: false,
        maxArrayLength: null
      })}; module.exports = trips;`,
      (err, ok) => {
        console.warn(`Done writing ${index}`);
    });
  });
};

/**
 * Query strings below...
 * @type {string}
 */

const trips = `select * from trips where id not in (120,123) order by id;`;

const destinations = `select id,
           airport,
           name,
           description,
           latitude,
           longitude,
           geoname_id,
           country_id,
           cast ( infrastructure_shock/5.0 * 100.0 as int ) + ( infrastructure_shock/5.0 * 100.0 > cast ( infrastructure_shock/5.0 * 100.0 as int )) as shocks_infrastructure,
           cast ( food_shock/5.0 * 100.0 as int ) + ( food_shock/5.0 * 100.0 > cast ( food_shock/5.0 * 100.0 as int )) as shocks_food,
           cast ( language_shock/5.0 * 100.0 as int ) + ( language_shock/5.0 * 100.0 > cast ( language_shock/5.0 * 100.0 as int )) as shocks_language,
           cast ( crime_shock/5.0 * 100.0 as int ) + ( crime_shock/5.0 * 100.0 > cast ( crime_shock/5.0 * 100.0 as int )) as shocks_crime,
           active_naturist as styles_an,
           chilled_naturist as styles_cn,
           culturist as styles_cu,
           oceanist as styles_oc,
           party_purist as styles_pp,
           hostel_accommodation as accommodation_hostel,
           budget_accommodation as accommodation_budget,
           mid_range_accommodation as accommodation_mid_range,
           luxe_accommodation as accommodation_luxe,
           ultra_accommodation as accommodation_ultra,
           full_days_min as days_min,
           full_days_max as days_max,
           natural_scenery,
           development_level,
           seasons,
           cast ((1-travel_style/2.0)*100.0 as int)+((1-travel_style/2.0)*100.0>cast((1-travel_style/2.0) * 100.0 as int)) as familiarity
    from destinations
    where country_id not in (31,105)
    order by id;`;

const destinationsByTrip = `select trip_id as itinerary_id, group_concat(position || ':' || destination_id) as destinations
    from trips_destinations
    where trip_id not in (120, 123)
    group by trip_id
    order by trip_id;`;

const activities = `select id,name,destination_activities_count as count from activities order by id;`;

const activitiesByDestinations = `select da.destination_id, group_concat(activity_id) as activities
    from destination_activities da
        join destinations d on da.destination_id = d.id
    where d.country_id not in (31,105)
    group by destination_id
    order by destination_id;`;

const images = `select d.id as destination_id, d.main_image, group_concat(p.id || '^' || p.image) as other_images
    from images p
        join destinations d on p.destination_id = d.id
    where d.country_id not in (31,105)
    group by d.id
    order by d.id;`;

const countries = `select c.id, c.geoname_id, c.country_code as code, c.name, c.continent_id from countries c where c.id not in (31,105) order by c.country_code;`;

const continents = `select c.id, c.geoname_id, c.continent_code as code, c.name from continents c where c.id!=9 order by c.continent_code;`;

const queries = {
  trips,
  destinations,
  destinationsByTrip,
  activities,
  activitiesByDestinations,
  images,
  countries,
  continents,
};

tripsGen()
  .then((r) => {
    formTrips().then((r) => null);
  })
  .catch((e) => console.error(e));
