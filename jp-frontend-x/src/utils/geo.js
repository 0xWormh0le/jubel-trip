import polylabel from 'polylabel';

export const getDestinationsGeoJson = (destinations = []) => {
  const lines = {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: linearRing(
            destinations.map(({ longitude, latitude }) => [longitude, latitude])
          ),
        },
      },
    ],
  };

  const points = {
    type: 'FeatureCollection',
    features: destinations.map(({ id, name, latitude, longitude }, index) => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [longitude, latitude],
      },
      properties: {
        kind: 'destination',
        number: index + 1,
        id,
        name,
      },
    })),
  };

  return { lines, points };
};

export const linearRing = (coordinates = []) => {
  if (coordinates.length < 2) return coordinates;
  const [firstLon, firstLat] = coordinates[0];
  const [lastLon, lastLat] = coordinates[coordinates.length - 1];
  if (firstLon === lastLon && firstLat === lastLat) return coordinates;
  return [...coordinates, [firstLon, firstLat]];
};

export const coordinatesFromDestinations = (destinations = []) =>
  destinations.map(({ latitude, longitude }) => [longitude, latitude]);

export const centroid = (coordinates) => {
  const ring = linearRing(coordinates);
  const pos = polylabel([ring], 1.0, true);
  return pos;
};

export const REPRESENTATIVE_COORDINATE_POLICY = 'centroid';

export const representativeCoordinate = (destinations = []) => {
  const coords = coordinatesFromDestinations(destinations);
  return REPRESENTATIVE_COORDINATE_POLICY === 'first'
    ? coords[0]
    : centroid(coords);
};

export const getBoundsFromDestinations = (destinations = []) => {
  const sw = [181, 91];
  const ne = sw.map((x) => -x);
  return destinations.reduce(
    ([sw, ne], { longitude, latitude }) => [
      [Math.min(sw[0], longitude), Math.min(sw[1], latitude)],
      [Math.max(ne[0], longitude), Math.max(ne[1], latitude)],
    ],
    [sw, ne]
  );
};

export const getBoundsFromTrips = (trips = []) => {
  return getBoundsFromDestinations(
    trips.reduce((z, t) => z.concat(t.destinations), [])
  );
};

export const getGeoJSONForTrips = (trips = []) => {
  const gj = {
    points: {
      type: 'FeatureCollection',
      features: trips.reduce((acc, { id, destinations, name }, index) => {
        acc.push({
          type: 'Feature',
          geometry: {
            type: 'Point',
            coordinates: representativeCoordinate(destinations),
          },
          properties: {
            kind: 'trip',
            number: index + 1,
            id,
            name,
          },
        });
        return acc;
      }, []),
    },
  };
  return gj;
};

export const getGeoJSONForDestination = (destination) => {
  const point = {
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [destination.longitude, destination.latitude],
    },
    properties: {
      kind: 'destination',
      id: destination.id,
      name: destination.name,
    },
  };

  return { point };
};

export const getBoundsOfDestinationFromTrips = (destination, trip) => {
  const { longitude, latitude } = destination;
  const radius = trip.destinations.reduce(
    (radius, dest) =>
      dest.id === destination.id
        ? radius
        : Math.min(
            radius,
            Math.abs(dest.longitude - longitude),
            Math.abs(dest.latitude - latitude)
          ),
    180
  );
  return [
    [longitude - radius, latitude - radius],
    [longitude + radius, latitude + radius],
  ];
};

export const getBoundsOfCountry = (country) => {
  const bb = country.boundingBox;
  return {
    country: {
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [[bb[0], bb[1]], [bb[2], bb[3]]],
      },
    },
  };
};
