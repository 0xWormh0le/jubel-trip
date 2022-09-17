import { toJS } from 'mobx';
import fp from 'lodash/fp';

export const isLoadingTrip = fp.compose(
  Boolean,
  fp.find({ type: 'tripDetails' }),
  toJS
);

export const isLoadingTrips = fp.compose(
  Boolean,
  fp.find({ type: 'matchedTrips' }),
  toJS
);
