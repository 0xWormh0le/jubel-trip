import { toJS } from 'mobx';
import fp from 'lodash/fp';

import { beautifyValue } from './strings';

export const getTravelPaceLabel = (value, t) =>
  value < 33.33
    ? t('slow', 'Slow')
    : value < 66.67
    ? t('balanced', 'Balanced')
    : t('fast', 'Fast');

export const getAccommodationLabel = (value, t) => {
  if (value < 20) return t('hostel', 'Hostel');
  if (value >= 20 && value < 40) return t('budget', 'Budget');
  if (value >= 40 && value < 60) return t('mid_range', 'Mid-range');
  if (value >= 60 && value < 80) return t('luxe', 'Luxe');
  if (value >= 80) return t('ultra', 'Ultra');
};

export const getComfortLabel = (value, t) =>
  value < 33.33
    ? t('basic', 'Basic')
    : value < 66.67
    ? t('mid_range', 'Mid-Range')
    : t('luxurious', 'Luxurious');

export const getDevelopmentLabel = (value, t) =>
  value < 33.33
    ? t('remote', 'Remote')
    : value < 66.67
    ? t('balanced', 'Balanced')
    : t('urban', 'Urban');

export const getFamiliarityLabel = (value, t) =>
  value < 33.33
    ? t('off_the_beaten_path', 'Off The Beaten Path')
    : value < 66.67
    ? t('balanced', 'Balanced')
    : t('popular_classics', 'Popular Classics');

export const getPoorInfrastructureLabel = (value, t) =>
  value < 33.33
    ? t('not_worried', 'Not Worried')
    : value < 66.67
    ? t('can_handle_some', 'Can Handle Some')
    : t('rather_avoid_it', 'Rather Avoid It');

export const getLanguageBarrierLabel = (value, t) =>
  value < 33.33
    ? t('not_worried', 'Not Worried')
    : value < 66.67
    ? t('can_handle_some', 'Can Handle Some')
    : t('rather_avoid_it', 'Rather Avoid It');

export const getFoodPoisoningRiskLabel = (value, t) =>
  value < 33.33
    ? t('not_worried', 'Not Worried')
    : value < 66.67
    ? t('can_handle_some', 'Can Handle Some')
    : t('rather_avoid_it', 'Rather Avoid It');

export const getPettyCrimeRiskLabel = (value, t) =>
  value < 33.33
    ? t('not_worried', 'Not Worried')
    : value < 66.67
    ? t('can_handle_some', 'Can Handle Some')
    : t('rather_avoid_it', 'Rather Avoid It');

export const getStyleSliderLabel = (value, t) =>
  value < 33.33
    ? t('less', 'Less')
    : value < 66.67
    ? t('balanced', 'Balanced')
    : t('more', 'More');

export const getShockLabelIndex = (shock) =>
  Math.floor(shock / 20) + (shock % 20 > 0 ? 1 : 0);

export const getDefaultTripAccommodation = (preferences, tripId) => {
  const prefAccommodations = toJS(preferences.data.get('accommodations'));
  const prefBudget = toJS(preferences.data.get('budget'));
  const accommodationsOptions = toJS(preferences.accommodationsOptions);
  const trip = toJS(preferences.getTripById(tripId));
  if (trip) {
    // TODO: confirm the correct accommodations field name and data structure in trip model
    const tripAccommodations = fp.compose(
      fp.sortBy('price'),
      fp.map((type) => ({
        type,
        price: trip.accommodation[type],
      })),
      fp.keys
    )(trip.accommodation);
    let tripAccommodationFromBudget;
    let tripAccommodationFromAccomm;

    if (prefBudget) {
      tripAccommodationFromBudget = fp.compose(
        fp.last,
        fp.filter((item) => item.price < prefBudget)
      )(tripAccommodations);
      if (!tripAccommodationFromBudget) {
        tripAccommodationFromBudget = tripAccommodations[0];
      }
    }

    if (prefAccommodations.length > 0) {
      // finds the highest accommodation type from preferences
      // and gets the price of this type from trip
      const prefAccommodation = fp.compose(
        (type) => fp.find({ type })(tripAccommodations),
        fp.defaultTo('mid_range'),
        fp.get('value'),
        fp.last,
        fp.sortBy((item) =>
          fp.findIndex({ value: item })(accommodationsOptions)
        )
      )(prefAccommodations);

      if (prefAccommodation) {
        // finds the accommodation from trip which is
        // the closest to the highest accommodation selected
        tripAccommodationFromAccomm = fp.compose(
          fp.last,
          fp.filter((item) => item.price <= prefAccommodation.price)
        )(tripAccommodations);
      }
    }

    if (tripAccommodationFromAccomm && tripAccommodationFromBudget) {
      return tripAccommodationFromBudget.price <
        tripAccommodationFromAccomm.price
        ? tripAccommodationFromBudget.type
        : tripAccommodationFromAccomm.type;
    } else if (tripAccommodationFromBudget) {
      return tripAccommodationFromBudget.type;
    } else if (tripAccommodationFromAccomm) {
      return tripAccommodationFromAccomm.type;
    } else if (
      tripAccommodations.length > 0 &&
      !fp.find({ type: 'mid_range' })(tripAccommodations)
    ) {
      return tripAccommodations[0].type;
    } else {
      return 'mid_range';
    }
  }
  return 'mid_range';
};

export const valueToOption = (s) => ({ value: s, label: beautifyValue(s) });

export const filterOptions = (options, inputValue) =>
  options.filter((op) =>
    op.label.toLowerCase().includes(inputValue.toLowerCase())
  );

export const getSortedOptionsFromResponse = fp.compose(
  fp.map(valueToOption),
  fp.sortBy(fp.trim),
  fp.defaultTo([])
);

export const scenery = [
  'Beach',
  'Coast',
  'Countryside',
  'Desert',
  'Forest',
  'Jungle',
  'Lake',
  'Mountains',
  'Plains',
  'Snow',
  'Urban',
].map(valueToOption);

export const accommodations = [
  'hostel',
  'budget',
  'mid_range',
  'luxe',
  'ultra',
].map(valueToOption);
