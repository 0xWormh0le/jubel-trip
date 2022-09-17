import { uniq, zip, entries, sum, findKey } from 'lodash';
import Fuse from 'fuse.js';
import { parse, toSeconds } from 'iso8601-duration';

const FUSE_OPTIONS = {
  shouldSort: true,
  includeScore: true,
  threshold: 0.6,
  location: 0,
  distance: 100,
  maxPatternLength: 32,
  minMatchCharLength: 1,
  keys: [
    'name',
    'description',
    'destNames',
    'destDescriptions',
    'countries',
    'continents',
  ],
};

const WEIGHTS = new Map([
  ['accommodation', 1],
  ['activity', 1],
  ['budget', 1.0],
  ['dates', 1.0],
  ['development', 1],
  ['duration', 50],
  ['familiarity', 1],
  ['groupSize', 1.0],
  ['groupType', 1.0],
  ['placeToSee', 5],
  ['placeToAvoid', 5],
  ['scenery', 1],
  ['searchText', 1],
  ['travelPace', 1],
  ['shock', -1.0],
  ['style', 1.0],
]);

const months = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

export default class TripMatch {
  constructor(trip, userPrefs) {
    this.userPrefs = userPrefs;
    this.trip = trip;
    this.name = trip.name;
    this.accommodations = [];
    this.computeScore();
  }

  /**
   * These come from the client-side.
   */
  userPrefAsFeatures() {
    const map = new Map();
    entries(this.userPrefs).forEach(([featureName, featureVal]) => {
      if (featureVal) {
        switch (featureName) {
          case 'accommodations':
            this.accommodations = featureVal;
            map.set(`accommodation`, 1.0);
            break;
          case 'activities':
            featureVal.forEach((f) => map.set(`activity~${f.value}`, 1.0));
            break;
          case 'budget':
            map.set('budget', featureVal / this.trip.estimatedCost);
            break;
          case 'departOnOrAfterDate':
            map.set(
              `dates~departOnAfter#${featureVal}#${
                this.userPrefs.datesFlexible ? 'flex' : 'strict'
              }`,
              1.0
            );
            break;
          case 'returnOnOrBeforeDate':
            map.set(
              `dates~returnOnOrBefore#${featureVal}#${
                this.userPrefs.datesFlexible ? 'flex' : 'strict'
              }`,
              1.0
            );
            break;
          case 'development':
            map.set('development', featureVal / 100);
            break;
          case 'durationInDays':
            map.set(`duration~${featureVal}`, 1.0);
            break;
          case 'familiarity':
            map.set('familiarity', featureVal / 100);
            break;
          case 'groupSize':
            map.set(`groupSize~${featureVal}`, 1.0);
            break;
          case 'groupType':
            map.set(`groupType~${featureVal}`, 1.0);
            break;
          case 'placesToAvoid':
            featureVal.forEach((f) => map.set(`placeToAvoid~${f.value}`, 0.0));
            break;
          case 'placesToSee':
            featureVal.forEach((f) => map.set(`placeToSee~${f.value}`, 1.0));
            break;
          case 'scenery':
            featureVal.forEach((f) => map.set(`scenery~${f.value}`, 1.0));
            break;
          case 'searchText':
          case 'placesToSeeHomeScreen':
            map.set(`searchText~${featureVal}`, 1.0);
            break;
          case 'travelPace':
            map.set('travelPace', featureVal / 100);
            break;
          case 'travelShocks':
            entries(featureVal).forEach(([shockName, shockScore]) =>
              map.set(`shock~${shockName}`, shockScore / 100)
            );
            break;
          case 'travelStyles':
            entries(featureVal).forEach(([styleName, styleScore]) =>
              map.set(`style~${styleName}`, styleScore / 100)
            );
            break;
        }
      }
    });
    return map;
  }

  /**
   * Probably just a temporary workaround, this
   * increases the weights of certain features
   * depending on their intensity (distance from the default value).
   * @param featureMap
   */
  adaptWeights(featureMap) {
    for (const [feature, featureVal] of featureMap.entries()) {
      const pref = feature.split('~')[0];
      switch (pref) {
        case 'travelPace':
        case 'style':
        case 'familiarity':
        case 'development': {
          if (featureVal || featureVal === 0) {
            const value = Math.abs(featureVal - 0.5) * 100;
            if (!WEIGHTS.get(pref) || WEIGHTS.get(pref) < value) {
              WEIGHTS.set(pref, value);
            }
          }
          break;
        }
      }
    }
  }

  computeScore() {
    const featureMap = this.userPrefAsFeatures();
    const features = Array.from(new Set(featureMap.keys())).sort();
    const prefsFeatures = features.map((f) => featureMap.get(f));
    this.adaptWeights(featureMap); // TODO experimental
    const rawWeights = features.map((f) => WEIGHTS.get(f.split('~')[0]));
    const weightSum = rawWeights.reduce((acc, w) => (acc += w), 0);
    const weights = rawWeights.map((weight) => weight / weightSum);
    const itineraryFeatures = features.map((f) => {
      const tf = this.getTripFeature(f);
      return tf || 0;
    });
    this.score = TripMatch.getDistance(
      itineraryFeatures,
      prefsFeatures,
      weights
    );
  }

  getTripFeature(feature) {
    switch (feature) {
      case 'accommodation':
        return this.getAccommodationScore();
      case 'budget':
        return 1.0;
      case 'development':
        return TripMatch.computeDevelopment(this.trip.developmentLevel);
      case 'familiarity':
        const familiarity = this.trip.destinations.map(d => d.conventionality);
        return TripMatch.computeFamiliarity(familiarity);
      case 'travelPace':
        return this.getTravelPaceScore();
      default: {
        const [prefix, suffix] = feature.split('~');
        switch (prefix) {
          case 'activity':
            return this.getActivityScore(suffix);
          case 'dates':
            return this.getDateScore(suffix);
          case 'duration':
            return this.getDurationScore(suffix);
          case 'groupSize':
          case 'groupType':
            return 1.0;
          case 'placeToSee':
            return this.getPlaceToSeeScore(suffix);
          case 'placeToAvoid':
            return this.getPlaceToAvoidScore(suffix);
          case 'scenery':
            return this.getSceneryScore(suffix);
          case 'searchText':
            return this.getSearchTextScore(suffix);
          case 'shock':
            return this.getShockScore(suffix);
          case 'style':
            return this.getStyleScore(suffix);
          default:
            return 0.0;
        }
      }
    }
  }

  getAccommodationScore() {
    return this.accommodations.reduce((acc, accommodation) => {
      return acc || (this.trip.accommodation[accommodation.value] ? 1 : 0);
    }, 0);
  }

  getActivityScore(activity) {
    return this.trip.activities.includes(activity) ? 1.0 : 0.0;
  }

  getSearchTextScore(searchText) {
    const fts = new Fuse(
      [
        {
          name: this.trip.name,
          countries: this.trip.countries.join(','),
          continents: this.trip.destinations.reduce(
            (acc, c) => `${acc}, ${c.continentName}`,
            ''
          ),
          description: this.trip.description,
          destNames: this.trip.destinations.reduce(
            (acc, e) => (acc += ` ${e.name}`),
            ''
          ),
          destDescriptions: this.trip.destinations.reduce(
            (acc, e) => (acc += ` ${e.description}`),
            ''
          ),
        },
      ],
      FUSE_OPTIONS
    ).search(searchText);
    // a score of 0 is a perfect match
    return fts[0] ? 1 - fts[0].score : 0;
  }

  getShockScore(shock) {
    return this.trip.shocks[shock] / 100;
  }

  getStyleScore(style) {
    return this.trip.travelStyles[style] / 100;
  }

  getDateScore(date) {
    const getScoreForSeasons = (month) => {
      const len = seasons.length;
      const vals = seasons.reduce((acc, season) => {
        switch (season[month]) {
          case 'R':
            return acc + 1.0;
          case 'O':
            return acc + 0.5;
          default:
            return acc + 0;
        }
      }, 0);
      return vals/len;
    };
    const split = date.split('#');
    const isFlexible = split[2] === 'flex';
    const dateStr = split[1];
    const d = new Date(dateStr);
    const seasons = this.trip.destinations.map((d) => d.seasons);
    if (d.getDate()) {
      const month = months[d.getMonth()];
      const score = getScoreForSeasons(month);
      if (isFlexible && score <= 1) {
        const plusOneMonthScore = getScoreForSeasons(month + 1);
        const minusOneMonthScore = getScoreForSeasons(month - 1);
        return Math.max(plusOneMonthScore, minusOneMonthScore, score);
      } else {
        return score;
      }
    } else {
      return 0.0;
    }
  }

  getDurationScore(duration) {
    const numOfDays = TripMatch.getDaysFromISODuration(this.trip.duration);
    const sigmoid = (input) => input / Math.sqrt(1 + Math.pow(input, 2));
    if (numOfDays || numOfDays === 0) {
      return 1 - Math.abs(0 - sigmoid(Math.abs(duration - numOfDays))) * 2;
    } else {
      return 0;
    }
  }

  getPlaceToSeeScore(place) {
    const totalDuration = TripMatch.getDaysFromISODuration(this.trip.duration);
    const placeLowerCase = place.toLowerCase();
    const matchedDest = this.trip.destinations.filter(d =>
      d.countryName.toLowerCase().includes(placeLowerCase) ||
      d.name.toLowerCase().includes(placeLowerCase) ||
      d.continentName.toLowerCase().includes(placeLowerCase)
    );
    if (matchedDest.length > 0) {
      const durationInWantedPlace = matchedDest
        .map(dest => Number(TripMatch.getDaysFromISODuration(dest.duration)))
        .reduce((acc, duration) => acc + duration, 0);
      return durationInWantedPlace/totalDuration;
    } else {
      return 0.0;
    }
  }

  getPlaceToAvoidScore(place) {
    if (this.trip.name.includes(place)) {
      return 1.0;
    } else if (
      this.trip.countries.includes(place) ||
      this.trip.continents.includes(place) ||
      this.trip.destinations.find(d => d.name.includes(place))
    ) {
      return 1.0;
    } else if (
      this.trip.destinations
        .reduce((acc, dest) => (acc += dest.name), '')
        .includes(place)
    ) {
      return 1.0;
    } else {
      return 0.0;
    }
  }

  getSceneryScore(scenery) {
    return this.trip.naturalScenery.includes(scenery) ? 1.0 : 0.0;
  }

  getTravelPaceScore() {
    const duration = TripMatch.getDaysFromISODuration(this.trip.duration);
    if (duration) {
      const value = this.trip.destinations.length / duration;
      return value > 1 ? 1 : value;
    }
    return 0;
  }

  /**
   * @param devLevelArr
   * @returns {number}
   */
  static computeDevelopment(devLevelArr) {
    const devScore =
      uniq(devLevelArr).reduce((acc, developmentFactor) => {
        switch (developmentFactor) {
          case 'Isolated':
            return (acc += 0.0);
          case 'Small City':
          case 'Town':
            return (acc += 0.5);
          default:
            return (acc += 1.0);
        }
      }, 0) / devLevelArr.length;
    return isFinite(devScore) ? devScore : 0.0;
  }

  /**
   *
   * @param familiarity: Array<string>
   * @returns {number}
   */
  static computeFamiliarity(familiarity) {
    return (familiarity.reduce((acc, f) => acc + f, 0) / familiarity.length) / 100;
  }

  static getDaysFromISODuration(duration) {
    return toSeconds(parse(duration))/86400;
  }

  /**
   *i
   * @param its
   * @param ups
   * @param weights
   * @returns {number}
   */
  static getDistance(its, ups, weights) {
    return (
      Math.sqrt(
        sum(
          zip(its, zip(ups, weights)).map(([x, [y, w]]) => {
            const d = x - y;
            return d * d * w;
          })
        )
      ) || 0.0
    );
  }
}
