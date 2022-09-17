import networkStatus from './NetworkStatus';
import { action, computed, observable, decorate } from 'mobx';
import {
  differenceInCalendarDays,
  isDate,
  isValid as isValidDate,
} from 'date-fns';
import isArray from 'lodash/isArray';
import isNil from 'lodash/isNil';
import omitBy from 'lodash/omitBy';
import uuid from 'uuid-random';

import PersistentData from './PersistentData';
import ToggleState from './ToggleState';
import {
  filterOptions,
  getSortedOptionsFromResponse,
  accommodations,
  scenery,
} from '../utils/preferencesHelpers';
import {
  parseQueryString,
  updateQueryStringParameter,
} from '../utils/queryHelpers';
const codec = require('json-url')('lzw');

const initialData = new Map([
  ['id', uuid()],
  ['userId', uuid()],
  ['timestamp', new Date()],
  ['accommodations', []],
  ['activities', []],
  ['budget', null],
  ['datesFlexible', true],
  ['departOnOrAfterDate', null],
  ['development', 50],
  ['durationInDays', null],
  ['familiarity', 50],
  ['placesToAvoid', []],
  ['placesToSee', []],
  ['returnOnOrBeforeDate', null],
  ['scenery', []],
  ['searchText', ''],
  ['shocks_crime', 0],
  ['shocks_infrastructure', 0],
  ['shocks_food', 0],
  ['shocks_language', 0],
  ['styles_an', 50],
  ['styles_cn', 50],
  ['styles_cu', 50],
  ['styles_oc', 50],
  ['styles_pp', 50],
  ['travelPace', 50],
  [
    'switches',
    {
      styles_an: false,
      styles_cn: false,
      styles_cu: false,
      styles_oc: false,
      styles_pp: false,
      travelPace: false,
      development: false,
      familiarity: false,
      shocks_crime: false,
      shocks_infrastructure: false,
      shocks_food: false,
      shocks_language: false,
    },
  ],
]);

export default class Preferences extends PersistentData {
  errors = new Map();
  locale;
  inProgress = false;
  toggler = new ToggleState('prefs');
  togglerName = `jubel-prefs-toggler`;
  showTips = true;
  datesPickerToggle = new ToggleState('dates-picker');
  datesPickerRefElement = null;
  matchedTrips = [];
  randomTrips = [];
  singleTrip;
  customer;
  openTriggered = false;
  closeTriggered = false;
  accommodationsOptions = [];
  asyncOptions = {
    activities: [],
    placesToSee: [],
  };

  constructor(props = {}) {
    const p = {
      name: 'jubel-prefs',
      locale: 'en-US',
      ...props,
    };
    super(p);
    this.locale = p.locale;
    if (p.customer) this.customer = p.customer;
    this.loadTogglerState();
    this.preloadAccommodationOptions();
  }

  getInitialData() {
    return initialData;
  }

  getTripById = (tripId) => {
    return (
      this.matchedTrips.find((trip) => Number(trip.id) === Number(tripId)) ||
      this.singleTrip
    );
  };

  fetchTripById = (tripId) => {
    const queryId = uuid();
    networkStatus.addQuery(queryId, 'tripDetails');
    fetch(`${process.env.REACT_APP_API_URL}/trips/${tripId}`, {
      method: 'get',
      headers: { 'Content-Type': 'application/json' },
    })
      .then((res) => res.json())
      .then((r) => {
        if (this.matchedTrips.length === 0) {
          this.matchedTrips = [r];
        }
        this.singleTrip = r;
      })
      .catch((e) => {
        this.singleTrip = null;
      })
      .finally(() => {
        networkStatus.removeQuery(queryId);
      });
  };

  fetchTrips = (searchValue) => {
    if (searchValue) {
      if (searchValue !== this.dataObject.placesToSee.value) {
        this.update({
          placesToSee: [
            {
              label: searchValue,
              value: searchValue,
            },
          ],
        });
      }
    } else {
      if (this.matchedTrips.length > 0) {
        // we already have fetched matched trips no need to reload.
        return;
      }
    }

    this.send().then(() => {
      if (this.matchedTrips.length > 0) {
        const routing = this.customer.app.routing;
        if (routing.location.pathname === '/' && this.isLoaded) {
          this.customer.app.goto('/trips');
        }
      }
    });
  };

  fetchRandomTrips = () => {
    const queryId = uuid();
    networkStatus.addQuery(queryId, 'randomTrips');
    fetch(`${process.env.REACT_APP_API_URL}/trips`, {
      method: 'get',
      headers: { 'Content-Type': 'application/json' },
    })
      .then((res) => res.json())
      .then((r) => {
        this.randomTrips = r;
      })
      .catch((e) => {
        console.error(e);
      })
      .finally(() => {
        networkStatus.removeQuery(queryId);
      });
  };

  resetMatchedTrips = () => {
    this.matchedTrips = [];
  };

  setLocale(locale) {
    this.locale = locale;
  }

  setCustomer(customer) {
    this.customer = customer;
  }

  formatDate(
    value,
    options = {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }
  ) {
    return new Intl.DateTimeFormat(this.locale, options).format(value);
  }

  get datesPickerOpen() {
    return this.datesPickerToggle.isOpen;
  }

  openDatesPicker(element) {
    this.datesPickerRefElement = element;
    this.datesPickerToggle.open();
  }

  closeDatesPicker() {
    this.datesPickerRefElement = null;
    this.datesPickerToggle.close();
  }

  get datesPickerPosition() {
    const inputOffset = 8;
    const rect = (
      this.datesPickerRefElement || document.body
    ).getBoundingClientRect();
    return {
      inputOffset,
      input: rect,
      top: rect.bottom + inputOffset,
      right: window.innerWidth - rect.right - inputOffset,
      screenWidth: window.innerWidth,
    };
  }

  get duration() {
    console.log('enter duration');
    if (this.datesValid) {
      console.log('dates valid');
      return (
        differenceInCalendarDays(
          this.returnOnOrBeforeDate,
          this.departOnOrAfterDate
        ) + 1
      );
    }
    console.log('dates invalid');
    return null;
  }

  get datesValid() {
    console.log('datesValid?', {
      departOnOrAfterDate: this.departOnOrAfterDate,
      returnOnOrBeforeDate: this.returnOnOrBeforeDate,
      isDateDepart: isDate(this.departOnOrAfterDate),
      isDateReturn: isDate(this.returnOnOrBeforeDate),
      // isValidDepart: isValidDate(this.departOnOrAfterDate),
      // isValidReturn: isValidDate(this.returnOnOrBeforeDate),
    });
    return (
      isDate(this.departOnOrAfterDate) &&
      isDate(this.returnOnOrBeforeDate) &&
      isValidDate(this.departOnOrAfterDate) &&
      isValidDate(this.returnOnOrBeforeDate)
    );
  }

  get dateRangeStrings() {
    const d = this.duration;
    if (d > 0) {
      return {
        days: d,
        nights: d - 1,
        startDate: this.departOnOrAfterDate,
        endDate: this.returnOnOrBeforeDate,
      };
    }
    return { days: 0, nights: 0, startDate: '', endDate: '' };
  }

  setDateRange = (from, to) => {
    this.update({
      departOnOrAfterDate: from,
      returnOnOrBeforeDate: to,
    });
    this.update({
      durationInDays: this.dateRangeStrings.days,
    });
  };

  clearDateRange = () => {
    this.update({
      departOnOrAfterDate: null,
      returnOnOrBeforeDate: null,
      durationInDays: null,
    });
  };

  get id() {
    return this.data.get('id');
  }

  get userId() {
    return this.data.get('userId');
  }

  get timestamp() {
    return this.data.get('timestamp');
  }

  get accommodations() {
    return this.data.get('accommodations');
  }

  get activities() {
    return this.data.get('activities');
  }

  get budget() {
    return this.data.get('budget');
  }

  get datesFlexible() {
    return this.data.get('datesFlexible');
  }

  get departOnOrAfterDate() {
    return this.data.get('departOnOrAfterDate');
  }

  get development() {
    return this.data.get('development');
  }

  get familiarity() {
    return this.data.get('familiarity');
  }

  get placesToAvoid() {
    return this.data.get('placesToAvoid');
  }

  get placesToSee() {
    return this.data.get('placesToSee');
  }

  get returnOnOrBeforeDate() {
    return this.data.get('returnOnOrBeforeDate');
  }

  get scenery() {
    return this.data.get('scenery');
  }

  get searchText() {
    return this.data.get('searchText');
  }

  get shocks_crime() {
    return this.data.get('shocks_crime');
  }

  get shocks_language() {
    return this.data.get('shocks_language');
  }

  get shocks_food() {
    return this.data.get('shocks_food');
  }

  get shocks_infrastructure() {
    return this.data.get('shocks_infrastructure');
  }

  get styles_cu() {
    return this.data.get('styles_cu');
  }

  get styles_pp() {
    return this.data.get('styles_pp');
  }

  get styles_an() {
    return this.data.get('styles_an');
  }

  get styles_cn() {
    return this.data.get('styles_cn');
  }

  get styles_oc() {
    return this.data.get('styles_oc');
  }

  get travelPace() {
    return this.data.get('travelPace');
  }

  get switches() {
    return this.data.get('switches');
  }

  get travelStyles() {
    return {
      an: this.styles_an,
      cn: this.styles_cn,
      cu: this.styles_cu,
      oc: this.styles_oc,
      pp: this.styles_pp,
    };
  }

  get enabledTravelStyles() {
    const switches = this.switches;
    return omitBy(
      {
        an: switches['styles_an'] ? this.styles_an : undefined,
        cn: switches['styles_cn'] ? this.styles_cn : undefined,
        cu: switches['styles_cu'] ? this.styles_cu : undefined,
        oc: switches['styles_oc'] ? this.styles_oc : undefined,
        pp: switches['styles_pp'] ? this.styles_pp : undefined,
      },
      isNil
    );
  }

  get travelShocks() {
    return {
      food: this.shocks_food,
      language: this.shocks_language,
      crime: this.shocks_crime,
      infrastructure: this.shocks_infrastructure,
    };
  }

  get enabledTravelShocks() {
    const switches = this.switches;
    return omitBy(
      {
        food: switches['shocks_food'] ? this.shocks_food : undefined,
        language: switches['shocks_language']
          ? this.shocks_language
          : undefined,
        crime: switches['shocks_crime'] ? this.shocks_crime : undefined,
        infrastructure: switches['shocks_infrastructure']
          ? this.shocks_infrastructure
          : undefined,
      },
      isNil
    );
  }

  toggleTips = () => {
    this.showTips = !this.showTips;
  };

  toggle = (isOpen) => {
    this.toggler.toggle(isOpen);
  };

  get open() {
    return this.toggler.isOpen;
  }

  preloadAccommodationOptions = async () => {
    this.accommodationsOptions = await this.loadOptions('accommodations')('');
  };

  fetchAsyncOptions = async (kind) => {
    const queryId = uuid();
    networkStatus.addQuery(queryId, kind);
    try {
      let res = await fetch(`${process.env.REACT_APP_API_URL}/${kind}`, {
        method: 'get',
        headers: { 'Content-Type': 'application/json' },
      });
      res = await res.json();
      this.asyncOptions[kind] = getSortedOptionsFromResponse(res[kind]);
    } catch (e) {
      this.asyncOptions[kind] = [];
    } finally {
      networkStatus.removeQuery(queryId);
    }
    return this.asyncOptions[kind];
  };

  loadAsyncOptions = (kind) => async (inputValue) => {
    if (!this.asyncOptions[kind] || !this.asyncOptions[kind].length) {
      await this.fetchAsyncOptions(kind);
    }
    return filterOptions(this.asyncOptions[kind], inputValue);
  };

  loadOptions = (kind) => {
    if (kind === 'placesToAvoid' || kind === 'placesToSee') {
      return this.loadAsyncOptions('placesToSee');
    } else if (kind === 'activities') {
      return this.loadAsyncOptions('activities');
    }

    const options =
      {
        scenery,
        accommodations,
      }[kind] || [];
    return (inputValue) =>
      new Promise((resolve) => resolve(filterOptions(options, inputValue)));
  };

  getValues(obj) {
    return isArray(obj) ? obj.map((x) => x.value) : obj.value;
  }

  fromSavedObject(obj) {
    ['departOnOrAfterDate', 'returnOnOrBeforeDate'].forEach((k) => {
      if (obj[k]) {
        // console.log(k, typeof obj[k]);
        obj[k] = new Date(obj[k]);
        // console.log(
        //   `after ${k}`,
        //   obj[k] instanceof Date,
        //   isDate(obj[k]),
        //   isValidDate(obj[k])
        // );
      }
    });
    // console.log('fromSavedObject', obj);
    return obj;
  }

  loadTogglerState() {
    const json = localStorage.getItem(this.togglerName);
    const isTripDetailsScreen = /trips\/(\d+)$/.test(window.location.pathname);
    if (isTripDetailsScreen) {
      return;
    }
    if (isNil(json)) {
      this.toggler.toggle(true);
    } else {
      this.toggler.toggle(Boolean(JSON.parse(json)));
    }
  }

  async extractPreferencesFromURL(url) {
    const { preferences: encodedPreferences } = parseQueryString(url);
    return codec.decompress(encodedPreferences);
  }

  load() {
    const hasPrefsInURL = Object.keys(
      parseQueryString(this._root('location.search'))
    ).includes('preferences');

    if (hasPrefsInURL) {
      this.extractPreferencesFromURL(this._root('location.search')).then(
        (extracted) => {
          console.log('Extracted Prefs', extracted);
          super.load(extracted);
        }
      );
    } else {
      super.load();
    }
  }

  updateSwitches = (data) => {
    this.update({
      switches: {
        ...this.switches,
        ...data,
      },
    });
  };

  toggleVisible = (value) => {
    this.toggler.toggle(value);
    if (this.toggler.isOpen) {
      this.openTriggered = true;
    } else {
      this.closeTriggered = true;
    }
    localStorage.setItem(this.togglerName, JSON.stringify(this.toggler.isOpen));
  };

  async generatePreferencesURL(accommodation) {
    const data = this.dataObject;
    data.accommodation = accommodation || 'mid_range';
    const compressed = await codec.compress(data);
    return encodeURI(
      updateQueryStringParameter(
        this._root('location.href'),
        'preferences',
        compressed
      )
    );
  }

  save() {
    const wasMutated = this.mutations.size > 0;
    if (wasMutated) {
      const id = uuid();
      this.data.set('id', id);
    }

    super.save();

    if (wasMutated) {
      // this.customer.send();
      this.send();
    }
  }

  clearAll = () => {
    this.data = this.getInitialData();
    this.save();
  };

  send = () => {
    const data = this.dataObject;
    const queryId = data.id;
    networkStatus.addQuery(queryId, 'matchedTrips');
    Object.keys(this.switches).forEach((key) => {
      if (!this.switches[key]) {
        delete data[key];
      }
    });
    data.travelShocks = this.enabledTravelShocks;
    data.travelStyles = this.enabledTravelStyles;
    ['an', 'cn', 'cu', 'oc', 'pp'].forEach((s) => delete data[`styles_${s}`]);
    ['crime', 'food', 'infrastructure', 'language'].forEach(
      (s) => delete data[`shocks_${s}`]
    );
    if (isNil(this.budget)) {
      delete data.budget;
    }
    delete data.switches;
    console.warn('sending', data);
    this.customer.trackPreferences();
    return fetch(`${process.env.REACT_APP_API_URL}/trips`, {
      method: 'post',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    })
      .then((res) => res.json())
      .then((r) => {
        this.matchedTrips = r;
      })
      .catch((e) => {
        console.error(e);
      })
      .finally(() => {
        networkStatus.removeQuery(queryId);
      });
  };
}

decorate(Preferences, {
  data: observable,
  errors: observable,
  locale: observable,
  inProgress: observable,
  openTriggered: observable,
  closeTriggered: observable,
  toggler: observable,
  showTips: observable,
  datesPickerToggle: observable,
  datesPickerRefElement: observable,
  departOnOrAfterDateInput: observable,
  returnOnOrBeforeDateInput: observable,
  matchedTrips: observable,
  randomTrips: observable,
  singleTrip: observable,
  setLocale: action,
  datesPickerOpen: computed,
  openDatesPicker: action,
  closeDatesPicker: action,
  datesPickerPosition: computed,
  datesValid: computed,
  dateRangeStrings: computed,
  setDateRange: action,
  clearDateRange: action,
  userId: computed,
  id: computed,
  timestamp: computed,
  accommodations: computed,
  activities: computed,
  budget: computed,
  datesFlexible: computed,
  departOnOrAfterDate: computed,
  development: computed,
  familiarity: computed,
  placesToSee: computed,
  placesToAvoid: computed,
  returnOnOrBeforeDate: computed,
  scenery: computed,
  searchText: computed,
  shocks_food: computed,
  shocks_crime: computed,
  shocks_language: computed,
  shocks_infrastructure: computed,
  styles_an: computed,
  styles_cn: computed,
  styles_cu: computed,
  styles_oc: computed,
  styles_pp: computed,
  travelStyles: computed,
  travelShocks: computed,
  enabledTravelStyles: computed,
  enabledTravelShocks: computed,
  travelPace: computed,
  toggleTips: action,
  toggle: action,
  open: computed,
  load: action,
  accommodationsOptions: observable,
  activitiesOptions: observable,
  placesOptions: observable,
  updateSwitches: action,
  send: action,
  fetchTrips: action,
  fetchTripById: action,
  resetMatchedTrips: action,
  fetchRandomTrips: action,
  clearAll: action,
});
