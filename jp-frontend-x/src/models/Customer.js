import { action, computed, decorate, observable, toJS } from 'mobx';
import Preferences from './Preferences';
import HubSpotTracker from './HubSpotTracker';
import ToggleState from './ToggleState';
import PersistentData from './PersistentData';
import { emailValid, lengthOk } from '../utils/strings';
import { getDefaultTripAccommodation } from '../utils/preferencesHelpers';
import uuid from 'uuid-random';
import { omit } from 'lodash';

const initialData = new Map([
  ['firstName', ''],
  ['lastName', ''],
  ['email', ''],
  ['optIn', false],
  ['tripAccommodations', {}],
  ['trips', {}],
]);

export default class Customer extends PersistentData {
  errors = new Map();
  infoDialog = new ToggleState('customer dialog');
  submissions = 0;
  preferences;
  hsq = new HubSpotTracker();
  isValid = false;
  app = null;

  constructor(app, props = {}) {
    const p = { name: 'jubel-customer', saveInterval: 5000, ...props };
    super(p);
    this.app = app;
    this.preferences = new Preferences({
      customer: this,
    });
    this.validate();
  }

  getInitialData() {
    return initialData;
  }

  get firstName() {
    return this.data.get('firstName');
  }

  get lastName() {
    return this.data.get('lastName');
  }

  get email() {
    return this.data.get('email');
  }

  get optIn() {
    return this.data.get('optIn');
  }

  get trips() {
    return this.data.get('trips');
  }

  setTripAccommodation = (tripId, accommodation) => {
    const tripAccommodations = {
      ...this.dataObject.tripAccommodations,
      [tripId]: accommodation,
    };
    this.update({ tripAccommodations });
  };

  get tripAccommodations() {
    return this.data.get('tripAccommodations');
  }

  getTripAccommodation = (tripId) => {
    let accommodation = this.tripAccommodations[tripId];
    if (!accommodation) {
      accommodation = getDefaultTripAccommodation(this.preferences, tripId);
    }
    return accommodation;
  };

  validate() {
    const errors = [];
    if (!lengthOk(this.firstName)) errors.push(['firstName', ['required']]);
    if (!lengthOk(this.lastName)) errors.push(['lastName', ['required']]);
    if (!lengthOk(this.email)) errors.push(['email', ['required']]);
    else if (!emailValid(this.email)) errors.push(['email', ['invalidEmail']]);
    console.log('validate', errors);
    if (errors.length > 0) {
      this.setErrors(errors);
      this.isValid = false;
    } else {
      this.clearErrors();
      this.isValid = true;
    }
  }

  send(trip) {
    console.log('Sending Preferences.');
    if (this.isValid) {
      const acc = this.getTripAccommodation(trip.id) || 'mid_range';
      this.preferences.generatePreferencesURL(acc).then((url) => {
        const trips = toJS(this.trips);
        if (trips[trip.id] !== url) {
          const tripId = trip.id + ''; // force string
          trips[tripId] = url;
          this.update({ trips });
          this.hsq.form(
            process.env.REACT_APP_CTA_FORM_ID,
            [
              {
                name: 'email',
                value: this.email,
              },
              {
                name: 'firstname',
                value: this.firstName,
              },
              {
                name: 'lastname',
                value: this.lastName,
              },
              {
                name: 'cta_trip_id',
                value: trip.id,
              },
              {
                name: 'cta_trip_name',
                value: trip.name,
              },
              {
                name: 'cta_trip_url',
                value: url,
              },
              {
                name: 'cta_submission_id',
                value: uuid(),
              },
              { name: 'cta_short_trip_url', value: window.location.href },
            ],
            { pageUri: '/get-quote', pageName: 'Get Quote' }
          );
          this.submissions += 1;
        }
      });
      return true;
    } else {
      return false;
    }
  }

  trackPreferences() {
    const title = document.title;
    const prefsData = this.preferences.dataObject;
    const custData = this.dataObject;
    const obj = omit(Object.assign({}, custData, prefsData), [
      'id',
      'timestamp',
      'userId',
    ]);
    this.hsq
      .identify(obj)
      .setPath(`/update-prefs/${prefsData.id}`, 'Update Preferences')
      .trackPageView();
    this.submissions += 1;
    document.title = title;
  }
}

decorate(Customer, {
  data: observable,
  errors: observable,
  infoDialog: observable,
  submissions: observable,
  preferences: observable,
  firstName: computed,
  lastName: computed,
  email: computed,
  optIn: computed,
  trips: computed,
  tripAccommodations: computed,
  validate: action,
  setTripAccommodation: action,
});
