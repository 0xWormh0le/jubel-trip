import { observable, action, computed, decorate } from 'mobx';
import { notifications } from './Notification';
import id64 from 'id64';
import Tracker from './Tracker';

export default class User {
  id;

  email;

  fullName;

  displayName;

  loggedIn = false;

  notifications = [];

  trips = [];

  tracker = new Tracker();

  constructor(props) {
    [
      'id',
      'email',
      'fullName',
      'displayName',
      'loggedIn',
      'notifications',
      'trips',
    ].forEach((p) => {
      if (props.hasOwnProperty(p)) this[p] = props[p];
    });
    if (!this.id) {
      this.id = id64.gen();
    }
  }

  logout() {
    this.loggedIn = false;
    this.tracker.push({
      event: 'social',
      network: this.loggedInNetwork,
      action: 'logout',
      target: this.id,
    });
    this.loggedInNetwork = '';
  }

  login(network = 'email') {
    this.loggedIn = true;
    this.loggedInNetwork = network;
    this.tracker.push({
      event: 'social',
      network: this.loggedInNetwork,
      action: 'login',
      target: this.id,
    });
  }

  get name() {
    return this.displayName || this.fullName || this.email;
  }
}

// for testing
export const nextdude = new User({
  email: 'nextdude@gmail.com',
  fullName: 'Robert Lyons',
  displayName: 'nextdude',
  loggedIn: true,
  loggedInNetwork: 'email',
  notifications: Array.from(notifications),
});

decorate(User, {
  email: observable,
  fullName: observable,
  displayName: observable,
  loggedIn: observable,
  notifications: observable,
  trips: observable,
  logout: action,
  login: action,
  name: computed,
});
