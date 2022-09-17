import { observable, computed, decorate } from 'mobx';

export class TripList {
  trips = [];

  get count() {
    return this.trips.length;
  }
}

decorate(TripList, {
  trips: observable,
  count: computed,
});
