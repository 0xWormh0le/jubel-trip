import { action, observable, decorate } from 'mobx';

import TabsState from './TabsState';
import ToggleState from './ToggleState';

export default class Route {
  panelExpandedToggle = new ToggleState({
    name: 'route panel',
    open: true,
  });

  countryProfilePanelToggle = new ToggleState();

  countryTabsState = new TabsState([]);

  summaryToggle = new ToggleState('route summary');

  setCountryTabsState = (countryCode, trip) => {
    const tripCountries = trip.destinations.reduce((acc, item) => {
      acc[item.countryCode] = item.countryName;
      return acc;
    }, {});
    this.countryTabsState = new TabsState(
      Object.keys(tripCountries).map((key) => ({
        id: key,
        name: tripCountries[key],
      })),
      countryCode
    );
  };
}

decorate(Route, {
  summary: observable,
  daily: observable,
  panelExpandedToggle: observable,
  countryTabsState: observable,
  countryProfilePanelToggle: observable,
  summaryToggle: observable,
  setCountryTabsState: action,
});
