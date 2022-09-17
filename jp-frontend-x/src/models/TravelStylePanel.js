import { action, observable, decorate } from 'mobx';

import { travelStylesByAbbr } from './TravelStyle';

export default class TravelStylePanelStore {
  activeStyle = travelStylesByAbbr.oc;
  isOpen = false;

  constructor(props = { activeStyle: 'oc' }) {
    this.setActiveStyle(props.activeStyle);
  }

  setActiveStyle(abbr) {
    if (travelStylesByAbbr.hasOwnProperty(abbr)) {
      this.activeStyle = travelStylesByAbbr[abbr];
    } else {
      console.error(
        `TravelStylePanelState.setActiveStyle() called with unknown style key '${abbr}'`
      );
    }
  }

  open(abbr) {
    if (abbr) this.setActiveStyle(abbr);
    this.isOpen = true;
  }

  toggle() {
    this.isOpen = !this.isOpen;
  }
}

decorate(TravelStylePanelStore, {
  activeStyle: observable,
  isOpen: observable,
  setActiveStyle: action,
  open: action,
  toggle: action,
});
