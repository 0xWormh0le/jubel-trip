import { action, observable, decorate } from 'mobx';
import ToggleState from './ToggleState';

export default class CountryProfilePanelState {
  toggle = new ToggleState('country profile');
  activeTab = 'profile';

  setActiveTab(tab) {
    this.activeTab = tab;
  }
}

decorate(CountryProfilePanelState, {
  toggle: observable,
  activeTab: observable,
  setActiveTab: action,
});
