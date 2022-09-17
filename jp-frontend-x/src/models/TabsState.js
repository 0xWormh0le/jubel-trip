import { action, computed, decorate, observable } from 'mobx';
import get from 'lodash/get';

export default class TabsState {
  _tabs = [];
  activeTab = '';

  constructor(tabs, activeTabId) {
    this._tabs = tabs;
    this.activeTab = activeTabId || get(tabs, '[0].id') || '';
  }

  setActiveTab(id) {
    this.activeTab = id;
  }

  get tabs() {
    return this._tabs.map(({ id, name }) => ({
      id,
      name,
      active: this.activeTab === id,
    }));
  }

  get activeTabName() {
    return (
      get(this._tabs.find((tab) => tab.id === this.activeTabId), 'name') || ''
    );
  }
}

decorate(TabsState, {
  activeTab: observable,
  setActiveTab: action,
  activeTabName: computed,
  tabs: computed,
});
