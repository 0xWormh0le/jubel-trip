import { observable, action, decorate } from 'mobx';

export default class Story {
  slidingPanelOpen = {
    leftPanel: false,
    rightPanel: false,
    nestedPanel: false,
    nonAnimatedPanel: false,
  };

  slidingPanelWidths = {
    leftPanel: null,
    nestedPanel: null,
  };

  openSlidingPanel = (panel) => {
    this.slidingPanelOpen[panel] = true;
  };

  closeSlidingPanel = (panel) => {
    this.slidingPanelOpen[panel] = false;
  };

  toggleSlidingPanel = (panel) => {
    this.slidingPanelOpen[panel] = !this.slidingPanelOpen[panel];
  };

  changeSlidingPanelWidth = (panel, width) => {
    this.slidingPanelWidths[panel] = width;
  };
}

decorate(Story, {
  slidingPanelOpen: observable,
  slidingPanelWidths: observable,
  openSlidingPanel: action,
  closeSlidingPanel: action,
  changeSlidingPanelWidth: action,
});
