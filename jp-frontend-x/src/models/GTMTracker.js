import Tracker from './Tracker';

const opt = (obj, key, variable, type) => {
  if (typeof variable === type) {
    switch (type) {
      case 'string':
        const v = variable.trim();
        if (v !== '') obj[key] = v;
        break;
      case 'number':
        obj[key] = variable;
        break;
      default:
        break;
    }
  }
};

export default class GTMTracker extends Tracker {
  containerID;

  appName = 'Unknown';
  appVersion = 'Unknown';

  constructor(props = {}) {
    super({ name: 'dataLayer', ...props });
    if (props.containerID) this.containerID = props.containerID;
    if (props.appName) this.appName = props.appName;
    if (props.appVersion) this.appVersion = props.appVersion;
    this.addData([{ appName: this.appName, appVersion: this.appVersion }]);
  }

  addData(obj) {
    this.push([obj]);
  }

  setExperiment(expId, expVar) {
    this.addData({ expId, expVar });
  }

  pageview(pageOpt) {
    const page = pageOpt || this._root('document.location.pathname');
    this.addData({ page });
    this.addData({ event: 'pageview' });
  }

  screenview(screenName) {
    const obj = {
      event: 'screenview',
      screenName,
    };
    this.addData(obj);
  }

  timing(timingCategory, timingVar, timingValue, timingLabelOpt) {
    const obj = {
      event: 'timing',
      timingCategory,
      timingVar,
      timingValue,
    };
    opt(obj, 'timingLabel', timingLabelOpt, 'string');
    this.addData(obj);
  }

  event(eventCategory, eventAction, eventLabel, eventValue) {
    const obj = { event: 'event', eventCategory, eventAction };
    opt(obj, 'eventLabel', eventLabel, 'string');
    opt(obj, 'eventValue', eventValue, 'number');
    this.addData(obj);
  }

  social(socialNetwork, socialAction, socialTarget) {
    const obj = { event: 'social', socialNetwork, socialAction, socialTarget };
    this.addData(obj);
  }

  exception(exDescription, exFatal = false) {
    const obj = { event: 'exception', exDescription, exFatal };
    this.addData(obj);
  }
}
