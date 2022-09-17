import Tracker from './Tracker';

export default class GoogleAnalyticsTracker extends Tracker {
  constructor(props = {}) {
    super(Object.assign({ name: 'dataLayer' }, props));
    this.gtag('js', new Date());
    this.gtag('config', 'UA-79781689-2');
  }
  gtag() {
    this.push(arguments);
  }
}
