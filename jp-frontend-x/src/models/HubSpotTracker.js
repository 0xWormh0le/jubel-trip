import Tracker from './Tracker';
import { getCookie } from '../utils/cookies';

export default class HubSpotTracker extends Tracker {
  ipAddress = '';

  constructor(props = {}) {
    super(Object.assign({ name: '_hsq' }, props));
    this.setIpAddress();
  }

  async setIpAddress() {
    try {
      const response = await fetch('https://api.ipify.org/?format=json');
      const json = await response.json();
      this.ipAddress = json.ip;
    } catch (error) {
      console.error('Failed to get client IP address', error);
    }
  }

  identify(event) {
    this.push(['identify', event]);
    return this;
  }

  setPath(path) {
    if (path) {
      this.push(['setPath', path]);
    }
    return this;
  }

  trackPageView() {
    this.push(['trackPageView']);
    return this;
  }

  trackEvent(id, value) {
    this.push(['trackEvent', { id, value }]);
    return this;
  }

  async form(formGuid, fields, origContext) {
    const context = {
      hutk: getCookie('hubspotutk'),
      ipAddress: this.ipAddress,
      ...origContext,
    };
    const portalID = process.env.REACT_APP_PORTAL_ID;
    const url = `https://api.hsforms.com/submissions/v3/integration/submit/${portalID}/${formGuid}`;
    try {
      const response = await fetch(url, {
        method: 'POST',
        body: JSON.stringify({ fields, context }),
        headers: {
          'Content-Type': 'application/json',
        },
      });
      const json = await response.json();
      // console.log({ formGuid, fields, context, json });
      return json;
    } catch (e) {
      console.error('Error:', e);
    }
  }
}
