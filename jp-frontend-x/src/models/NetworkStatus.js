import { action, observable, decorate } from 'mobx';

class NetworkStatus {
  networkUp = true;
  lastPingTime = 0; // a millisecond timestamp, should be set to the time of the last periodic check.
  pendingQueries = []; // for tracking outstanding queries sent to the backend for which we expect web-socket responses
  nextPingTime = 0; // seconds
  canRetry = true; // this could be more sophisticated, to avoid spam

  DEFAULT_PING_INTERVAL = 2; // seconds
  MAX_RETRIES = 3; // before emitting a "network down" status
  MAX_BACKOFF_SECONDS = 64;
  HIT_MAX = false;

  constructor() {
    this.healthCheck();
  }

  healthCheck(retries = 0, once = false) {
    this.emitPing()
      .then((_) => {
        this.networkUp = true;
        this.HIT_MAX = false;
        this.nextPingTime = 0;
        if (!once) {
          this.setLastAndNewPingTimes(this.DEFAULT_PING_INTERVAL).then(() =>
            this.healthCheck(1)
          );
        }
      })
      .catch((_) => {
        if (!once) {
          retries = retries > this.MAX_RETRIES && this.networkUp ? 0 : retries;
          const newInterval =
            this.HIT_MAX && !this.networkUp
              ? this.MAX_BACKOFF_SECONDS
              : this.getNewInterval(this.DEFAULT_PING_INTERVAL, retries);
          if (retries >= this.MAX_RETRIES) {
            this.networkUp = false;
            // To avoid excessively long intervals
            if (newInterval >= this.MAX_BACKOFF_SECONDS) {
              this.HIT_MAX = true;
            }
          }
          this.setLastAndNewPingTimes(newInterval)
            .then(() => this.healthCheck(retries + 1))
            .catch(() => null);
        }
      });
  }

  addQuery(id, type) {
    this.pendingQueries.push({ id, type });
  }

  removeQuery(id) {
    this.pendingQueries = this.pendingQueries.filter((item) => item.id !== id);
  }
  /**
   * Misc
   */
  removeAllQueries() {
    this.pendingQueries = [];
  }

  forceCheck() {
    if (this.canRetry) {
      this.canRetry = false;
      this.healthCheck(1, true);
      setTimeout(() => (this.canRetry = true), 2000);
    }
  }

  async calcNextPingTime(interval) {
    this.nextPingTime = interval;
    return new Promise((res, rej) => {
      const loop = () => {
        if (this.nextPingTime > 0) {
          this.nextPingTime -= 1;
          return setTimeout(() => loop(), 1000);
        } else {
          return res(true);
        }
      };
      return loop();
    });
  }

  async emitPing() {
    return new Promise((res, rej) => {
      if (navigator.onLine) {
        fetch(process.env.REACT_APP_API_URL, {
          headers: {
            'Cache-Control': 'no-cache',
            Pragma: 'no-cache',
          },
        })
          .then(() => res(true))
          .catch((_) => rej(false));
      } else {
        rej(false);
      }
    });
  }

  // helpers
  getNewInterval = (prevInterval, retries) =>
    Math.floor(prevInterval + Math.pow(2, retries) + Math.random());
  setLastAndNewPingTimes = async (interval) => this.calcNextPingTime(interval);
}

decorate(NetworkStatus, {
  networkUp: observable,
  lastPingTime: observable,
  pendingQueries: observable,
  nextPingTime: observable,
  canRetry: observable,
  healthCheck: action,
  addQuery: action.bound,
  removeQuery: action,
  removeAllQueries: action.bound,
  forceCheck: action.bound,
  calcNextPingTime: action,
});

export default new NetworkStatus();
