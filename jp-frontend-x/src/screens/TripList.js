import React from 'react';
import { inject } from 'mobx-react';
import fp from 'lodash/fp';

import withTutorial from '../hocs/withTutorial';

const tutorialData = [
  {
    selector: '[data-panel-id="slp-preferences"]',
    intro:
      'Hi! Play with your travel preferences here, we have many options so simply use the ones most important to you. We’ll immediately start recommending extraordinary trips that match what you are looking for',
  },
  {
    selector: '#trip_list',
    intro:
      'These are the ten trips that best match your preferences out of thousands available, click into them to learn more',
    position: 'right',
  },
];

class TripList extends React.Component {
  componentDidMount() {
    const {
      setTripsMode,
      togglePreferences,
      trips,
      startTutorial,
    } = this.props;
    if (trips.length > 0) {
      setTripsMode();
      togglePreferences(true);
      setTimeout(() => startTutorial(), 2000);
    }
  }

  componentDidUpdate(prevProps) {
    const { setTripsMode, trips, startTutorial } = this.props;
    if (trips !== prevProps.trips && trips.length > 0) {
      setTripsMode();
      setTimeout(() => startTutorial(), 2000);
    }
  }

  render() {
    return null;
  }
}

export default fp.compose(
  withTutorial('tripList', tutorialData),
  inject(({ app }) => ({
    headerHeight: app.headerHeight,
    isPrefsLoaded: app.preferences.isLoaded,
    prefsWidth: app.prefsWidth,
    setTripsMode: app.setTripsMode,
    togglePreferences: app.preferences.toggle,
    tripListPanelWidth: app.tripListPanelWidth,
    trips: app.trips,
  }))
)(TripList);
