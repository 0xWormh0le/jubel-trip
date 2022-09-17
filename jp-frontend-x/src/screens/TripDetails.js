import React from 'react';
import { inject } from 'mobx-react';
import { Redirect } from 'react-router-dom';
import fp from 'lodash/fp';

import { isLoadingTrip, isLoadingTrips } from '../utils/networkStatusHelpers';
import withTutorial from '../hocs/withTutorial';

const getTripHelper = (getTripById, tripId) =>
  tripId ? getTripById(Number(tripId)) : null;

const tutorialData = [
  {
    selector: '.route-element:first-child',
    intro:
      'Click into the country or the destinations to learn more about them',
    position: 'right',
  },
  {
    selector: '#trip_panel_tabs button:nth-child(2)',
    intro:
      'Find out what activities or tours are available, the trip’s style, the culture shocks you should expect, and more',
  },
  {
    selector: '#trip_panel_tabs button:nth-child(3)',
    intro: 'See what’s included in the trip’s estimated price here',
  },
  {
    selector: '#cta_get_quote',
    intro:
      'Get connected 1-on-1 with a Jubel travel expert to easily customize and book this or a similar trip for you',
    position: 'right',
  },
];

class TripDetails extends React.Component {
  componentDidMount() {
    const {
      setCurrentTrip,
      trip,
      fetchTripById,
      match,
      togglePreferences,
      startTutorial,
    } = this.props;
    togglePreferences(false);
    if (trip) {
      setCurrentTrip(trip);
      setTimeout(() => startTutorial(), 2000);
    } else {
      const { tripId } = match.params;
      if (tripId) {
        fetchTripById(tripId);
      }
    }
  }

  componentDidUpdate(prevProps) {
    const { setCurrentTrip, trip, pendingQueries, startTutorial } = this.props;
    const loading = isLoadingTrips(pendingQueries);
    const prevLoading = isLoadingTrips(prevProps.pendingQueries);
    // hack, wait until both API calls are finished if present, to fix choppy animation.
    if (
      trip &&
      !loading &&
      (trip !== prevProps.trip || loading !== prevLoading)
    ) {
      setCurrentTrip(trip);
      setTimeout(() => startTutorial(), 2000);
    }
  }

  render() {
    const { trip, pendingQueries } = this.props;
    return trip !== null || isLoadingTrip(pendingQueries) ? null : (
      <Redirect to="/trips" />
    );
  }
}

export default fp.compose(
  withTutorial('tripDetails', tutorialData),
  inject(({ app, network }, { match }) => ({
    fetchTripById: app.preferences.fetchTripById,
    pendingQueries: network.pendingQueries,
    trip: getTripHelper(app.preferences.getTripById, match.params.tripId),
    setCurrentTrip: app.setCurrentTrip,
    togglePreferences: app.preferences.toggle,
  }))
)(TripDetails);
