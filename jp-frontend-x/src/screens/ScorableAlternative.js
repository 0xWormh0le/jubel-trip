import React from 'react';
import { inject } from 'mobx-react';
import { Redirect } from 'react-router-dom';

import { isLoadingTrip } from '../utils/networkStatusHelpers';

const getTripHelper = (getTripById, tripId) =>
  tripId ? getTripById(Number(tripId)) : null;

class ScorableAlternative extends React.Component {
  componentDidMount() {
    const { selectDestination, trip, fetchTripById, match } = this.props;
    const { tripId, destinationId } = match.params;
    if (trip) {
      selectDestination(Number(tripId), Number(destinationId));
    } else {
      const { tripId } = match.params;
      if (tripId) {
        fetchTripById(tripId);
      }
    }
  }

  componentDidUpdate(prevProps) {
    const { match, selectDestination, trip } = this.props;
    const { tripId, destinationId } = match.params;
    if (trip !== prevProps.trip) {
      selectDestination(Number(tripId), Number(destinationId));
    }
  }

  componentWillUnmount() {
    const { close } = this.props;
    close();
  }

  render() {
    const { trip, pendingQueries } = this.props;
    return trip !== null || isLoadingTrip(pendingQueries) ? null : (
      <Redirect to="/trips" />
    );
  }
}

export default inject(({ app, network }, { match }) => ({
  fetchTripById: app.preferences.fetchTripById,
  pendingQueries: network.pendingQueries,
  selectDestination: app.selectDestination,
  trip: getTripHelper(app.preferences.getTripById, match.params.tripId),
  close: app.routeElementToggle.close,
}))(ScorableAlternative);
