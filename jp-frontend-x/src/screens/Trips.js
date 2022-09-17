import React from 'react';
import { Route, Switch } from 'react-router-dom';
import { inject, observer } from 'mobx-react';
import compose from 'lodash/fp/compose';

import Screen from '../components/Screen';

import Country from './Country';
import ScorableAlternative from './ScorableAlternative';
import TripDetails from './TripDetails';
import TripList from './TripList';
import ChatWidget from '../models/HubSpotConversations';

const Trips = ({ mapIsInitialized }) => {
  return mapIsInitialized ? (
    <Screen name="trips">
      <ChatWidget />
      <Switch>
        <Route
          exact
          path="/trips"
          render={(routeProps) => <TripList {...routeProps} />}
        />
        <Route
          exact
          path="/trips/:tripId"
          render={(routeProps) => <TripDetails {...routeProps} />}
        />
        <Route
          exact
          path="/trips/:tripId/country/:countryCode"
          render={(routeProps) => <Country {...routeProps} />}
        />
        <Route
          exact
          path="/trips/:tripId/destination/:destinationId"
          render={(routeProps) => <ScorableAlternative {...routeProps} />}
        />
      </Switch>
    </Screen>
  ) : null;
};

export default compose(
  inject(({ app }) => ({
    mapIsInitialized: app.map.initialized,
  })),
  observer,
  React.memo
)(Trips);
