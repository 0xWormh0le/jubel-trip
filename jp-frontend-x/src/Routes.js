import React, { useEffect } from 'react';
import { inject, observer } from 'mobx-react';
import { Redirect, Route, Switch } from 'react-router-dom';
import compose from 'lodash/fp/compose';

import { GlobalStyles } from './design-system/theme';
import { parseQueryString } from './utils/queryHelpers';
import { TooltipWrapper } from './components';
import Layout from './components/Layout';
import Search from './screens/Search';
import Trips from './screens/Trips';

const Routes = ({
  fetchTrips,
  isPrefsLoaded,
  location,
  update,
  fetchRandomTrips,
}) => {
  useEffect(() => {
    const query = parseQueryString(location.search);
    const { q: searchValue } = query;
    fetchTrips(searchValue || null);
  }, [fetchTrips, isPrefsLoaded, location.search, update]);

  useEffect(() => {
    fetchRandomTrips();
  }, [fetchRandomTrips]);

  return (
    <>
      <TooltipWrapper />
      <GlobalStyles />
      <Layout>
        <Switch>
          <Route
            path="/trips"
            render={(routeProps) => <Trips key="trips" {...routeProps} />}
          />
          <Route exact path="/" component={Search} />
          <Redirect to="/" />
        </Switch>
      </Layout>
    </>
  );
};

export default compose(
  inject(({ app }) => ({
    isPrefsLoaded: app.preferences.isLoaded,
    fetchTrips: app.preferences.fetchTrips,
    fetchRandomTrips: app.preferences.fetchRandomTrips,
    update: app.preferences.update,
  })),
  observer,
  React.memo
)(Routes);
