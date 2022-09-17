/* eslint-disable import/first */
import React from 'react';
import { Provider } from 'mobx-react';
import { Router } from 'react-router-dom';
import { storiesOf } from '@storybook/react';
import { syncHistoryWithStore } from 'mobx-react-router';
import createBrowserHistory from 'history/createBrowserHistory';

import AppState from '../models/AppState';
import Box from '../design-system/Box';
import TripList from '../components/TripList';

const appState = new AppState();
const browserHistory = createBrowserHistory();
const history = syncHistoryWithStore(browserHistory, appState.routing);

appState.map.initMap({ target: null });

storiesOf('TripList', module)
  .addDecorator((story) => (
    <Provider app={appState}>
      <Router history={history}>
        <Box mx="auto" width={600} p={3} height="100%" scrollable>
          {story()}
        </Box>
      </Router>
    </Provider>
  ))
  .add('default', () => <TripList maxWidth={600} />, {
    notes: {
      markdown: `
#### Trip List Application Component

**Resolves**
[PR#84](https://github.com/Jubel-co/jp-frontend/pull/84)`,
    },
  });
