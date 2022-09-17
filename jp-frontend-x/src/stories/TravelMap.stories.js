/* eslint-disable import/first */
import React from 'react';
import { Provider } from 'mobx-react';
import { Router } from 'react-router-dom';
import { storiesOf } from '@storybook/react';
import { syncHistoryWithStore } from 'mobx-react-router';
import createBrowserHistory from 'history/createBrowserHistory';

import TravelMap from '../components/TravelMap';
import AppState from '../models/AppState';

const appState = new AppState();
const browserHistory = createBrowserHistory();
const history = syncHistoryWithStore(browserHistory, appState.routing);

storiesOf('TravelMap', module)
  .addDecorator((story) => (
    <Provider app={appState}>
      <Router history={history}>{story()}</Router>
    </Provider>
  ))
  .add('default', () => <TravelMap />, {
    notes: {
      markdown: `
### TravelMap Component

**Resolves**
[Issue#95](https://github.com/Jubel-co/jp-frontend/issues/95)`,
    },
  });
