require('dotenv').config(); // eslint-disable-line
if (!('ListFormat' in Intl)) {
  require('intl-list-format');
  require('intl-list-format/locale-data/en');
}
/* eslint-disable import/first */
import React, { Suspense } from 'react';
// import 'react-dates/initialize';
import 'focus-visible';
import 'intro.js/introjs.css';
import { createBrowserHistory } from 'history';
import ReactDOM from 'react-dom';
import { syncHistoryWithStore } from 'mobx-react-router';
import { Route, Router } from 'react-router-dom';
import { Provider } from 'mobx-react';
import { ThemeProvider } from 'emotion-theming';
import './i18n';
import { theme } from './design-system';
import registerServiceWorker from './registerServiceWorker';

import { AppState, networkStatus } from './models';
import { MaxPanel, Text } from './design-system';
import Routes from './Routes';
/* eslint-enable import/first */

const appState = new AppState();
const browserHistory = createBrowserHistory();

const history = syncHistoryWithStore(browserHistory, appState.routing);

registerServiceWorker();

const render = (NextRoutes) => {
  return ReactDOM.render(
    <Provider app={appState} network={networkStatus}>
      <Suspense
        fallback={
          <MaxPanel>
            <Text>loading...</Text>
          </MaxPanel>
        }
      >
        <ThemeProvider theme={theme}>
          <Router history={history}>
            <Route path="/" component={NextRoutes} />
          </Router>
        </ThemeProvider>
      </Suspense>
    </Provider>,
    document.getElementById('root')
  );
};

render(Routes);

if (process.env.REACT_APP_ENABLE_HOT_RELOADING) {
  if (module.hot) {
    module.hot.accept('./Routes', () => {
      const NextRoutes = require('./Routes').default;
      render(NextRoutes);
    });
  }
}
