/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';
import { action } from '@storybook/addon-actions';
import { Provider, observer } from 'mobx-react';
import { withRouter } from 'react-router';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import { MaxPanel } from '../design-system';
import { MainHeader } from '../components/MainHeader';
import { locales } from '../models/AppState';
import { travelStyles } from '../models/TravelStyle';

const TranslatedMainHeader = compose(
  withRouter,
  withTranslation('common'),
  observer
)(MainHeader);

const appState = {
  tripCount: 10,
};

const network = {
  networkUp: true,
  pendingQueries: false,
  nextPingTime: 0,
  checkAgain: false,
  canRetry: false,
};

storiesOf('MainHeader', module)
  .addDecorator((story) => (
    <Provider app={appState} network={network}>
      <MaxPanel centered height="100vh">
        {story()}
      </MaxPanel>
    </Provider>
  ))
  .add('default', () => (
    <TranslatedMainHeader
      locales={locales}
      travelStyles={travelStyles}
      goto={action('goto')}
      setActiveTravelStyle={action('setActiveTravelStyle')}
      toggleTrips={action('toggleTrips')}
      hasTrips
      headerHeight={60}
    />
  ))
  .add('searchScreen', () => (
    <TranslatedMainHeader
      locales={locales}
      travelStyles={travelStyles}
      goto={action('goto')}
      setActiveTravelStyle={action('setActiveTravelStyle')}
      toggleTrips={action('toggleTrips')}
      hasTrips
      headerHeight={60}
      isSearchScreen
    />
  ));
