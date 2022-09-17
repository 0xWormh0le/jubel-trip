import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withTranslation } from 'react-i18next';

import { FlexPanel, MaxPanel } from '../design-system';
import { TripCard } from './';

export const TripList = ({ trips, width }) => (
  <MaxPanel bg="background" width={width} id="trip_list">
    <FlexPanel vscrollable p={3}>
      {trips.map((trip, key) => (
        <TripCard key={key} trip={trip} mb={3} />
      ))}
    </FlexPanel>
  </MaxPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    trips: app.trips,
    tripCount: app.tripCount,
    togglePrefs: () => app.preferences.toggler.toggle(),
    prefsOpen: app.preferences.toggler.isOpen,
  })),
  observer
)(TripList);
