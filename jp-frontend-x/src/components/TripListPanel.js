import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';

import { SlidingPanel } from '../design-system';
import { TripList } from './';

export const TripListPanel = ({ open, maxWidth }) => (
  <SlidingPanel id="trip-list" maxWidth={maxWidth} open={open} closeType="none">
    <TripList width={maxWidth} />
  </SlidingPanel>
);

export default compose(
  inject(({ app }) => ({
    open: app.isTripListPanelOpen,
    maxWidth: app.tripListPanelWidth,
  })),
  observer
)(TripListPanel);
