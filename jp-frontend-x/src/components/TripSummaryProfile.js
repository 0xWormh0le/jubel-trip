import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import { DonutChart, FlexPanel, ImageSorter, Box } from '../design-system';
import { TripProfileTable } from '.';
import TravelDistribution from '../models/TravelDistribution';

export const TripSummaryProfile = ({ trip, t }) => {
  const data = new TravelDistribution(trip.travelStyles);
  return (
    <FlexPanel vscrollable flex={1}>
      <ImageSorter height={300} minHeight={300} images={trip.images} />
      <Box>
        <DonutChart
          t={t}
          startAngle={45}
          endAngle={-315}
          data={data.getData()}
          colors={data.getColors()}
          labelsMode="external"
          width={250}
          height={250}
          fontSize={2}
        />
      </Box>
      <TripProfileTable trip={trip} tripSummary />
    </FlexPanel>
  );
};

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    trip: app.currentTrip,
  })),
  observer
)(TripSummaryProfile);
