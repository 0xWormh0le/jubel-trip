import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import DonutChart from '../design-system/DonutChart';
import FlexPanel from '../design-system/FlexPanel';
import TravelDistribution from '../models/TravelDistribution';
import TripProfileTable from './TripProfileTable';

const ScorableAlternativeProfile = ({ element, t }) => {
  const data = new TravelDistribution(element.travelStyles);
  return (
    <FlexPanel vscrollable flex={1}>
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
      <TripProfileTable trip={element} />
    </FlexPanel>
  );
};

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    element: app.selectedRouteElement,
  })),
  observer
)(ScorableAlternativeProfile);
