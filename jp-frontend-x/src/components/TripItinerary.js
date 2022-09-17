import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withRouter } from 'react-router-dom';

import { Box, FlexPanel } from '../design-system';
import { DailyRouteElements, SummarizedRouteElements } from '.';

export const TripItinerary = ({ tripId, summary, daily, departureDate }) => (
  <FlexPanel bg="background" borderLeft={1} width={1} vscrollable flex={1}>
    <Box p={3} vscrollable hideHorizontalOverflow>
      {summary.map((element, index) => (
        <SummarizedRouteElements
          key={`summary-${index}`}
          isExpanded
          last={summary.length - 1 === index}
          tripId={tripId}
          destinationId={element.id}
          {...element}
        />
      ))}
      {daily.map(({ start, days, elements }, index) => (
        <DailyRouteElements
          departureDate={departureDate}
          key={`daily-${index}`}
          isExpanded
          start={start}
          days={days}
          last={daily.length - 1 === index}
          elements={elements}
          tripId={tripId}
        />
      ))}
    </Box>
  </FlexPanel>
);

export default compose(
  withRouter,
  inject(({ app }) => ({
    summary: app.routeSummary,
    daily: app.routeDaily,
    tripId: app.currentTrip.id,
    departureDate: app.preferences.departureDate,
  })),
  observer
)(TripItinerary);
