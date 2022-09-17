import { inject, observer } from 'mobx-react';
import { withRouter } from 'react-router';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import React from 'react';

import { Flex, Icon, Link, Text } from '../design-system';

export const Breadcrumbs = ({ t, tripCount, trip, routeElement, location }) => {
  const tripsText = `${t(
    'best_matching_trips',
    'Best Matching Trips'
  )} (${tripCount})`;
  const isVisible = tripCount > 0 && location.pathname !== '/';
  return isVisible ? (
    <Flex px={3} flex={1} lineHeight={1}>
      {trip ? (
        <Link nowrap bg="surface" borderRadius="top.sm" to="/trips">
          {tripsText}
        </Link>
      ) : (
        <Text p={2} nowrap>
          {tripsText}
        </Text>
      )}
      {trip && (
        <>
          <Icon name="arrow-right" mx={2} size={24} />
          {routeElement ? (
            <Link
              nowrap
              bg="surface"
              borderRadius="top.sm"
              to={`/trips/${trip.id}`}
            >
              {trip.name}
            </Link>
          ) : (
            <Text p={2} nowrap>
              {trip.name}
            </Text>
          )}
        </>
      )}
      {routeElement && (
        <>
          <Icon name="arrow-right" mx={2} size={24} />
          <Text p={2} nowrap>
            {routeElement.name}
          </Text>
        </>
      )}
    </Flex>
  ) : null;
};

export default compose(
  withRouter,
  withTranslation('common'),
  inject(({ app }) => ({
    tripCount: app.tripCount,
    trip: app.currentTrip,
    routeElement: app.selectedRouteElement,
  })),
  observer
)(Breadcrumbs);
