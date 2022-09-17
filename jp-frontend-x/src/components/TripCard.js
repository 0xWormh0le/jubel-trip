import React, { useMemo } from 'react';
import { toJS } from 'mobx';
import { inject, observer } from 'mobx-react';
import { withRouter } from 'react-router-dom';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import fp from 'lodash/fp';
import PropTypes from 'prop-types';

import {
  Box,
  Card,
  Divider,
  Flex,
  FlexPanel,
  Heading5,
  ImageSorter,
  Text,
} from '../design-system';
import { durationToDays } from '../utils/date';
import { TravelStylesDistributionView } from './';

const shuffleArray = (arr) => arr.sort(() => Math.random() - 0.5);

const getAllImagesFromTrip = (trip) =>
  fp.compose(
    shuffleArray,
    fp.uniqBy('src'),
    fp.reduce(
      (images, destination) => [...images, ...toJS(destination.images || [])],
      toJS(trip.images) || []
    )
  )(trip.destinations || []);

export const TripCard = ({
  trip,
  t,
  setCurrentTrip,
  travelStyleLabels,
  history,
  mapIsInitialized,
  prefAccommodation,
  accommodationsOptions,
  ...cardProps
}) => {
  const tripImages = useMemo(() => getAllImagesFromTrip(trip), [trip]);
  const accommodationItem = accommodationsOptions.find(
    (item) => item.value === prefAccommodation
  );
  return (
    <Card {...cardProps}>
      <Flex>
        <Flex flex={1.5} height="auto">
          {mapIsInitialized && (
            <ImageSorter
              borderRadius="left.sm"
              images={tripImages}
              height="100%"
            />
          )}
        </Flex>
        <Box
          p={3}
          flex={2}
          hoverable
          onClick={() => history.push(`/trips/${trip.id}`)}
        >
          <FlexPanel>
            <FlexPanel justifyContent="space-around">
              <Heading5 m={0} textStyle="capitalize">
                {trip.name}
              </Heading5>
              <Text mt={1} mb={3} fontSize={3} uppercase>
                {t('list_format', {
                  list: trip.destinations.map((d) => d.name),
                })}
              </Text>
            </FlexPanel>
            <Flex justifyContent="space-between">
              <FlexPanel flex={1} alignItems="center">
                <Text fontSize={4} bold>
                  {t('dollar_amount', {
                    amount:
                      trip.accommodation[prefAccommodation] ||
                      trip.estimatedCost ||
                      'N/A',
                  })}
                </Text>
                <Text
                  pt={2}
                  fontSize={2}
                  color="misc.muted"
                  uppercase
                  textAlign="center"
                >
                  {t('per_person', 'per person')}
                </Text>
                <Text fontSize={4} bold mt={2}>
                  {accommodationItem && accommodationItem.label}
                </Text>
              </FlexPanel>
              <Divider vertical height={100} />
              <FlexPanel flex={1} alignItems="center">
                <Text fontSize={4} bold>
                  {durationToDays(trip.duration)}
                </Text>
                <Text pt={2} fontSize={2} uppercase color="misc.muted" center>
                  {t('day', { count: durationToDays(trip.duration) })}
                </Text>
              </FlexPanel>
              <Divider vertical height={100} />
              <Flex fontSize={2} justifyContent="center" flex={2}>
                <TravelStylesDistributionView values={trip.travelStyles} />
              </Flex>
            </Flex>
          </FlexPanel>
        </Box>
      </Flex>
    </Card>
  );
};

TripCard.propTypes = {
  trip: PropTypes.object.isRequired,
  t: PropTypes.func.isRequired,
};

const TripCardWrapper = (props) => <TripCard {...props} />;

export default compose(
  withRouter,
  withTranslation('common'),
  inject(({ app }, { trip }) => ({
    mapIsInitialized: app.map.initialized,
    accommodationsOptions: app.preferences.accommodationsOptions || [],
    prefAccommodation: app.customer.getTripAccommodation(trip.id),
  })),
  observer
)(TripCardWrapper);
