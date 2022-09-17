import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import fp from 'lodash/fp';

import { Flex, FlexPanel, Grid, Button, Text } from '../design-system';
import { getShockLabelIndex } from '../utils/preferencesHelpers';

const NUM_ACTIVITIES = 10;

const aggregateDestinationsFamiliarity = fp.compose(
  fp.countBy(fp.identity),
  fp.map(fp.get('conventionality')),
  fp.get('destinations')
);

const getTripFamiliarity = (trip) => {
  const familiarities = aggregateDestinationsFamiliarity(trip);
  const totalCount = trip.destinations.length || 1;
  if (familiarities['classic'] / totalCount >= 0.7) {
    return 'classic';
  } else if (
    familiarities['off-the-grid'] +
      familiarities['off-the-beaten-path'] / totalCount >=
    0.7
  ) {
    return 'off-the-grid';
  } else {
    return 'off-the-beaten-path';
  }
};

const destinationInfo = (
  trip,
  t,
  showMore,
  showMoreToggle,
  tripSummary = false
) => {
  const activities = (showMore
    ? trip.activities
    : trip.activities.slice(0, NUM_ACTIVITIES)
  ).join(', ');
  const wantsToggle = trip.activities.length > NUM_ACTIVITIES;
  const durationDays = trip.duration.slice(1, trip.duration.length - 1);
  const familiarity = !tripSummary
    ? trip.conventionality
    : getTripFamiliarity(trip);

  return [
    {
      duration: (
        <Flex key="duration">
          {t('duration_days', { duration: trip.duration })}
          <Text pl={1}>
            {parseInt(durationDays) > 1
              ? t('day_plural', 'Days')
              : t('day', 'Day')}
          </Text>
        </Flex>
      ),
    },
    tripSummary && {
      destinations: t('list_format', {
        list: trip.destinations.map((d) => d.name),
      }),
    },
    {
      activity_plural: (
        <Text key="activity_plural" lineHeight="copy">
          {activities}
          {wantsToggle && (
            <Button
              minWidth="auto"
              fontSize={3}
              ml={2}
              bg="surface"
              onClick={showMoreToggle}
            >
              {showMore ? t('show_less', 'Less...') : t('show_more', 'More...')}
            </Button>
          )}
        </Text>
      ),
    },
    { scenery: t('list_format', { list: trip.naturalScenery }) },
    {
      development: t('list_format', {
        list:
          typeof trip.developmentLevel === 'string'
            ? [trip.developmentLevel]
            : trip.developmentLevel,
      }),
    },
    {
      familiarity: familiarity
        ? t(
            `${tripSummary ? 'trip_' : 'destination_'}${familiarity}`,
            familiarity
          )
        : '',
    },
  ];
};

const cultureShocksDest = (trip) => {
  return [
    {
      infrastructure: `infrastructure_shock_${getShockLabelIndex(
        trip.shocks.infrastructure
      )}`,
    },
    {
      language_level: `language_shock_${getShockLabelIndex(
        trip.shocks.language
      )}`,
    },
    { food_poisoning: `food_shock_${getShockLabelIndex(trip.shocks.food)}` },
    { petty_crime: `crime_shock_${getShockLabelIndex(trip.shocks.crime)}` },
  ];
};

export const TripProfileTable = ({
  trip,
  showMore,
  toggleShowMore,
  tripSummary,
  t,
}) => (
  <FlexPanel bg="background" fontSize={4}>
    {destinationInfo(trip, t, showMore, toggleShowMore, tripSummary)
      .filter((elem) => elem !== false)
      .map((elem, index) => (
        <Grid
          key={index}
          gridTemplateColumns="1fr 2fr"
          gridGap={2}
          p={3}
          borderBottom={1}
          borderColor="background"
        >
          <Text fontSize={4}>{t(Object.keys(elem))}</Text>
          <Text fontSize={4}>{Object.values(elem)}</Text>
        </Grid>
      ))}
    <Grid gridTemplateColumns="1fr" gridGap={3} p={3}>
      <Text fontSize={4} bold>
        {t('cultureShocks', 'Culture Shocks')}
      </Text>
    </Grid>
    {cultureShocksDest(trip).map((elem, index) => (
      <Grid
        key={index}
        gridTemplateColumns="1fr 2fr"
        borderTop={1}
        borderColor="background"
        gridGap={2}
        p={3}
      >
        {elem && <Text fontSize={4}>{t(Object.keys(elem))}</Text>}
        <Text fontSize={4}>{t(Object.values(elem))}</Text>
      </Grid>
    ))}
  </FlexPanel>
);

export default compose(
  withTranslation('common'),
  inject(({ app }) => ({
    showMore: app.showMore,
    toggleShowMore: () => app.toggleShowMore(),
  })),
  observer
)(TripProfileTable);
