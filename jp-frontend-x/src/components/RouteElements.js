import React from 'react';
import { LazyLoadComponent } from 'react-lazy-load-image-component';
import { Link } from 'react-router-dom';
import { PropTypes } from 'prop-types';
import { withTranslation } from 'react-i18next';
import { themeGet } from 'styled-system';

import { rem2px } from '../design-system/theme/utils';

import {
  Absolute,
  Box,
  Flex,
  FlexPanel,
  Icon,
  IconButton,
  RatingControl,
  Text,
} from '../design-system';
import system from '../utils/System';
import { addDays } from 'date-fns';
import { flagEmoji } from '../design-system/theme/utils';

const MIN_HEIGHT = 40;

const IconColumn = system('IconColumn', {
  extend: Flex,
  minHeight: MIN_HEIGHT,
  width: 32,
  color: 'misc.muted',
  centered: true,
});

const TextColumn = system('TextColumn', {
  extend: FlexPanel,
  ml: 3,
  justifyContent: 'center',
});

const connectorSpace = 3;

const RouteElements = system('RouteElements', {
  extend: Flex,
  boxShadow: 2,
  mb: connectorSpace,
  bg: 'surface',
  borderRadius: 'sm',
  width: 1,
  className: 'route-element',
});

const Splitter = system(
  'Splitter',
  {
    extend: Box,
    borderLeft: 1,
    width: '1px',
    className: 'splitter',
  },
  (props) => ({
    marginBottom:
      props.connector && `-${themeGet(`space.${connectorSpace}`)(props)}`,
  })
);

const ContentRow = system(
  'ContentRow',
  {
    extend: Flex,
    flex: 1,
    px: 3,
    fontSize: 4,
    borderBottom: 1,
    hoverable: true,
  },
  ({ large }) => ({
    minHeight: large ? 2 * MIN_HEIGHT : MIN_HEIGHT,
    '&:last-child': {
      borderBottom: 'none',
    },
  })
);

const SummaryConnector = system(
  'SummaryConnector',
  {
    extend: Absolute,
    top: `100%`,
    borderRight: 1,
    width: '1px',
  },
  (props) => ({
    left:
      rem2px(themeGet(`space.3`)(props)) +
      rem2px(themeGet(`space.1`)(props)) +
      18,
    height: themeGet(`space.${connectorSpace}`)(props),
  })
);

const DailyTransportRow = ({ isExpanded, icon, duration, t }) => (
  <ContentRow>
    <IconColumn>
      <Icon name={icon} />
    </IconColumn>
    {isExpanded && (
      <TextColumn alignItems="center">
        <Flex>
          <Text color="misc.muted" pl={2}>
            {t('humanize_duration', { duration })}
          </Text>
        </Flex>
      </TextColumn>
    )}
  </ContentRow>
);

const DailyDestinationCountryRow = ({ isExpanded, code, name, t, tripId }) => (
  <ContentRow
    as={Link}
    to={`/trips/${tripId}/country/${code}`}
    className="daily-destination-country-row"
  >
    <IconColumn>
      <Text fontSize="32px">{flagEmoji(code)}</Text>
    </IconColumn>
    {isExpanded && (
      <TextColumn alignItems="center">
        <Text bold color="on.surface">
          {name}
        </Text>
      </TextColumn>
    )}
  </ContentRow>
);

const DailyDestinationCityRow = ({
  destinationId,
  tripId,
  isExpanded,
  name,
  days,
  images,
  t,
}) => (
  <Link to={`/trips/${tripId}/destination/${destinationId}`}>
    <ContentRow
      px={0}
      large
      backgroundPosition="center"
      backgroundSize="cover"
      backgroundImage={images.length > 0 ? `url(${images[0].src})` : undefined}
    >
      {isExpanded && (
        <FlexPanel
          backgroundImage="linear-gradient(to right, rgba(1,1,1,.5), rgba(1,1,1,.25), transparent)"
          px={3}
          color="white"
          flex={1}
          justifyContent="center"
        >
          <Text bold>{name}</Text>
          <Text fontSize={3} uppercase bold>
            {t('day_with_count', { count: days })}
          </Text>
        </FlexPanel>
      )}
    </ContentRow>
  </Link>
);

const DailyAccommodationRow = ({ isExpanded, name, rating, days, t }) => (
  <ContentRow large>
    <IconColumn>
      <Icon name="hotel" />
    </IconColumn>
    {isExpanded && (
      <TextColumn py={2}>
        <Text>{name}</Text>
        <Flex mt={1} alignItems="center">
          <RatingControl
            readonly
            size={14}
            initialRating={rating}
            fullColor="secondaryVariant"
            ml={3}
          />
          <Text
            textStyle="uppercase"
            color="misc.muted"
            fontSize={2}
            mr={1}
            ml={2}
          >
            {t('days_nights', { days: days, nights: days - 1 })}
          </Text>
        </Flex>
      </TextColumn>
    )}
  </ContentRow>
);

const DailyActivityRow = ({ isExpanded, name, duration, t }) => (
  <ContentRow>
    <IconColumn>
      <Icon name="directions-run" />
    </IconColumn>
    {isExpanded && (
      <TextColumn color="on.surface" py={2}>
        <Text uppercase letterSpacing={1} fontSize={2} color="misc.muted">
          {t('humanize_duration', { duration })}
        </Text>
        <Text>{t(`activity_${name.toLowerCase()}`, name)}</Text>
      </TextColumn>
    )}
  </ContentRow>
);

const wt = (c) => withTranslation('common')(c);
const rowComponents = {
  transport: wt(DailyTransportRow),
  'destination-country': wt(DailyDestinationCountryRow),
  'destination-city': wt(DailyDestinationCityRow),
  accommodation: wt(DailyAccommodationRow),
  activity: wt(DailyActivityRow),
};

const DateColumn = wt(({ day, departureDate, t }) => {
  const [label, dayString] = (() => {
    if (departureDate instanceof Date) {
      const date = addDays(departureDate, day);
      return [t('date_MMM', { date }), t('date_D', { date })];
    } else {
      return [t('day', { count: 1 }), String(day)];
    }
  })();
  return (
    <FlexPanel
      fontSize={3}
      centered
      p={2}
      width={35}
      minWidth={35}
      minHeight={MIN_HEIGHT}
    >
      <Text center uppercase color="misc.muted">
        {label}
      </Text>
      <Text mt={1} color="on.background" bold>
        {dayString}
      </Text>
    </FlexPanel>
  );
});

export const DailyRouteElements = ({
  start = 0,
  days = 1,
  last,
  departureDate,
  isExpanded,
  elements,
  tripId,
}) => (
  <RouteElements>
    {elements.length > 0 && (
      <LazyLoadComponent>
        <DateColumn
          day={start}
          departureDate={departureDate}
          isExpanded={isExpanded}
        />
        <Splitter connector={!last} />
        <FlexPanel flex={1}>
          {elements.map((element, index) => {
            const RowComponent = rowComponents[element.type];
            return RowComponent ? (
              <RowComponent
                key={index}
                isExpanded={isExpanded}
                hoverable
                start={start}
                days={days}
                tripId={tripId}
                destinationId={element.id}
                {...element}
              />
            ) : null;
          })}
        </FlexPanel>
      </LazyLoadComponent>
    )}
  </RouteElements>
);

DailyRouteElements.propTypes = {
  isExpanded: PropTypes.bool,
  day: PropTypes.number,
  departureDate: PropTypes.oneOfType([PropTypes.string, PropTypes.object]), // string, Date or moment object
  elements: PropTypes.array,
};

export const SummarizedRouteElements = wt(
  ({ isExpanded, type, transport, icon, duration, name, last, t }) => (
    <RouteElements position="relative">
      <FlexPanel py={2}>
        <ContentRow>
          <IconColumn ml={1}>
            <Icon name={icon} />
          </IconColumn>
          {isExpanded && (
            <TextColumn>
              <Text color="on.surface" bold={type === 'destination-city'}>
                {type === 'transport'
                  ? t(`${transport}_to_place`, { place: name })
                  : name}
              </Text>
              <Flex mt={1}>
                <Text
                  uppercase
                  fontSize={3}
                  color="misc.muted"
                  letterSpacing={1}
                >
                  {t('humanize_duration', { duration })}
                </Text>
              </Flex>
            </TextColumn>
          )}
        </ContentRow>
      </FlexPanel>
      {!last && <SummaryConnector />}
    </RouteElements>
  )
);

SummarizedRouteElements.propTypes = {
  icon: PropTypes.string,
  duration: PropTypes.string,
  name: PropTypes.string,
  t: PropTypes.func,
};

export const NewRouteElement = wt(
  ({ day = 0, departureDate = new Date(), isExpanded, t }) => (
    <RouteElements bg="background">
      <DateColumn day={day} departureDate={departureDate} />
      <Splitter />
      <ContentRow
        alignItems="center"
        justifyContent={isExpanded ? 'flex-end' : 'center'}
      >
        <IconButton
          bg="background"
          label={isExpanded && t('add', 'add')}
          name="add"
        />
      </ContentRow>
    </RouteElements>
  )
);
