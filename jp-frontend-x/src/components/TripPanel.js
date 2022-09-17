import React from 'react';
import { inject, observer } from 'mobx-react';
import { withRouter } from 'react-router-dom';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import GetQuoteForm from './GetQuoteForm';
import Tabs from './Tabs';
import TripItinerary from './TripItinerary';
import TripSummaryProfile from './TripSummaryProfile';
import TripSummaryWhatsIncluded from './TripSummaryWhatsIncluded';

import Absolute from '../design-system/Absolute';
import Button from '../design-system/Button';
import DropDownButton from '../design-system/DropDownButton';
import Flex from '../design-system/Flex';
import FlexPanel from '../design-system/FlexPanel';
import IconButton from '../design-system/IconButton';
import MaxPanel from '../design-system/MaxPanel';
import Modal from '../design-system/Modal';
import Relative from '../design-system/Relative';
import SlidingPanel from '../design-system/SlidingPanel';
import Text from '../design-system/Text';
import withAccommodationOptions from '../hocs/withAccommodationOptions';

const renderTab = (activeTab = 'itinerary') => {
  const TabContent = {
    itinerary: TripItinerary,
    profile: TripSummaryProfile,
    included: TripSummaryWhatsIncluded,
  }[activeTab];
  return <TabContent />;
};

const getItems = (t) => [
  {
    value: 'get_a_quote',
    label: t('customize_get_a_quote', 'Customize and Get Quote'),
  },
  {
    type: 'divider',
  },
  {
    value: 'itinerary',
    label: t('itinerary', 'Itinerary'),
  },
  {
    value: 'profile',
    label: t('profile', 'Profile'),
  },
  {
    value: 'included',
    label: t('included', "What's included"),
  },
];

const renderDotsButton = ({ iconSize, onClick, ref }) => (
  <IconButton
    onClick={onClick}
    size={iconSize}
    name="dots-vertical"
    px={1}
    ref={ref}
  />
);

const renderMenuItem = ({ label, selected, disabled }) => (
  <IconButton
    label={label}
    labelUppercase={false}
    labelBold={false}
    size={12}
    labelMargin={2}
    labelFontSize={4}
    name={selected ? 'check' : ''}
    hoverable={false}
    disabled={disabled}
    cursor="default"
  />
);

const getAccommodationLabelRender = (accommodationsOptions) => ({
  item,
  label,
  iconSize,
  onClick,
  ref,
  open,
  uppercase,
}) => {
  const activeItem =
    accommodationsOptions.find((accomm) => accomm.value === item.value) || item;
  const textStyle = uppercase ? 'uppercase' : 'normal';
  return (
    <IconButton
      labelPosition="left"
      ref={ref}
      onClick={onClick}
      size={iconSize}
      name={open ? 'arrow-drop-up' : 'arrow-drop-down'}
      px={1}
      textStyle={textStyle}
      label={activeItem.value ? activeItem.label : label}
    />
  );
};

const filterTripAccommodations = (accommodationsOptions, trip, t) => {
  if (!trip) {
    return accommodationsOptions;
  }
  const values = Object.keys(trip.accommodation);
  return accommodationsOptions.map((item) => ({
    value: item.value,
    label: `${item.label} - ${
      values.includes(item.value)
        ? t('dollar_amount', {
            amount: trip.accommodation[item.value],
          })
        : 'not available'
    }`,
    disabled: !values.includes(item.value),
  }));
};

export const TripPanel = ({
  prefAccommodation,
  accommodationsOptions,
  setTripAccommodation,
  t,
  trip,
  open,
  toggleQuote,
  toggleQuoteOpen,
  backToTrips,
  maxWidth,
  tabs,
}) => {
  const handleMenuSelect = (item) => {
    switch (item.value) {
      case 'get_a_quote':
        toggleQuote.open();
        break;
      case 'itinerary':
      case 'profile':
      case 'included':
        tabs.setActiveTab(item.value);
        break;
      default:
        break;
    }
  };

  const handleSetTripAccommodation = (item) => {
    setTripAccommodation(trip.id, item.value);
  };

  const renderAccommodationLabel = getAccommodationLabelRender(
    accommodationsOptions
  );

  const filteredAccommodationOptions = filterTripAccommodations(
    accommodationsOptions,
    trip,
    t
  );

  return (
    <SlidingPanel
      id="trip-panel"
      open={open}
      maxWidth={maxWidth}
      closeType="none"
    >
      {trip && (
        <MaxPanel bg="background" width={maxWidth}>
          <Relative>
            <FlexPanel fontSize={4} bg="surface" borderBottom={1}>
              <IconButton
                p={3}
                bg="surface"
                onClick={backToTrips}
                name="arrow-back"
                labelMl={2}
                labelFontSize={5}
                labelBold
                labelColor="on.background"
              >
                {trip.name}
              </IconButton>
              <Absolute right={5} top={10}>
                <DropDownButton
                  menuMinWidth={150}
                  color="on.surface"
                  onSelect={handleMenuSelect}
                  items={getItems(t)}
                  value={tabs.activeTab}
                  customButtonRenderer={renderDotsButton}
                  customMenuItemRenderer={renderMenuItem}
                />
              </Absolute>
            </FlexPanel>
          </Relative>
          <Flex
            fontSize={4}
            alignItems="center"
            flexDirection="column"
            p={3}
            bg="surface"
          >
            <Button
              width={1}
              secondary
              py={3}
              px={4}
              bold
              truncate
              fontSize={5}
              onClick={toggleQuote.toggle}
              id="cta_get_quote"
            >
              {t('customize_get_a_quote', 'Customize and Get Quote')}
            </Button>
            <Flex flex={1} mt={3} alignItems="center">
              <DropDownButton
                menuMinWidth={175}
                placement="bottom-start"
                color="on.surface"
                iconSize={16}
                customButtonRenderer={renderAccommodationLabel}
                onSelect={handleSetTripAccommodation}
                items={filteredAccommodationOptions}
                value={prefAccommodation}
              />
              <Text bold as="span" ml={3}>
                {t('duration_days', { duration: trip.duration })}{' '}
                {t('day_plural', 'Days')} &mdash;{' '}
                {t('dollar_amount', {
                  amount:
                    trip.accommodation[prefAccommodation] ||
                    trip.estimatedCost ||
                    'N/A',
                })}
              </Text>
              <Text ml={2} as="span" textStyle="uppercase" truncate>
                {t('per_person', 'Per Person')}
              </Text>
            </Flex>
            <Modal open={toggleQuoteOpen} close={toggleQuote.close}>
              <GetQuoteForm close={toggleQuote.close} />
            </Modal>
          </Flex>
          <Tabs tabs={tabs} bg="surface" id="trip_panel_tabs" />
          {renderTab(tabs.activeTab)}
        </MaxPanel>
      )}
    </SlidingPanel>
  );
};

export default compose(
  withRouter,
  withTranslation('common'),
  withAccommodationOptions,
  inject(({ app }, { history }) => ({
    open: app.isTripPanelOpen,
    trip: app.currentTrip,
    tripCount: app.tripCount,
    toggleQuote: app.toggleQuote,
    toggleQuoteOpen: app.toggleQuote.isOpen,
    backToTrips: () => history.push('/trips'),
    maxWidth: app.tripPanelWidth,
    tabs: app.tripTabsState,
    setTripAccommodation: app.customer.setTripAccommodation,
    prefAccommodation: app.currentTrip
      ? app.customer.getTripAccommodation(app.currentTrip.id)
      : null,
  })),
  observer
)(TripPanel);
