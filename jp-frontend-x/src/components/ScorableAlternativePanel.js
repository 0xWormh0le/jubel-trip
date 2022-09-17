import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import React from 'react';
import { withRouter } from 'react-router';
import { withTranslation } from 'react-i18next';

import Absolute from '../design-system/Absolute';
import DropDownButton from '../design-system/DropDownButton';
import FlexPanel from '../design-system/FlexPanel';
import IconButton from '../design-system/IconButton';
import MaxPanel from '../design-system/MaxPanel';
import Relative from '../design-system/Relative';
import SlidingPanel from '../design-system/SlidingPanel';

import {
  ScorableAlternativeAbout,
  ScorableAlternativeAlternatives,
  ScorableAlternativeProfile,
  Tabs,
} from './';

const renderTab = (activeTab = 'about') => {
  const TabContent = {
    about: ScorableAlternativeAbout,
    alternatives: ScorableAlternativeAlternatives,
    profile: ScorableAlternativeProfile,
  }[activeTab];
  return <TabContent />;
};

export const SCORABLE_ALTERNATIVE_PANEL_WIDTH = 600;

const menuItems = (t) => [
  {
    value: 'remove',
    label: t('remove_from_itinerary', 'Remove from Itinerary'),
  },
  {
    type: 'divider',
  },
  {
    value: 'about',
    label: t('about', 'About'),
  },
  {
    value: 'profile',
    label: t('profile', 'Profile'),
  },
  {
    value: 'alternatives',
    label: t('alternatives', 'Alternatives'),
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

const renderMenuItem = ({ label, selected }) => (
  <IconButton
    label={label}
    labelUppercase={false}
    labelBold={false}
    size={12}
    labelMargin={2}
    labelFontSize={4}
    name={selected ? 'check' : ''}
    hoverable={false}
    cursor="default"
  />
);

export const ScorableAlternativePanel = ({
  element,
  tabs,
  isOpen,
  onClose,
  rtl,
  maxWidth,
  t,
}) => {
  const handleMenuSelect = (item) => {
    switch (item.value) {
      case 'about':
      case 'alternatives':
      case 'profile':
        tabs.setActiveTab(item.value);
        break;
      default:
        break;
    }
  };

  return (
    <SlidingPanel
      id="scorable-alternative"
      open={isOpen}
      onToggle={onClose}
      maxWidth={maxWidth}
      rtl={rtl}
      closeType="none"
    >
      <MaxPanel bg="background" width={SCORABLE_ALTERNATIVE_PANEL_WIDTH}>
        <FlexPanel fontSize={4} bg="surface">
          <Relative>
            <FlexPanel fontSize={4} bg="surface">
              <IconButton
                p={3}
                bg="surface"
                onClick={onClose}
                name="arrow-back"
                labelMl={2}
                labelFontSize={5}
                labelBold
                labelColor="on.background"
              >
                {element && element.name}
              </IconButton>
              <Absolute right={5} top={10}>
                <DropDownButton
                  menuMinWidth={150}
                  color="on.surface"
                  onSelect={handleMenuSelect}
                  items={menuItems(t)}
                  value={tabs.activeTab}
                  customButtonRenderer={renderDotsButton}
                  customMenuItemRenderer={renderMenuItem}
                />
              </Absolute>
            </FlexPanel>
          </Relative>
          <Tabs tabs={tabs} />
        </FlexPanel>
        {element && renderTab(tabs.activeTab)}
      </MaxPanel>
    </SlidingPanel>
  );
};

export default compose(
  withRouter,
  withTranslation('common'),
  inject(({ app }, { history }) => ({
    isOpen: app.routeElementToggle.isOpen,
    onClose: () => history.push(`/trips/${app.currentTrip.id}`),
    element: app.selectedRouteElement,
    tabs: app.selectedRouteElementTabsState,
    maxWidth: app.scorableAltPanelWidth,
  })),
  observer
)(ScorableAlternativePanel);
