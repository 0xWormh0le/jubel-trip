import React from 'react';
import { inject, observer, PropTypes as MobxPropTypes } from 'mobx-react';
import { withRouter } from 'react-router-dom';
import compose from 'lodash/fp/compose';
import PropTypes from 'prop-types';

import { FlexPanel, IconButton, SlidingPanel } from '../design-system';
import { withTranslation } from 'react-i18next';
import { CountryProfile, Tabs } from '.';

export const CountryProfilePanel = (props) => {
  const {
    countryTabsState,
    isOpen,
    onClose,
    t,
    setCountry,
    width = 600,
    ...otherProps
  } = props;
  return (
    <SlidingPanel
      id="country-profile"
      maxWidth={width}
      closeType="none"
      open={isOpen}
      onToggle={onClose}
      {...otherProps}
    >
      <FlexPanel
        bg="background"
        flex={1}
        alignContent="flex-start"
        borderLeft={1}
        width={width}
      >
        <FlexPanel fontSize={4} bg="surface">
          <IconButton
            p={3}
            bg="surface"
            onClick={onClose}
            name="arrow-back"
            mb={1}
            labelFontSize={5}
            labelMl={2}
            labelColor="on.background"
          >
            {t('countries_visited', 'Countries Visited')}
          </IconButton>
          <Tabs
            tabs={countryTabsState}
            shouldTranslate={false}
            onChange={setCountry}
          />
        </FlexPanel>
        <CountryProfile countryCode={countryTabsState.activeTab} />
      </FlexPanel>
    </SlidingPanel>
  );
};

CountryProfilePanel.propTypes = {
  countryTabsState: MobxPropTypes.objectOrObservableObject.isRequired,
  t: PropTypes.func.isRequired,
  isOpen: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
};

export default compose(
  withRouter,
  withTranslation('common'),
  inject(({ app }, { history }) => ({
    isOpen: app.isCountryProfilePanelOpen,
    onClose: () => history.push(`/trips/${app.currentTrip.id}`),
    countryTabsState: app.route.countryTabsState,
    width: app.countryPanelWidth,
    setCountry: (code) =>
      app.goto(`/trips/${app.currentTrip.id}/country/${code}`),
  })),
  observer
)(CountryProfilePanel);
