/* eslint-disable import/first */
import React from 'react';
import { inject, Provider, observer } from 'mobx-react';
import { storiesOf } from '@storybook/react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';

import { Preferences } from '../components/Preferences';
import { travelStyles } from '../models/TravelStyle';
import Box from '../design-system/Box';
import Customer from '../models/Customer';
import PrefState from '../models/Preferences';

const customer = new Customer();
const prefState = new PrefState({ customer });

const PreferencesWrapper = compose(
  withTranslation('common'),
  inject(({ app }) => ({
    minWidth: 40,
    maxWidth: 360,
    travelStyles: app.travelStyles,
    accommodations: app.preferences.accommodations,
    activities: app.preferences.activities,
    budget: app.preferences.budget,
    datesFlexible: app.preferences.datesFlexible,
    development: app.preferences.development,
    groupType: app.preferences.groupType,
    loadOptions: app.preferences.loadOptions,
    familiarity: app.preferences.familiarity,
    open: app.preferences.open,
    placesToAvoid: app.preferences.placesToAvoid,
    placesToSee: app.preferences.placesToSee,
    scenery: app.preferences.scenery,
    searchText: app.preferences.searchText,
    shocks_crime: app.preferences.shocks_crime,
    shocks_infrastructure: app.preferences.shocks_infrastructure,
    shocks_food: app.preferences.shocks_food,
    shocks_language: app.preferences.shocks_language,
    showTips: app.preferences.showTips,
    travelStyleValues: app.preferences.travelStyles,
    travelPace: app.preferences.travelPace,
    triggerSearch: app.preferences.executeSearch,
    toggleTips: app.preferences.toggleTips,
    update: app.preferences.update,
    updateSwitches: app.preferences.updateSwitches,
    switches: app.preferences.switches,
    toggle: app.preferences.toggleVisible,
    openTriggerd: app.preferences.openTriggerd,
    closeTriggered: app.preferences.closeTriggered,
    clearAll: app.preferences.clearAll,
  })),
  observer
)(Preferences);

storiesOf('Preferences', module)
  .addDecorator((story) => (
    <Provider app={{ preferences: prefState, travelStyles }}>
      <Box mx="auto" width={300} p={3}>
        {story()}
      </Box>
    </Provider>
  ))
  .add('normal', () => <PreferencesWrapper />, {
    notes: {
      markdown: `
  ### Preferences application component

  **Resolves** [JP-174](https://github.com/Jubel-co/jp-frontend/issues/174)`,
    },
  });
