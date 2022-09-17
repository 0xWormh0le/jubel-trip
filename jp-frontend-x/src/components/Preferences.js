import React from 'react';
import { inject, observer } from 'mobx-react';
import { withTranslation } from 'react-i18next';
import compose from 'lodash/fp/compose';
import isNil from 'lodash/isNil';

import { PrefRow, TravelDatesPicker, PrefSliderField } from './';
import {
  getTravelPaceLabel,
  getDevelopmentLabel,
  getFamiliarityLabel,
  getPoorInfrastructureLabel,
  getLanguageBarrierLabel,
  getFoodPoisoningRiskLabel,
  getPettyCrimeRiskLabel,
  getStyleSliderLabel,
} from '../utils/preferencesHelpers';
import {
  Box,
  Button,
  Absolute,
  CheckBox,
  Grid,
  IconButton,
  Input,
  MaxPanel,
  RemoteSelect,
  SlidingPanel,
} from '../design-system';
import withHubspotDetect from '../hocs/withHubspotDetect';

export const Preferences = ({
  travelStyles,
  accommodations,
  activities,
  budget,
  datesFlexible,
  development,
  loadOptions,
  familiarity,
  open,
  placesToAvoid,
  placesToSee,
  scenery,
  searchText,
  shocks_crime,
  shocks_infrastructure,
  shocks_food,
  shocks_language,
  travelStyleValues,
  travelPace,
  showTips,
  t,
  toggle,
  toggleTips,
  triggerSearch,
  update,
  minWidth,
  maxWidth,
  openTriggerd,
  closeTriggered,
  switches,
  updateSwitches,
  hubspotLoaded,
  clearAll,
}) => (
  <SlidingPanel
    id="preferences"
    open={open}
    onToggle={toggle}
    minWidth={minWidth}
    maxWidth={maxWidth}
    rtl
    toggleOn="click"
    closeType="none"
  >
    {!open && (
      <MaxPanel centered borderLeft={1} alignItems="stretch">
        <IconButton
          name="prefs"
          onClick={toggle}
          height="100%"
          iconPosition="relative"
          iconZIndex={1}
          data-rh={t(
            'prefs_toggler_open_tooltip',
            'Click here to edit preferences'
          )}
        >
          {!openTriggerd ? (
            <Absolute className="ripple" width={24} height={24} />
          ) : (
            undefined
          )}
        </IconButton>
      </MaxPanel>
    )}
    {open && (
      <MaxPanel fontSize={4} borderLeft={1} width={maxWidth}>
        <Grid
          alignItems="center"
          gridTemplateColumns="1fr auto"
          bg="background"
          borderBottom={1}
        >
          <IconButton
            p={3}
            bg="background"
            name="arrow-forward"
            labelFontSize={5}
            labelMl={2}
            onClick={toggle}
            position="relative"
            data-rh={t(
              'prefs_toggler_close_tooltip',
              'Click here to edit preferences'
            )}
          >
            {t('prefs_header', 'Travel Preferences')}
            {!closeTriggered ? (
              <Absolute
                className="ripple"
                width={24}
                height={24}
                left="1.5rem"
              />
            ) : (
              undefined
            )}
          </IconButton>
          <CheckBox
            p={3}
            fontSize={3}
            color="misc.muted"
            justifySelf="end"
            checked={showTips}
            onChange={toggleTips}
            label={t('prefs_show_tips', 'Show Tips')}
          />
        </Grid>
        <Grid gridTemplateColumns="1fr" vscrollable={open} alignItems="center">
          <PrefRow
            key="prefs_places_to_see"
            tkey="prefs_places_to_see"
            label="Places to See"
            info={
              'What places you would like to see? You can type into the field'
            }
          >
            <RemoteSelect
              multiple
              placeholder={t(
                'prefs_places_to_see_placeholder',
                'Type or select a place name to see'
              )}
              value={placesToSee}
              cacheOptions
              defaultOptions
              loadOptions={loadOptions('placesToSee')}
              onChange={(placesToSee) => update({ placesToSee })}
            />
          </PrefRow>
          <PrefRow
            key="prefs_places_to_avoid"
            tkey="prefs_places_to_avoid"
            label="Places to Avoid"
            info={
              'What places you would prefer to avoid? You can type into the field.'
            }
          >
            <RemoteSelect
              multiple
              placeholder={t(
                'prefs_places_to_avoid_placeholder',
                'Type or select a place name to avoid'
              )}
              value={placesToAvoid}
              cacheOptions
              defaultOptions
              loadOptions={loadOptions('placesToAvoid')}
              onChange={(placesToAvoid) => update({ placesToAvoid })}
            />
          </PrefRow>
          <PrefRow
            key="prefs_budget"
            tkey="prefs_budget"
            label="Per-Person Budget"
            info={
              'How much is your per-person budget for accommodations, activities or tours, and local transfers between destinations?'
            }
          >
            <Input
              type="number"
              min="250"
              max="10000"
              step="250"
              value={isNil(budget) ? '' : budget}
              onChange={(budget) =>
                update({ budget: budget === '' ? null : Number(budget) })
              }
            />
          </PrefRow>
          <PrefRow
            key="prefs_travel_dates"
            tkey="prefs_travel_dates"
            label="Travel Dates"
            info={
              'Enter your expected travel dates and indicate if your dates are flexible.'
            }
          >
            <TravelDatesPicker
              placeholder={t(
                'prefs_travel_dates_placeholder',
                'Select your travel dates'
              )}
            />
            <CheckBox
              mt={2}
              label={t('prefs_dates_flexible', 'Dates flexible?')}
              checked={datesFlexible}
              onChange={(datesFlexible) => update({ datesFlexible })}
            />
          </PrefRow>
          <>
            {travelStyles.map(({ abbr, name, property }) => {
              const key = `prefs_styles_${abbr}`;
              return (
                <PrefSliderField
                  key={key}
                  tkey={key}
                  label={name}
                  info={t(`prefs_styles_how_much`, {
                    property: property,
                    abbr: abbr,
                  })}
                  tipType="style"
                  value={travelStyleValues[abbr]}
                  valueFormatter={getStyleSliderLabel}
                  sliderColor={`misc.branding.styles.${abbr}.bg`}
                  onChange={(value) => update({ [`styles_${abbr}`]: value })}
                  updateSwitches={updateSwitches}
                  switchName={`styles_${abbr}`}
                  switchValue={switches[`styles_${abbr}`]}
                />
              );
            })}
          </>
          <PrefRow
            key="prefs_activities"
            tkey="prefs_activities"
            label="Activities"
            info={t('What specific activitives would you like to do?')}
          >
            <RemoteSelect
              multiple
              placeholder={t(
                'prefs_activities_placeholder',
                'Type or select an activity'
              )}
              value={activities}
              cacheOptions
              defaultOptions
              loadOptions={loadOptions('activities')}
              onChange={(activities) => update({ activities })}
            />
          </PrefRow>
          <PrefRow
            key="prefs_scenery"
            tkey="prefs_scenery"
            label="Scenery"
            info={'What kinds of scenery would you like to see?'}
          >
            <RemoteSelect
              multiple
              placeholder={t(
                'prefs_scenery_placeholder',
                'Type or select a natural feature'
              )}
              value={scenery}
              cacheOptions
              defaultOptions
              loadOptions={loadOptions('scenery')}
              onChange={(scenery) => update({ scenery })}
            />
          </PrefRow>
          <PrefRow
            key="prefs_accommodations"
            tkey="prefs_accommodations"
            label="Accommodations"
            info={'Where would you like to stay?'}
          >
            <RemoteSelect
              multiple
              placeholder={t(
                'prefs_accommodations_placeholder',
                'Type or select a kind of place to stay'
              )}
              value={accommodations}
              cacheOptions
              defaultOptions
              loadOptions={loadOptions('accommodations')}
              onChange={(accommodations) => update({ accommodations })}
            />
          </PrefRow>
          <PrefSliderField
            key="prefs_travel_pace"
            tkey="prefs_travel_pace"
            label="Travel Pace"
            info="How quickly or slowly do you want to move from destination to destination?"
            value={travelPace}
            onChange={(travelPace) => update({ travelPace })}
            valueFormatter={getTravelPaceLabel}
            updateSwitches={updateSwitches}
            switchName="travelPace"
            switchValue={switches.travelPace}
          />
          <PrefSliderField
            key="prefs_development"
            tkey="prefs_development"
            label="Development"
            info="Do you prefer isolated destinations or more urban and developed?"
            value={development}
            onChange={(development) => update({ development })}
            valueFormatter={getDevelopmentLabel}
            updateSwitches={updateSwitches}
            switchName="development"
            switchValue={switches.development}
          />
          <PrefSliderField
            key="prefs_familiarity"
            tkey="prefs_familiarity"
            label="Familiarity"
            info="Do you prefer places off the beaten path, or more familiar tourist destinations?"
            value={familiarity}
            onChange={(familiarity) => update({ familiarity })}
            valueFormatter={getFamiliarityLabel}
            updateSwitches={updateSwitches}
            switchName="familiarity"
            switchValue={switches.familiarity}
          />
          <PrefSliderField
            key="prefs_shocks_infrastructure"
            tkey="prefs_shocks_infrastructure"
            label="Poor Infrastructure"
            info="How much do you prefer countries with modern infrastructure?"
            value={shocks_infrastructure}
            onChange={(shocks_infrastructure) =>
              update({ shocks_infrastructure })
            }
            valueFormatter={getPoorInfrastructureLabel}
            updateSwitches={updateSwitches}
            switchName="shocks_infrastructure"
            switchValue={switches.shocks_infrastructure}
          />
          <PrefSliderField
            key="prefs_shocks_language"
            tkey="prefs_shocks_language"
            label="Language Barrier"
            info="How much do you prefer destinations where your own language is spoken?"
            value={shocks_language}
            onChange={(shocks_language) => update({ shocks_language })}
            valueFormatter={getLanguageBarrierLabel}
            updateSwitches={updateSwitches}
            switchName="shocks_language"
            switchValue={switches.shocks_language}
          />
          <PrefSliderField
            key="prefs_shocks_food"
            tkey="prefs_shocks_food"
            label="Food Poisoning Risk"
            info="How much do you prefer destinations with less risk of getting food poisoning?"
            value={shocks_food}
            onChange={(shocks_food) => update({ shocks_food })}
            valueFormatter={getFoodPoisoningRiskLabel}
            updateSwitches={updateSwitches}
            switchName="shocks_food"
            switchValue={switches.shocks_food}
          />
          <PrefSliderField
            key="prefs_shocks_crime"
            tkey="prefs_shocks_crime"
            label="Petty Crime Risk"
            info="How much do you want avoid the likelihood of encountering petty crime?"
            value={shocks_crime}
            onChange={(shocks_crime) => update({ shocks_crime })}
            valueFormatter={getPettyCrimeRiskLabel}
            updateSwitches={updateSwitches}
            switchName="shocks_crime"
            switchValue={switches.shocks_crime}
          />
          <PrefRow
            key="prefs_search"
            tkey="prefs_search"
            label="Search"
            info={'Enter some words or phrases to narrow your search further'}
          >
            <Input
              value={searchText}
              onKeyUp={(event) => triggerSearch(event.key)}
              onChange={(searchText) => update({ searchText })}
            />
          </PrefRow>
          <Box p={3}>
            <Button primary onClick={clearAll} py={3} width={1}>
              {t('clear_all', 'Clear All')}
            </Button>
          </Box>
          {hubspotLoaded && <Box borderTop={1} py={5} />}
        </Grid>
      </MaxPanel>
    )}
  </SlidingPanel>
);

export default compose(
  withTranslation('common'),
  withHubspotDetect,
  inject(({ app }) => ({
    minWidth: app.isTripListScreen ? app.prefsWidths[0] : 0,
    maxWidth: app.prefsWidths[1],
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
