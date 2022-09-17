import { MainHeader, Preferences, TravelMap, TravelStylePanel } from '.';
import compose from 'lodash/fp/compose';
import { inject, observer } from 'mobx-react';
import * as React from 'react';

import { Absolute, MaxPanel } from '../design-system';

import CountryProfilePanel from '../components/CountryProfilePanel';
import ScorableAlternativePanel from '../components/ScorableAlternativePanel';
import TripListPanel from '../components/TripListPanel';
import TripPanel from '../components/TripPanel';
import withMobileDetect from '../hocs/withMobileDetect';

export const Layout = ({
  isTripScreen,
  headerHeight,
  showBaseMap,
  children,
}) => (
  <MaxPanel flexDirection="row">
    {showBaseMap && <TravelMap />}
    {children}
    <Absolute top={isTripScreen ? headerHeight : 0} bottom={0} left={0}>
      <TripListPanel key="tripListPanel" />
      <TripPanel key="tripPanel" />
      <ScorableAlternativePanel key="scorableAlternativePanel" />
      <CountryProfilePanel key="countryProfilePanel" />
      <TravelStylePanel />
    </Absolute>
    {isTripScreen && (
      <Absolute top={headerHeight} bottom={0} right={0} width={40}>
        <Preferences />
      </Absolute>
    )}
    <MainHeader />
  </MaxPanel>
);

export default compose(
  withMobileDetect,
  inject(({ app }) => ({
    isTripScreen: app.isTripScreen,
    showBaseMap: app.showBaseMap,
    headerHeight: app.headerHeight,
  })),
  observer
)(Layout);
