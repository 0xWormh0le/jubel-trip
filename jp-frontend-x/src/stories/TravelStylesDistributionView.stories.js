/* eslint-disable import/first */
import React from 'react';
import { storiesOf } from '@storybook/react';

import { travelStyles } from '../models/TravelStyle';
import Box from '../design-system/Box';
import TravelStylesDistributionView from '../components/TravelStylesDistributionView';

storiesOf('TravelStylesDistributionView', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add('normal', () => <TravelStylesDistributionView values={travelStyles} />, {
    notes: {
      markdown: `
      #### Travel Style Distribution Component

      A design component to use pie chart with internal and external labels
      and onmouse over tooltip or display data with rating component

      view: pie | ratings

      width: number,

      height: number,

      labelsMode: none | internal | external

      tooltipMode: none | description | value | both

      **Resolves**
      [PR#68](https://github.com/Jubel-co/jp-frontend/pull/68)`,
    },
  });
