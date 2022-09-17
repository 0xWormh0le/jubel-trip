/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';

import { travelStyles } from '../models/TravelStyle';
import Box from '../design-system/Box';
import TravelStyleButton from '../components/TravelStyleButton';

storiesOf('TravelStyleButton', module)
  .addDecorator((story) => (
    <Box mx="auto" width={500} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'enabled',
    () =>
      travelStyles.map((ts, index) => (
        <TravelStyleButton
          key={index}
          travelStyle={ts}
          width={60}
          height={60}
          onClick={action('onClick')}
        />
      )),
    {
      notes: {
        markdown: `
#### TravelStyleButton  Design Component

A design component for Travel style buttons

**Resolves**
[PR#63](https://github.com/Jubel-co/jp-frontend/pull/63)`,
      },
    }
  )
  .add('disabled', () =>
    travelStyles.map((ts, index) => (
      <TravelStyleButton
        key={index}
        travelStyle={ts}
        width={60}
        height={60}
        disabled
        onClick={action('onClick')}
      />
    ))
  );
