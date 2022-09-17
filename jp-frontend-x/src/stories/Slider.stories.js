/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import Slider from '../design-system/Slider';

storiesOf('Slider', module)
  .addDecorator((story) => (
    <Box mx="auto" width={500} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'rtl',
    () => (
      <Slider
        rtl
        name="comfortLevel"
        label="Comfort Level"
        minLabel="Economy"
        maxLabel="Luxury"
        value={30}
        onChange={action('onChange')}
      />
    ),
    {
      notes: {
        markdown: `
#### Slider Design Component

A design component for a Slider based on React Compound Slider.

**Resolves**
[PR#65](https://github.com/Jubel-co/jp-frontend/pull/65)`,
      },
    }
  )
  .add('vertical 1', () => (
    <Slider
      vertical
      minLabel="minLabel"
      maxLabel="maxLabel"
      height={150}
      spacing={4}
      thickness={1}
      handleShape="rect"
      name="mixer.an"
      label="AN"
      value={30}
      onChange={action('onChange')}
    />
  ))
  .add('vertical 2', () => (
    <Slider
      vertical
      minLabel="minLabel"
      maxLabel="maxLabel"
      height={150}
      spacing={4}
      thickness={1}
      type="mixer"
      handleShape="rect"
      name="mixer.an"
      label="Mixer"
      value={30}
      onChange={action('onChange')}
    />
  ));
