/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';

import Box from '../design-system/Box';
import MultiSelect from '../design-system/MultiSelect';

let options = [
  { value: 'fishing', label: 'Fishing' },
  { value: 'scuba', label: 'Scuba' },
  { value: 'climbing', label: 'Climbing' },
  { value: 'hunting', label: 'Hunting' },
];

storiesOf('MultiSelect', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add('default', () => (
    <MultiSelect
      options={options}
      selected={['fishing']}
      label="Activities..."
      onSelect={action('onSelect')}
      onUnselect={action('onUnselect')}
      onAdd={action('onAdd')}
      onSearch={action('onSearch')}
    />
  ));
