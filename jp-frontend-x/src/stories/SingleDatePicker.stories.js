/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';
import moment from 'moment';

import Box from '../design-system/Box';
import SingleDatePicker from '../design-system/SingleDatePicker';

const dt = moment();

storiesOf('SingleDatePicker', module)
  .addDecorator((story) => (
    <Box mx="auto" width={500} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'default',
    () => (
      <SingleDatePicker
        label="From:"
        placeholder="MM/DD/YYYY"
        onChange={action('onChange')}
        value={dt}
        maxDate="2019-12-22"
        minDate="2018-09-22"
        id="singleDate"
      />
    ),
    {
      notes: {
        markdown: `
#### SingleDatePicker Design Component
A design component for SingleDatePicker
**Resolves**
[Issues#86](https://github.com/Jubel-co/jp-frontend/pull/48)`,
      },
    }
  )
  .add('disabled', () => (
    <SingleDatePicker
      label="Disabled:"
      placeholder="MM/DD/YYYY"
      onChange={action('onChange')}
      value={dt}
      maxDate="2019-12-22"
      minDate="2018-09-22"
      id="singleDateDisabled"
      disabled
    />
  ));
