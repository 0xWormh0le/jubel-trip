/* eslint-disable import/first */
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';
import { Box } from '../design-system';
import DatePicker from '../design-system/DatePicker';
import { toValidDate } from '../utils/date';
import i18n from '../i18n';
import { stripMargin } from '../utils/strings';

storiesOf('DatePicker', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'normal',
    () => (
      <DatePicker
        label="Date:"
        formatDate={(value) => toValidDate(value, i18n.language)}
        onChange={(value) => action('onChange')(value)}
        value={new Date('2019-08-08')}
      />
    ),
    {
      notes: stripMargin(`
      #### DatePicker Design Component
      A design component for DatePicker. This has been deprecated
      in favor of [\`SingleDatePicker\`](/stories/SingleDatePickerStory).

      **Resolves**
      [PR#48](https://github.com/Jubel-co/jp-frontend/pull/48)
      `),
    }
  )
  .add(
    'with minDate',
    () => (
      <DatePicker
        label="Date:"
        formatDate={(value) => toValidDate(value, i18n.language)}
        onChange={(value) => action('onChange')(value)}
        value={new Date('2019-08-08')}
        minDate={new Date('2019-07-15')}
      />
    ),
    {
      notes: 'A date picker with min date.',
    }
  )
  .add(
    'with maxDate',
    () => (
      <DatePicker
        label="Date:"
        formatDate={(value) => toValidDate(value, i18n.language)}
        onChange={(value) => action('onChange')(value)}
        value={new Date('2019-08-08')}
        maxDate={new Date('2019-09-15')}
      />
    ),
    {
      notes: 'A date picker with max date.',
    }
  )
  .add(
    'with minDate & maxDate',
    () => (
      <DatePicker
        label="Date:"
        formatDate={(value) => toValidDate(value, i18n.language)}
        onChange={(value) => action('onChange')(value)}
        value={new Date('2019-08-08')}
        minDate={new Date('2019-07-15')}
        maxDate={new Date('2019-09-15')}
      />
    ),
    {
      notes: 'A date picker with max date.',
    }
  )
  .add(
    'disabled',
    () => (
      <DatePicker
        label="Date:"
        placeholder="Disabled"
        disabled
        value={new Date('2019-08-08')}
      />
    ),
    {
      notes: 'A **disabled** date picker.',
    }
  );
