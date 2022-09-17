/* eslint-disable import/first */
import 'react-dates/initialize';
import React from 'react';
import { action } from '@storybook/addon-actions';
import { storiesOf } from '@storybook/react';
import moment from 'moment';

import { Box } from '../design-system';
import DateRangePicker from '../design-system/DateRangePicker';
import { stripMargin } from '../utils/strings';

storiesOf('DateRangePicker', module)
  .addDecorator((story) => (
    <Box mx="auto" width={300} p={3}>
      {story()}
    </Box>
  ))
  .add(
    'normal',
    () => (
      <DateRangePicker
        startLabel="From:"
        endLabel="To:"
        onChange={(value) => action('onChange')(value)}
        value={{
          startDate: moment('2019-08-08'),
          endDate: moment('2019-08-20'),
        }}
        startDateId="startDate"
        endDateId="endDate"
      />
    ),
    {
      notes: stripMargin(`
      #### DateRangePicker Design Component
      A design component for DateRangePicker
      **Resolves**
      [Issues#86](https://github.com/Jubel-co/jp-frontend/pull/48)
      `),
    }
  )
  .add(
    'with minDate',
    () => (
      <DateRangePicker
        startLabel="From:"
        endLabel="To:"
        onChange={(value) => action('onChange')(value)}
        value={{
          startDate: moment('2019-08-08'),
          endDate: moment('2019-08-20'),
        }}
        minDate="2019-05-22"
        startDateId="startDate"
        endDateId="endDate"
      />
    ),
    {
      notes: 'A date range picker with minDate prop specified.',
    }
  )
  .add(
    'with maxDate',
    () => (
      <DateRangePicker
        startLabel="From:"
        endLabel="To:"
        onChange={(value) => action('onChange')(value)}
        value={{
          startDate: moment('2019-08-08'),
          endDate: moment('2019-08-20'),
        }}
        maxDate="2019-12-22"
        startDateId="startDate"
        endDateId="endDate"
      />
    ),
    {
      notes: 'A date range picker with maxDate prop specified.',
    }
  )
  .add(
    'with minDate & maxDate',
    () => (
      <DateRangePicker
        startLabel="From:"
        endLabel="To:"
        onChange={(value) => action('onChange')(value)}
        value={{
          startDate: '2019-08-08',
          endDate: '2019-08-20',
        }}
        minDate="2018-09-22"
        maxDate="2019-12-22"
        startDateId="startDate"
        endDateId="endDate"
      />
    ),
    {
      notes: 'A date range picker with maxDate prop specified.',
    }
  )
  .add(
    'disabled',
    () => (
      <DateRangePicker
        startLabel="From:"
        endLabel="To:"
        startPlaceholder="MM/DD/YYYY"
        endPlaceholder="MM/DD/YYYY"
        disabled
        startDateId="startDateDisabled"
        endDateId="endDateDisabled"
      />
    ),
    {
      notes: 'A **disabled** date range picker.',
    }
  );
