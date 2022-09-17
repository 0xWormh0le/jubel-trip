import React from 'react';
import moment from 'moment';
import toJson from 'enzyme-to-json';
import 'react-dates/initialize';

import { mountWithTheme } from '../../utils/testHelpers';
import DateRangePicker, { CalendarHeader } from '../DateRangePicker';
import mockDate from '../../__mocks__/dateMocks';
import Flex from '../Flex';
import Select from '../Select';

describe('DateRangePicker component', () => {
  let timestamp;
  beforeEach(() => {
    timestamp = mockDate(new Date(2019, 6, 6));
  });

  it('renders date range picker correctly with label', () => {
    const localProps = {
      startLabel: 'From:',
      endLabel: 'To:',
      onChange: () => {},
      value: moment('2019-06-06'),
      maxDate: '2019-12-22',
      minDate: '2018-09-22',
      startDateId: 'startDate',
      endDateId: 'endDate',
    };
    const wrapper = mountWithTheme(<DateRangePicker {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(Flex)
        .first()
        .text()
    ).toContain(localProps.startLabel);
    expect(
      wrapper
        .find(Flex)
        .first()
        .text()
    ).toContain(localProps.endLabel);
  });

  it('renders months in select box within the range of 1 year back and forth', () => {
    const localProps = {
      month: moment('2019-06-06'),
      onMonthSelect: () => {},
    };
    const wrapper = mountWithTheme(<CalendarHeader {...localProps} />);
    const minMonthFromThisYear = localProps.month.month() - 12;
    const maxMonthFromThisYear = localProps.month.month() + 12;
    const options = wrapper
      .find(Select)
      .at(0)
      .prop('children');
    expect(options[0].props.value).toEqual(minMonthFromThisYear);
    expect(options[options.length - 1].props.value).toEqual(
      maxMonthFromThisYear
    );
  });

  it('renders months in select box between min & max dates if they are in the range of 1 year back and forth', () => {
    const localProps = {
      month: moment('2019-06-06'),
      maxDate: '2019-12-22',
      minDate: '2018-09-22',
      onMonthSelect: () => {},
    };
    const wrapper = mountWithTheme(<CalendarHeader {...localProps} />);
    const options = wrapper
      .find(Select)
      .at(0)
      .prop('children');
    expect(options[0].props.value).toEqual(-4);
    expect(options[options.length - 1].props.value).toEqual(11);
  });
});
