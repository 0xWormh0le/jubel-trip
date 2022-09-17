import React from 'react';
import DatePickerInput from 'react-datepicker';
import toJson from 'enzyme-to-json';

import { mountWithTheme } from '../../utils/testHelpers';
import { toValidDate } from '../../utils/date';
import DatePicker from '../DatePicker';
import i18n from '../../i18n';
import mockDate from '../../__mocks__/dateMocks';
import Select from '../Select';
import Text from '../Text';

describe('DatePicker component', () => {
  let timestamp;
  beforeEach(() => {
    timestamp = mockDate(new Date(2019, 6, 6));
  });

  it('renders date picker correctly with label', () => {
    const localProps = {
      label: 'From:',
      formatDate: (value) => toValidDate(value, i18n.language),
      onChange: () => {},
      value: new Date(2019, 6, 6),
    };
    const wrapper = mountWithTheme(<DatePicker {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(Text)
        .first()
        .prop('children')
    ).toEqual('From:');
  });

  it('renders the dates outside min and max dates as disabled', () => {
    const localProps = {
      label: 'From:',
      formatDate: (value) => toValidDate(value, i18n.language),
      onChange: () => {},
      value: new Date(2019, 6, 6),
      minDate: new Date(2019, 6, 1),
      maxDate: new Date(2019, 6, 30),
    };
    const wrapper = mountWithTheme(<DatePicker {...localProps} />);
    expect(wrapper.find(DatePickerInput).prop('minDate')).toEqual(
      localProps.minDate
    );
    expect(wrapper.find(DatePickerInput).prop('maxDate')).toEqual(
      localProps.maxDate
    );
  });

  it('renders months in select box within the range of 1 year back and forth', () => {
    const localProps = {
      label: 'From:',
      formatDate: (value) => toValidDate(value, i18n.language),
      onChange: () => {},
      value: new Date(2019, 6, 6),
    };
    const wrapper = mountWithTheme(<DatePicker {...localProps} />);
    wrapper.find('.react-datepicker__input-container input').simulate('click');
    const minMonthFromThisYear = localProps.value.getMonth() - 12;
    const maxMonthFromThisYear = localProps.value.getMonth() + 12;
    const options = wrapper.find(Select).prop('children');
    expect(options[0].props.value).toEqual(minMonthFromThisYear);
    expect(options[options.length - 1].props.value).toEqual(
      maxMonthFromThisYear
    );
  });
});
