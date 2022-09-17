import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Text from '../Text';

describe('Text component', () => {
  it('renders uppercase Text correctly', () => {
    const localProps = {
      uppercase: true,
      children: 'Upper Case',
    };
    const wrapper = mount(<Text {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders lowercase Text correctly', () => {
    const localProps = {
      textStyle: 'lowercase',
      children: 'Lower Case',
    };
    const wrapper = mount(<Text {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders capitalized Text correctly', () => {
    const localProps = {
      textStyle: 'capitalize',
      children: 'Capitalized',
    };
    const wrapper = mount(<Text {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders truncated Text correctly', () => {
    const localProps = {
      textStyle: 'truncate',
      children: 'Truncated',
    };
    const wrapper = mount(<Text {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
