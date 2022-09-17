import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import ReviewButton from '../ReviewButton';

describe('ReviewButton component', () => {
  it('renders correctly', () => {
    const localProps = {
      label: 'Green Comments',
      color: 'green',
    };
    const wrapper = mount(<ReviewButton {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
