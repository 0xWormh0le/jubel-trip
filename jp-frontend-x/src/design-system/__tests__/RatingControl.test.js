import React from 'react';
import { shallow } from 'enzyme';
import toJson from 'enzyme-to-json';

import RatingControl from '../RatingControl';

describe('RatingControl component', () => {
  it('renders with specified color state correctly', () => {
    const localProps = {
      color: 'orange',
      fullColor: 'pink',
    };
    const wrapper = shallow(<RatingControl {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });

  it('renders with custom icons correctly', () => {
    const localProps = {
      fractions: 2,
      emptyIcon: 'empty-circle',
      fullIcon: 'full-circle',
      size: 18,
    };
    const wrapper = shallow(<RatingControl {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
