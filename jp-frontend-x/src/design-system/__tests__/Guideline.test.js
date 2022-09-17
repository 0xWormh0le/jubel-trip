import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';

import Guideline from '../Guideline';

describe('Gridline component', () => {
  it('should render Guideline component', () => {
    const wrapper = mount(<Guideline />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
  it('should render guideline with props', () => {
    const localProps = {
      left: '30%',
    };
    const wrapper = mount(<Guideline {...localProps} />);
    expect(toJson(wrapper)).toMatchSnapshot();
    expect(
      wrapper
        .find(Guideline)
        .first()
        .prop('left')
    ).toBe('30%');
  });
});
