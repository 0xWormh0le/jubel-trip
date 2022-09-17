import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Absolute from '../Absolute';

describe('Absolute component', () => {
  it('should render as Absolute', () => {
    const wrapper = mount(<Absolute />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
