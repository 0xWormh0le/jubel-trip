import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Grid from '../Grid';

describe('Grid component', () => {
  it('should render as Grid', () => {
    const wrapper = mount(<Grid />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
