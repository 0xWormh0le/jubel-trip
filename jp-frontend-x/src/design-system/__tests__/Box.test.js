import React from 'react';
import { mount } from 'enzyme';
import toJson from 'enzyme-to-json';
import Box from '../Box';

describe('Box component', () => {
  it('should render as box', () => {
    const wrapper = mount(<Box />);
    expect(toJson(wrapper)).toMatchSnapshot();
  });
});
